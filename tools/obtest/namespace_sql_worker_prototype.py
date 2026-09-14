#!/usr/bin/env python3
"""Throwaway V13/V14: SQL-only workers, query deadlines and autocommit INSERT.

SEEKDB_FORK_PROTOTYPE_TEST_ROOT=/tmp python3 tools/obtest/namespace_sql_worker_prototype.py --binary build_release/src/observer/seekdb
Add --case insert for V14 writes, rollback, isolation and crash recovery.
Linux integration probe. No claim of Windows/macOS or high-concurrency validation.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import re
import resource
import signal
import socket
import struct
import time

import pymysql
from namespace_lineage_prototype import LineageExperiment


class WorkerExperiment(LineageExperiment):
    def start(self):
        super().start()
        self.sql("ALTER SYSTEM SET syslog_level='WARN'")

    def worker_connect(self, namespace):
        return pymysql.connect(host="127.0.0.1", port=self.port, user="root", password="",
                               database=f"__fork_ns_{namespace}__db1", charset="utf8mb4",
                               autocommit=True, connect_timeout=10, read_timeout=40, write_timeout=10)

    def worker_pid(self, namespace):
        matches = re.findall(r"PROTOTYPE_V10_WORKER_READY ns=(\d+) generation=(\d+) pid=(\d+)",
                             self.engine_log())
        return next(int(pid) for ns, _, pid in reversed(matches) if int(ns) == namespace)

    def run_inserts(self):
        self.setup_lineage()
        c, _ = self.capture("b", "c")
        first = second = sibling = None
        try:
            first, second, sibling = self.worker_connect(self.b), self.worker_connect(self.b), self.worker_connect(c)
            pid = self.worker_pid(self.b)
            self.sql("SET @v=7", first)
            with first.cursor() as cursor:
                assert cursor.execute("INSERT INTO t1 VALUES(3,@v*6),(4,NULL)") == 2
            self.sql("INSERT INTO t1(v,id) VALUES(-50,5)", first)
            assert self.sql("SELECT id,v FROM t1 WHERE id>=3 ORDER BY id", second) == ((3,42),(4,None),(5,-50))
            self.record("worker_insert_expressions_and_affected_rows", affected=2, second_session_visible=True)

            rows = tuple((i, i*10) for i in range(100,196))
            with first.cursor() as cursor:
                assert cursor.execute("INSERT INTO t1 VALUES" + ",".join(f"({i},{v})" for i,v in rows)) == len(rows)
            assert self.sql("SELECT id,v FROM t1 WHERE id>=100 ORDER BY id", second) == rows
            self.record("worker_insert_multiple_batches", rows=len(rows), batch_limit=32)

            failed_rows = ",".join(f"({i},{i*10})" for i in range(200,241)) + ",(1,999)"
            try:
                self.sql("INSERT INTO t1 VALUES" + failed_rows, first)
            except pymysql.IntegrityError as error:
                assert error.args[0] == 1062, error.args
                self.record("duplicate_rolls_back_whole_statement", error=error.args, rows_before_duplicate=41)
            else:
                raise AssertionError("duplicate INSERT succeeded")
            assert self.sql("SELECT id,v FROM t1 WHERE id>=200", second) == ()
            assert self.sql("SELECT v FROM t1 WHERE id=1", first) == ((10,),)
            self.sql("INSERT INTO t1 VALUES(300,3000)", first)
            assert self.worker_pid(self.b) == pid

            self.sql("SET SESSION ob_query_timeout=500000", first)
            started = time.monotonic()
            try:
                self.sql("INSERT INTO t1 VALUES(400,4000),(401,1+SLEEP(2))", first)
            except pymysql.MySQLError as error:
                assert error.args[0] == 4012, error.args
                self.record("insert_timeout_rolled_back", seconds=time.monotonic()-started, error=error.args)
            else:
                raise AssertionError("expired INSERT succeeded")
            self.sql("SET SESSION ob_query_timeout=10000000", first)
            assert self.sql("SELECT id FROM t1 WHERE id>=400", second) == ()
            assert self.sql("SELECT 1", first) == ((1,),)

            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [pool.submit(self.sql, f"INSERT INTO t1 VALUES({key},{key*10})", connection)
                           for key, connection in ((310,first),(311,second))]
                for future in futures:
                    future.result(timeout=15)
            assert self.sql("SELECT id,v FROM t1 WHERE id BETWEEN 310 AND 311 ORDER BY id", first) == ((310,3100),(311,3110))
            self.record("concurrent_insert_sessions", shared_worker=pid, transactions=2)

            for query in ("INSERT IGNORE INTO t1 VALUES(1,0)",
                          "INSERT INTO t1 VALUES(1,0) ON DUPLICATE KEY UPDATE v=0",
                          "REPLACE INTO t1 VALUES(1,0)", "INSERT INTO t1 SELECT id+1000,v FROM t1"):
                try:
                    self.sql(query, first)
                except pymysql.MySQLError as error:
                    self.record("unsupported_insert_rejected", sql=query, error=error.args)
                else:
                    raise AssertionError(query)
            assert self.sql("SELECT id FROM t1 WHERE id>=1000", second) == ()
            assert self.sql("SELECT id,v FROM t1 ORDER BY id", sibling) == ((1,10),(2,20))
            assert self.sql("SELECT id,v FROM db1.t1 ORDER BY id") == ((1,10),(2,20))
            assert self.sql("SELECT id,v FROM " + self.table(self.root("a")[0], "db1.t1") + " ORDER BY id") == ((1,10),(2,20))
            self.record("worker_insert_namespace_isolation", source_unchanged=True, sibling_unchanged=True)
        finally:
            for connection in (first, second, sibling):
                if connection is not None:
                    connection.close()
        self.restart()
        first, sibling = self.worker_connect(self.b), self.worker_connect(c)
        try:
            assert self.sql("SELECT id,v FROM t1 WHERE id>=100 ORDER BY id", first) == rows + ((300,3000),(310,3100),(311,3110))
            assert self.sql("SELECT id,v FROM t1 WHERE id BETWEEN 3 AND 5 ORDER BY id", first) == ((3,42),(4,None),(5,-50))
            assert self.sql("SELECT id,v FROM t1 ORDER BY id", sibling) == ((1,10),(2,20))
            self.sql("INSERT INTO t1 VALUES(301,3010)", first)
            assert self.sql("SELECT v FROM t1 WHERE id=301", first) == ((3010,),)
            self.record("PASS", case="namespace_worker_insert", crash_recovery=True,
                        worker_sql_and_das=True, shared_transaction_and_storage=True)
        finally:
            first.close()
            sibling.close()

    def session_events(self, kind):
        events = []
        for path in (self.base / "run").glob(f"namespace-worker-{self.b}-*/process.out"):
            for line in path.read_text(errors="replace").splitlines():
                if line.startswith("PROTOTYPE_V11_SESSION_" + kind + " "):
                    events.append({k: int(v) for k, v in re.findall(r"(\w+)=(\d+)", line)})
        return events

    def run_sessions(self, first, second):
        assert self.sql("SELECT CONNECTION_ID()", first) == ((first.thread_id(),),)
        self.sql("SET @x=17, @label='你好'", first)
        assert self.sql("SELECT @x, @label", first) == ((17, "你好"),)
        assert self.sql("SELECT @x, @label", second) == ((None, None),)
        self.sql("SET @x=29", second)
        assert self.sql("SELECT @x", first) == ((17,),)
        initial_mode = self.sql("SELECT @@sql_mode", second)
        self.sql("SET SESSION sql_mode='ANSI_QUOTES'", first)
        assert self.sql('SELECT "id" FROM t1 WHERE id=1', first) == ((1,),)
        assert self.sql("SELECT @@sql_mode", first) == (("ANSI_QUOTES",),)
        assert self.sql("SELECT @@sql_mode", second) == initial_mode
        self.sql("SET NAMES utf8mb4 COLLATE utf8mb4_bin", first)
        assert self.sql("SELECT @@collation_connection, @label", first) == (("utf8mb4_bin", "你好"),)
        self.sql("SET SESSION ob_query_timeout=5000000", first)
        assert self.sql("SELECT @@ob_query_timeout", first) == ((5000000,),)
        assert self.sql("SELECT '; stays inside a string';", first) == (("; stays inside a string",),)
        self.sql("USE db2", first)
        assert self.sql("SELECT DATABASE(),v FROM t1 WHERE id=1", first) == (("db2", 10),)
        assert self.sql("SELECT DATABASE(),v FROM t1 WHERE id=1", second) == (("db1", 90),)
        first.select_db("db1")
        assert self.sql("SELECT DATABASE(),v FROM t1 WHERE id=1", first) == (("db1", 90),)
        for database in ("missing_db", "oceanbase", "__fork_ns_3__db1", "db1`; SET @x=999; --"):
            try:
                first.select_db(database)
            except pymysql.MySQLError as error:
                self.record("database_change_rejected", database=database, error=error.args)
            else:
                raise AssertionError(database)
            assert self.sql("SELECT DATABASE(),@x", first) == (("db1", 17),)
        for query in ("SET GLOBAL sql_mode=''", "SET autocommit=0", "BEGIN", "COMMIT",
                      "SET @x=(SELECT v FROM t1 WHERE id=1)", "SELECT missing_column FROM t1",
                      "SELECT 1; SET @x=999", "SET @x=999; SELECT @x"):
            try:
                self.sql(query, first)
            except pymysql.MySQLError as error:
                self.record("session_command_rejected", sql=query, error=error.args)
            else:
                raise AssertionError(query)
            assert self.sql("SELECT @x", first) == ((17,),)
        self.record("persistent_session_variables_and_database", isolated=True, native_commands=True)

        extras = []
        closed = len(self.session_events("CLOSE"))
        try:
            for i in range(12):
                conn = self.worker_connect(self.b)
                extras.append(conn)
                self.sql(f"SET @x={100+i}", conn)
            # Reallocating the slot vector must not move the sessions themselves.
            assert self.sql("SELECT @x,@label", first) == ((17, "你好"),)
            for i, conn in enumerate(extras):
                assert self.sql("SELECT @x", conn) == ((100+i,),)
        finally:
            for conn in extras:
                conn.close()
        self.wait_until(lambda: len(self.session_events("CLOSE")) == closed + len(extras), "sessions not reclaimed")
        high_water = self.session_events("OPEN")[-1]["slots"]
        assert high_water == 14, self.session_events("OPEN")
        for i in range(8):
            conn = self.worker_connect(self.b)
            assert self.sql("SELECT @x,@label", conn) == ((None, None),)
            event = self.session_events("OPEN")[-1]
            assert event["slots"] == high_water and event["generation"] > 1, event
            # Alternate graceful COM_QUIT and abrupt TCP resets.
            if i % 2:
                conn._sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                conn._force_close()
            else:
                conn.close()
            self.wait_until(lambda: len(self.session_events("CLOSE")) == closed + len(extras) + i + 1,
                            "closed/reused session not reclaimed")
        self.record("slots_grow_on_demand_and_reuse", high_water=high_water,
                    live=self.session_events("CLOSE")[-1]["active"], full_session_released=True)

        interrupted = self.worker_connect(self.b)
        before = len(self.session_events("CLOSE"))
        scan_log = self.base / "log" / "seekdb.log"
        offset = scan_log.stat().st_size
        pid = self.worker_pid(self.b)
        def scan_opened():
            with scan_log.open("rb") as log:
                log.seek(offset)
                return f"PROTOTYPE_V10_SCAN_OPEN ns={self.b} ".encode() in log.read()
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(self.sql, "SELECT SLEEP(2)+v FROM t1 WHERE id=1", interrupted)
                self.wait_until(scan_opened, "disconnect probe never opened scan")
                interrupted._sock.shutdown(socket.SHUT_RDWR)
                try:
                    pending.result(timeout=10)
                except pymysql.MySQLError:
                    pass
                else:
                    raise AssertionError("disconnected query succeeded")
            self.wait_until(lambda: len(self.session_events("CLOSE")) == before + 1,
                            "in-flight disconnect did not reclaim session")
            assert self.worker_pid(self.b) == pid
            assert f"PROTOTYPE_V11_RESPONSE_DRAIN ns={self.b} " in self.engine_log()
            assert self.sql("SELECT @x,v FROM t1 WHERE id=1", first) == ((17,90),)
            assert self.sql("SELECT @x", second) == ((29,),)
            self.record("inflight_disconnect_drains_without_killing_other_sessions", pid=pid)
        finally:
            interrupted.close()
        self.sql("SET ob_query_timeout=30000000", first)

    def run_concurrency(self, first, second):
        scan_log = self.base / "log" / "seekdb.log"
        offset = scan_log.stat().st_size
        def opened():
            with scan_log.open("rb") as log:
                log.seek(offset)
                return f"PROTOTYPE_V10_SCAN_OPEN ns={self.b} ".encode() in log.read()
        with ThreadPoolExecutor(max_workers=2) as pool:
            slow = pool.submit(self.sql, "SELECT SLEEP(3),@x,v FROM t1 WHERE id=1", first)
            self.wait_until(opened, "slow query never opened storage scan")
            start = time.monotonic()
            assert self.sql("SELECT @x,v FROM t1 WHERE id=1", second) == ((29,90),)
            elapsed = time.monotonic()-start
            assert elapsed < 2 and not slow.done(), elapsed
            assert slow.result(timeout=10) == ((0,17,90),)
            def scan_many(connection, variable, reverse):
                for _ in range(8):
                    rows = self.sql("SELECT id,v+@x,SLEEP(0.02) FROM t1 ORDER BY id " +
                                    ("DESC" if reverse else "ASC"), connection, log=False)
                    expected = ((1,90+variable,0),(2,20+variable,0))
                    assert rows == (tuple(reversed(expected)) if reverse else expected), rows
            a = pool.submit(scan_many, first, 17, False)
            b = pool.submit(scan_many, second, 29, True)
            a.result(timeout=20); b.result(timeout=20)
        self.record("same_worker_concurrent_sql_and_scans", fast_seconds=elapsed, interleaved_queries=16)

    def run_slow_client(self, healthy):
        slow = self.worker_connect(self.b)
        pid = self.worker_pid(self.b)
        closed = len(self.session_events("CLOSE"))
        sid = slow.thread_id()
        directory = max((self.base / "run").glob(f"namespace-worker-{self.b}-*"),
                        key=lambda path: int(path.name.rsplit("-", 1)[1]))
        def worker_log():
            return (directory / "process.out").read_text(errors="replace")
        offset = len(worker_log())
        def began():
            return re.search(rf"PROTOTYPE_V12_EXECUTE_BEGIN .*session={sid}\n", worker_log()[offset:])
        def ended():
            return re.search(rf"PROTOTYPE_V12_EXECUTE_END .*session={sid} ret=", worker_log()[offset:])
        try:
            # Read no response bytes. The result exceeds the TCP buffers; the
            # existing NIO writer blocks this request while IPC credits bound it.
            slow._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            slow._execute_command(3, "SELECT id,REPEAT('x',65536) FROM t1 ORDER BY id")
            self.wait_until(began, "slow reader query never started")
            time.sleep(1)
            assert not ended(), "probe did not stall the result stream"
            start = time.monotonic()
            assert self.sql("SELECT SUM(v) FROM t1", healthy) == ((48590,),)
            elapsed = time.monotonic()-start
            assert elapsed < 2 and not ended(), elapsed
            slow._sock.shutdown(socket.SHUT_RDWR)
            slow._force_close()
            self.wait_until(lambda: len(self.session_events("CLOSE")) == closed+1,
                            "slow disconnected session not reclaimed")
            assert self.worker_pid(self.b) == pid and ended()
            assert self.sql("SELECT 1", healthy) == ((1,),)
            self.record("slow_tcp_reader_does_not_block_other_session", fast_seconds=elapsed,
                        result_bytes_at_least=98*65536, worker_survived=True)
        finally:
            slow.close()

    def run_timeouts(self, first, second):
        pid = self.worker_pid(self.b)
        directory = max((self.base / "run").glob(f"namespace-worker-{self.b}-*"),
                        key=lambda path: int(path.name.rsplit("-", 1)[1]))
        def worker_log():
            return (directory / "process.out").read_text(errors="replace")
        def expect_timeout(future):
            try:
                future.result(timeout=5)
            except pymysql.MySQLError as error:
                assert error.args[0] == 4012, error.args
                return error.args
            raise AssertionError("query exceeded its deadline without an error")

        # A real interval longer than the removed IPC limit. B must progress
        # while A sends no rows for 31 seconds, and A must then succeed.
        self.sql("SET ob_query_timeout=40000000", first)
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(self.sql, "SELECT SLEEP(31),@x,v FROM t1 WHERE id=1", first)
            time.sleep(.3)
            assert self.sql("SELECT @x,v FROM t1 WHERE id=1", second) == ((29,90),)
            assert not pending.done()
            assert pending.result(timeout=38) == ((0,17,90),)
        self.record("query_exceeds_old_30_second_ipc_limit", seconds=time.monotonic()-started, pid=pid)

        self.sql("SET ob_query_timeout=500000", first)
        for attempt in range(5):
            started = time.monotonic()
            with ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(self.sql, "SELECT SLEEP(10)+v FROM t1 WHERE id=1", first)
                assert self.sql("SELECT @x,v FROM t1 WHERE id=1", second) == ((29,90),)
                error = expect_timeout(pending)
            elapsed = time.monotonic()-started
            assert .3 < elapsed < 2, elapsed
            assert self.sql("SELECT @x,v FROM t1 WHERE id=1", first) == ((17,90),)
            assert self.worker_pid(self.b) == pid and Path(f"/proc/{pid}").exists()
            self.record("query_timeout_keeps_session_and_worker", attempt=attempt, seconds=elapsed, error=error)
        assert f"PROTOTYPE_V13_SCANS_RELEASED ns={self.b} remaining=1" in self.engine_log()

        queued = self.worker_connect(self.b)
        try:
            self.sql("SET ob_query_timeout=500000", queued)
            self.sql("SET ob_query_timeout=10000000", first)
            self.sql("SET ob_query_timeout=10000000", second)
            offset = len(worker_log())
            with ThreadPoolExecutor(max_workers=3) as pool:
                a = pool.submit(self.sql, "SELECT SLEEP(3)", first)
                b = pool.submit(self.sql, "SELECT SLEEP(3)", second)
                self.wait_until(lambda: all(re.search(rf"PROTOTYPE_V12_EXECUTE_BEGIN .*session={sid}\n",
                                                      worker_log()[offset:])
                                            for sid in (first.thread_id(),second.thread_id())),
                                "both execution threads did not start")
                started = time.monotonic()
                c = pool.submit(self.sql, "SET @queued=999", queued)
                expect_timeout(c)
                elapsed = time.monotonic()-started
                assert elapsed < 2 and not a.done() and not b.done(), elapsed
                a.result(timeout=5); b.result(timeout=5)
            assert self.sql("SELECT @queued", queued) == ((None,),)
            assert self.sql("SELECT 1", queued) == ((1,),)
            assert self.worker_pid(self.b) == pid
            self.record("queued_timeout_does_not_wait_for_executor", seconds=elapsed, statement_not_executed=True)
        finally:
            queued.close()

    def run_slow_client_timeout(self, healthy):
        assert self.sql("SELECT REPEAT('x',65536)", log=False) == (("x"*65536,),)
        assert self.sql("SELECT REPEAT('x',65536)", healthy, log=False) == (("x"*65536,),)
        slow = self.worker_connect(self.b)
        pid = self.worker_pid(self.b)
        directory = max((self.base / "run").glob(f"namespace-worker-{self.b}-*"),
                        key=lambda path: int(path.name.rsplit("-", 1)[1]))
        log = directory / "process.out"
        offset = len(log.read_text(errors="replace"))
        try:
            self.sql("SET ob_query_timeout=2000000", slow)
            # Exclude the SET completion from the cancellation evidence.
            offset = len(log.read_text(errors="replace"))
            before = self.engine_log().count(f"PROTOTYPE_V13_SCANS_RELEASED ns={self.b} ")
            slow._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            started = time.monotonic()
            slow._execute_command(3, "SELECT id,REPEAT('x',65536) FROM t1 ORDER BY id")
            self.wait_until(lambda: re.search(rf"PROTOTYPE_V12_EXECUTE_END .*session={slow.thread_id()} ret=-4012",
                                              log.read_text(errors="replace")[offset:]),
                            "slow client kept execution alive after deadline")
            self.wait_until(lambda: self.engine_log().count(f"PROTOTYPE_V13_SCANS_RELEASED ns={self.b} ") > before,
                            "slow client kept gateway scans alive after deadline")
            elapsed = time.monotonic()-started
            assert elapsed < 5, elapsed
            assert self.sql("SELECT 1", healthy) == ((1,),)
            # Resume reading: already sent rows followed by ERR must form a
            # valid MySQL response so this same connection can be reused.
            slow._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024*1024)
            try:
                slow._read_query_result(unbuffered=False)
            except pymysql.MySQLError as error:
                assert error.args[0] == 4012, error.args
            else:
                raise AssertionError("slow client query did not time out")
            assert self.sql("SELECT 1", slow) == ((1,),)
            assert self.worker_pid(self.b) == pid
            self.record("slow_client_timeout_releases_scans_and_keeps_session", seconds=elapsed, pid=pid)
        finally:
            slow.close()

    def run_workers(self):
        self.setup_lineage()
        c, _ = self.capture("b", "c")
        self.sql("UPDATE " + self.table(self.b, "db1.t1") + " SET v=90 WHERE id=1")
        bconn = cconn = reconnect = stale = None
        try:
            bconn, cconn = self.worker_connect(self.b), self.worker_connect(c)
            bp, cp = self.worker_pid(self.b), self.worker_pid(c)
            assert bp != cp and bp != self.proc.pid and cp != self.proc.pid
            self.record("three_processes_one_public_port", port=self.port, engine=self.proc.pid, b=bp, c=cp)
            assert self.sql("SELECT 1 + 2 AS answer", bconn) == ((3,),)
            assert self.sql("SELECT id,v FROM t1 ORDER BY id", bconn) == ((1,90),(2,20))
            stale = self.worker_connect(self.b)
            assert self.sql("SELECT id,v FROM t1 ORDER BY id", cconn) == ((1,10),(2,20))
            assert self.sql("SELECT id,v+7 FROM t1 WHERE id>=2 ORDER BY id DESC", bconn) == ((2,27),)
            assert self.sql("SELECT SUM(v) FROM t1", bconn) == ((110,),)
            assert self.sql("SELECT id,v FROM t1 WHERE id=1", cconn) == ((1,10),)
            self.run_sessions(bconn, stale)
            self.run_concurrency(bconn, stale)
            self.run_timeouts(bconn, stale)
            for query in ("UPDATE t1 SET v=1", "SELECT * FROM __fork_ns_3__db1.t1" if self.b != 3 else "SELECT * FROM __fork_ns_2__db1.t1"):
                try:
                    self.sql(query, bconn)
                except pymysql.MySQLError as error:
                    self.record("worker_rejected_unsupported_query", sql=query, error=error.args)
                else:
                    raise AssertionError(query)
            bconn.select_db("db2")
            assert self.sql("SELECT id,v FROM t1 ORDER BY id", bconn) == ((1,10),(2,20))
            bconn.select_db("db1")
            assert self.sql("SELECT id,v FROM t1 ORDER BY id", bconn) == ((1,90),(2,20))
            scan_log = self.base / "log" / "seekdb.log"
            before = scan_log.stat().st_size
            def scan_opened():
                with scan_log.open("rb") as log:
                    log.seek(before)
                    return f"PROTOTYPE_V10_SCAN_OPEN ns={self.b} ".encode() in log.read()
            victim = self.worker_connect(self.b)
            try:
                with ThreadPoolExecutor(max_workers=2) as pool:
                    pending = pool.submit(self.sql, "SELECT SLEEP(15)+v FROM t1 WHERE id=1", bconn)
                    self.wait_until(scan_opened, "remote storage scan did not open")
                    assert self.sql("SELECT 1", stale) == ((1,),)
                    assert not pending.done()
                    second = pool.submit(self.sql, "SELECT SLEEP(15)+v FROM t1 WHERE id=2", victim)
                    def both_opened():
                        with scan_log.open("rb") as log:
                            log.seek(before)
                            return log.read().count(f"PROTOTYPE_V10_SCAN_OPEN ns={self.b} ".encode()) >= 2
                    self.wait_until(both_opened, "second remote scan did not open")
                    os.kill(bp, signal.SIGKILL)
                    assert self.sql("SELECT id,v FROM t1 ORDER BY id", cconn) == ((1,10),(2,20))
                    for future in (pending, second):
                        try:
                            future.result(timeout=10)
                        except pymysql.MySQLError as error:
                            self.record("worker_death_failed_own_query", namespace=self.b, pid=bp, error=error.args)
                        else:
                            raise AssertionError("killed worker query succeeded")
                    self.record("worker_death_wakes_all_inflight_requests", requests=2)
            finally:
                victim.close()
            reconnect = self.worker_connect(self.b)
            assert self.worker_pid(self.b) != bp
            assert self.sql("SELECT id,v FROM t1 ORDER BY id", reconnect) == ((1,90),(2,20))
            assert self.sql("SELECT @x,@label", reconnect) == ((None,None),)
            try:
                self.sql("SELECT 1", stale)
            except pymysql.MySQLError as error:
                self.record("old_activation_connection_rejected", error=error.args)
            else:
                raise AssertionError("old connection entered the new worker")
            extra = [(i, i * 10) for i in range(3, 99)]
            self.sql("INSERT INTO " + self.table(self.b, "db1.t1") + " VALUES" +
                     ",".join(f"({i},{v})" for i,v in extra))
            assert self.sql("SELECT id,v FROM t1 WHERE id>=3 ORDER BY id", reconnect) == tuple(extra)
            assert self.sql("SELECT id,v+1 FROM t1 WHERE id>=3 AND MOD(v,30)=0 ORDER BY v DESC LIMIT 4", reconnect) == tuple(
                (i,v+1) for i,v in reversed(extra) if v % 30 == 0)[:4]
            assert self.sql("SELECT id,v FROM t1 WHERE id>=3 AND MOD(v,30)=0 ORDER BY id LIMIT 4 OFFSET 2", reconnect) == tuple(
                (i,v) for i,v in extra if v % 30 == 0)[2:6]
            assert self.sql("SELECT id FROM t1 WHERE v<0", reconnect) == ()
            self.record("bounded_scan_batches_and_worker_filter_sort", rows=len(extra), batch_limit=32)
            self.run_slow_client(reconnect)
            self.run_slow_client_timeout(reconnect)
            assert self.root("b") and self.root("c")
            for pid in (self.worker_pid(self.b), cp):
                status = Path(f"/proc/{pid}/status").read_text()
                memory = Path(f"/proc/{pid}/smaps_rollup").read_text()
                targets = []
                for fd in Path(f"/proc/{pid}/fd").iterdir():
                    try:
                        targets.append(os.readlink(fd))
                    except FileNotFoundError:
                        pass
                assert not any(str(self.base / "store") in target or target.startswith("socket:") for target in targets), targets
                self.record("worker_resources", pid=pid,
                            status=[line for line in status.splitlines() if line.startswith(("VmRSS:", "Threads:"))],
                            memory=[line for line in memory.splitlines() if line.startswith(("Pss:", "Private_Clean:", "Private_Dirty:"))],
                            no_engine_storage_or_network_fds=True)
                private_kib = sum(int(line.split()[1]) for line in memory.splitlines()
                                  if line.startswith(("Private_Clean:", "Private_Dirty:")))
                # Tiny warmed workload: catch cache sizing from host RAM instead of worker budget.
                assert private_kib < 64 * 1024, (pid, private_kib, "worker private memory exceeds 64 MiB")
            worker_dirs = list((self.base / "run").glob("namespace-worker-*"))
            assert worker_dirs and all(not (p / "store").exists() for p in worker_dirs)
            self.record("PASS", case="namespace_sql_worker_flow", workers_restarted=True,
                        actual_sql_pipeline=True, shared_storage=True, one_public_port=self.port)
        finally:
            for connection in (bconn, cconn, reconnect, stale):
                if connection is not None:
                    connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--case", choices=("full", "slow-timeout", "insert"), default="full")
    args = parser.parse_args()
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    os.environ["SEEKDB_NAMESPACE_SQL_WORKER_PROTOTYPE"] = "1"
    experiment = WorkerExperiment(args.binary, "insert_v14" if args.case == "insert" else "timeout_v13", prototype=6)
    try:
        experiment.start()
        if args.case == "insert":
            experiment.run_inserts()
        elif args.case == "slow-timeout":
            experiment.setup_lineage()
            experiment.sql("INSERT INTO " + experiment.table(experiment.b, "db1.t1") + " VALUES" +
                           ",".join(f"({i},{i*10})" for i in range(3,99)))
            conn = experiment.worker_connect(experiment.b)
            try:
                for _ in range(5):
                    experiment.run_slow_client_timeout(conn)
            finally:
                conn.close()
        else:
            experiment.run_workers()
    finally:
        experiment.close()


if __name__ == "__main__":
    main()
