#!/usr/bin/env python3
"""Throwaway V11: connection sessions, two real SQL-only workers, one engine.

SEEKDB_FORK_PROTOTYPE_TEST_ROOT=/tmp python3 tools/obtest/namespace_sql_worker_prototype.py --binary build_release/src/observer/seekdb
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
            with ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(self.sql, "SELECT SLEEP(15)+v FROM t1 WHERE id=1", bconn)
                self.wait_until(scan_opened, "remote storage scan did not open")
                try:
                    self.sql("SELECT 1", stale)
                except pymysql.MySQLError as error:
                    self.record("busy_worker_rejected_extra_execution", error=error.args)
                else:
                    raise AssertionError("worker admitted a concurrent execution")
                os.kill(bp, signal.SIGKILL)
                assert self.sql("SELECT id,v FROM t1 ORDER BY id", cconn) == ((1,10),(2,20))
                try:
                    pending.result(timeout=10)
                except pymysql.MySQLError as error:
                    self.record("worker_death_failed_own_query", namespace=self.b, pid=bp, error=error.args)
                else:
                    raise AssertionError("killed worker query succeeded")
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
    args = parser.parse_args()
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    os.environ["SEEKDB_NAMESPACE_SQL_WORKER_PROTOTYPE"] = "1"
    experiment = WorkerExperiment(args.binary, "sql_session_v11", prototype=6)
    try:
        experiment.start()
        experiment.run_workers()
    finally:
        experiment.close()


if __name__ == "__main__":
    main()
