#!/usr/bin/env python3
"""Throwaway V10: one MySQL port, two real SQL-only workers, one engine.

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
            for query in ("UPDATE t1 SET v=1", "SELECT * FROM __fork_ns_3__db1.t1" if self.b != 3 else "SELECT * FROM __fork_ns_2__db1.t1"):
                try:
                    self.sql(query, bconn)
                except pymysql.MySQLError as error:
                    self.record("worker_rejected_unsupported_query", sql=query, error=error.args)
                else:
                    raise AssertionError(query)
            try:
                bconn.select_db("db2")
            except pymysql.MySQLError as error:
                self.record("unsupported_protocol_command_rejected", error=error.args)
            else:
                raise AssertionError("COM_INIT_DB unexpectedly changed the binding")
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
    experiment = WorkerExperiment(args.binary, "sql_worker_v10", prototype=6)
    try:
        experiment.start()
        experiment.run_workers()
    finally:
        experiment.close()


if __name__ == "__main__":
    main()
