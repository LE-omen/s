#!/usr/bin/env python3
"""Native client ingress into the namespace worker; shared SQL stays forbidden."""
import argparse
import concurrent.futures
import io
import os
import re
import resource
import subprocess

import pymysql
import mysql.connector
from namespace_worker_bootstrap_prototype import BootstrapExperiment


def connect(port):
    return pymysql.connect(host="127.0.0.1", port=port, user="root", password="",
                           autocommit=True, connect_timeout=3, read_timeout=20)


def protocol_probe(experiment, port):
    result = subprocess.run(["mysql", "--protocol=TCP", "-h127.0.0.1", f"-P{port}",
                             "-uroot", "--connect-timeout=3", "-N", "-B", "-e", "SELECT 1"],
                            check=True, capture_output=True, text=True, timeout=15)
    assert result.stdout.strip() == "1", result
    with pymysql.connect(host="127.0.0.1", port=port, user="root", autocommit=True,
                         charset="utf8mb4", client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS,
                         connect_timeout=3, read_timeout=20) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT '中文😀'; SELECT 2")
            assert cursor.fetchone() == ("中文😀",)
            assert cursor.nextset()
            assert cursor.fetchone() == (2,)
            assert not cursor.nextset()
    experiment.record("direct_cli_multiresult_charset_verified")

    connection = mysql.connector.connect(host="127.0.0.1", port=port, user="root", password="",
                                         use_pure=True, ssl_disabled=True, autocommit=True,
                                         database="direct_check", connection_timeout=3)
    connection._socket.sock.settimeout(20)
    try:
        with connection.cursor(prepared=True) as cursor:
            statement = "SELECT id,v FROM t WHERE id=%s"
            for key, value in [(1, "committed"), (2, "second"), (1, "committed")]:
                cursor.execute(statement, (key,))
                assert cursor.fetchall() == [(key, value)]
            payload = b"long-data" * 40000
            cursor.execute("SELECT OCTET_LENGTH(%s)", (io.BytesIO(payload),))
            assert cursor.fetchone() == (len(payload),)
        experiment.record("direct_binary_prepared_long_data_verified")
        with connection.cursor() as cursor:
            cursor.execute("SET @pooled_value=10")
            cursor.execute("BEGIN")
            cursor.execute("UPDATE t SET v='pool rollback' WHERE id=1")
        connection.reset_session()
        with connection.cursor() as cursor:
            cursor.execute("SELECT @pooled_value, v FROM direct_check.t WHERE id=1")
            assert cursor.fetchone() == (None, "committed")
        experiment.record("direct_connection_reset_verified")
    finally:
        connection.close()


def probe(experiment):
    matches = re.findall(r"PROTOTYPE_V10_WORKER_READY ns=1 generation=\d+ pid=(\d+) port=(\d+)",
                         experiment.engine_log())
    assert matches, experiment.base
    pid, port = map(int, matches[-1])
    assert port and port != experiment.port
    experiment.record("direct_endpoint", worker_pid=pid, port=port)
    with connect(port) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1, CONNECTION_ID()")
            assert cursor.fetchone()[0] == 1
            cursor.execute("BEGIN")
            assert connection.server_status & 1
            cursor.execute("SELECT count(*) FROM oceanbase.__all_database")
            assert cursor.fetchone()[0] >= 6
            cursor.execute("ROLLBACK")
            assert not connection.server_status & 1
            cursor.execute("USE test")
            cursor.execute("SELECT DATABASE()")
            assert cursor.fetchone() == ("test",)
            cursor.execute("CREATE DATABASE direct_check")
            cursor.execute("USE direct_check")
            cursor.execute("CREATE TABLE t(id INT PRIMARY KEY, v VARCHAR(64))")
            experiment.record("direct_table_created")
            cursor.execute("INSERT INTO t VALUES (1,'first'),(2,'second')")
            experiment.record("direct_insert_committed")
            cursor.execute("BEGIN")
            cursor.execute("UPDATE t SET v='rolled back' WHERE id=1")
            cursor.execute("ROLLBACK")
            cursor.execute("SELECT v FROM t WHERE id=1")
            assert cursor.fetchone() == ("first",)
            cursor.execute("BEGIN")
            cursor.execute("SAVEPOINT direct_savepoint")
            cursor.execute("UPDATE t SET v='savepoint rollback' WHERE id=1")
            cursor.execute("ROLLBACK TO SAVEPOINT direct_savepoint")
            cursor.execute("RELEASE SAVEPOINT direct_savepoint")
            cursor.execute("COMMIT")
            cursor.execute("SELECT v FROM t WHERE id=1")
            assert cursor.fetchone() == ("first",)
            cursor.execute("BEGIN")
            cursor.execute("UPDATE t SET v='committed' WHERE id=1")
            cursor.execute("COMMIT")
            assert not connection.server_status & 1
            cursor.execute("SELECT id,v FROM t ORDER BY id")
            assert cursor.fetchall() == ((1, "committed"), (2, "second"))
            experiment.record("direct_ddl_dml_verified")
    def client(value):
        with connect(port) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET @direct_value=%s", (value,))
                cursor.execute("SELECT @direct_value, SLEEP(0.1)")
                assert cursor.fetchone() == (value, 0)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(client, range(8)))
    protocol_probe(experiment, port)
    assert "PROTOTYPE_V18_SHARED_SQL_REJECT" not in experiment.engine_log()
    experiment.record("direct_native_ingress_verified", clients=8, transactions=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    args = parser.parse_args()
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    os.environ["SEEKDB_NAMESPACE_SQL_WORKER_LISTEN"] = "1"
    os.environ["SEEKDB_NAMESPACE_SQL_WORKER_DIRECT_PROBE"] = "1"
    experiment = BootstrapExperiment(args.binary, "direct_v19", prototype=6)
    try:
        experiment.start()
        probe(experiment)
        experiment.record("PASS", direct_client_ingress=True)
    finally:
        experiment.close()


if __name__ == "__main__":
    main()
