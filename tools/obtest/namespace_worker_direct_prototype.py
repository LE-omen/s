#!/usr/bin/env python3
"""Native client ingress into the namespace worker; shared SQL stays forbidden."""
import argparse
import concurrent.futures
import datetime
import decimal
import io
import os
import re
import resource
import subprocess
import time
import traceback

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


def authentication_probe(experiment, port):
    with connect(port) as root, root.cursor() as cursor:
        cursor.execute("CREATE USER 'direct_reader'@'%' IDENTIFIED BY 'Direct-test-19!'")
        cursor.execute("GRANT SELECT ON direct_check.t TO 'direct_reader'@'%'")
    for user, password in [("direct_reader", "incorrect"), ("missing_reader", "")]:
        try:
            pymysql.connect(host="127.0.0.1", port=port, user=user, password=password,
                            connect_timeout=3, read_timeout=20)
        except pymysql.MySQLError as error:
            assert error.args[0] == 1045, error
        else:
            raise AssertionError("invalid credentials were accepted")
    experiment.record("direct_authentication_rejection_verified")
    with pymysql.connect(host="127.0.0.1", port=port, user="direct_reader", password="Direct-test-19!",
                         database="direct_check", autocommit=True, connect_timeout=3, read_timeout=20) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT v FROM t WHERE id=1")
            assert cursor.fetchone() == ("committed",)
            try:
                cursor.execute("UPDATE t SET v='unauthorized' WHERE id=1")
            except pymysql.MySQLError as error:
                assert error.args[0] == 1142, error
            else:
                raise AssertionError("table write without a grant was accepted")
            with connect(port) as root, root.cursor() as admin:
                admin.execute("REVOKE SELECT ON direct_check.t FROM 'direct_reader'@'%'")
            try:
                cursor.execute("SELECT v FROM t WHERE id=1")
            except pymysql.MySQLError as error:
                assert error.args[0] in (1044, 1142), error
            else:
                raise AssertionError("revoked table grant remained effective")
            with connect(port) as root, root.cursor() as admin:
                admin.execute("GRANT SELECT(v) ON direct_check.t TO 'direct_reader'@'%'")
            cursor.execute("SELECT v FROM t ORDER BY v")
            assert cursor.fetchall() == (("committed",), ("second",))
            try:
                cursor.execute("SELECT id FROM t")
            except pymysql.MySQLError as error:
                assert error.args[0] == 1143, error
            else:
                raise AssertionError("column without a grant was readable")
    experiment.record("direct_table_privileges_verified")


def lifecycle_probe(experiment, port):
    with connect(port) as control, control.cursor() as admin:
        victim = connect(port)
        with victim.cursor() as cursor:
            cursor.execute("BEGIN")
            cursor.execute("UPDATE direct_check.t SET v='disconnected' WHERE id=1")
        victim.close()
        admin.execute("SET ob_query_timeout=3000000")
        admin.execute("UPDATE direct_check.t SET v='committed' WHERE id=1")
        experiment.record("direct_disconnect_rollback_unlock_verified")
        with connect(port) as victim:
            def interrupted_query():
                with victim.cursor() as cursor:
                    try:
                        cursor.execute("SELECT SLEEP(10)")
                    except pymysql.MySQLError as error:
                        assert error.args[0] == 1317, error
                    else:
                        raise AssertionError("KILL QUERY did not interrupt SLEEP")
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                waiting = pool.submit(interrupted_query)
                time.sleep(.2)
                admin.execute(f"KILL QUERY {victim.thread_id()}")
                waiting.result(timeout=5)
            with victim.cursor() as cursor:
                cursor.execute("SELECT 1")
                assert cursor.fetchone() == (1,)
        experiment.record("direct_query_cancel_session_reuse_verified")
        admin.execute("SET GLOBAL autocommit=0")
        try:
            with pymysql.connect(host="127.0.0.1", port=port, user="root", autocommit=None,
                                 connect_timeout=3, read_timeout=20) as fresh:
                assert not fresh.server_status & 2
                with fresh.cursor() as cursor:
                    cursor.execute("SELECT @@autocommit")
                    assert cursor.fetchone() == (0,)
        finally:
            admin.execute("SET GLOBAL autocommit=1")
        experiment.record("direct_global_variable_greeting_verified")


def management_concurrency_probe(experiment, port):
    def client(index):
        with connect(port) as connection, connection.cursor() as cursor:
            for generation in range(2):
                database = f"concurrent_ddl_{index}_{generation}"
                cursor.execute(f"CREATE DATABASE {database}")
                cursor.execute(f"CREATE TABLE {database}.t(id INT PRIMARY KEY, v INT)")
                cursor.execute(f"INSERT INTO {database}.t VALUES(1,%s)", (index,))
                cursor.execute(f"SELECT v FROM {database}.t WHERE id=1")
                assert cursor.fetchone() == (index,)
                cursor.execute(f"DROP DATABASE {database}")
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(client, range(3)))
    experiment.record("direct_concurrent_management_verified", clients=3, databases=6)


def ddl_probe(experiment, port):
    with connect(port) as connection, connection.cursor() as cursor:
        cursor.execute("CREATE DATABASE ddl_check")
        cursor.execute("ALTER DATABASE ddl_check CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
        cursor.execute("USE ddl_check")
        cursor.execute("CREATE TABLE records(tenant_id INT, id INT, label VARCHAR(64), amount DECIMAL(12,2), "
                       "created DATE, nullable_value INT NULL, PRIMARY KEY(tenant_id,id))")
        cursor.execute("INSERT INTO records VALUES(1,1,'first',12.34,'2026-09-15',NULL),(1,2,'second',56.78,'2026-09-14',9)")
        cursor.execute("SELECT label,amount,created,nullable_value FROM records WHERE tenant_id=1 AND id=1")
        assert cursor.fetchone() == ("first", decimal.Decimal("12.34"), datetime.date(2026, 9, 15), None)
        experiment.record("direct_composite_key_types_verified")
        cursor.execute("SELECT /*+ parallel(2) */ SUM(amount) FROM records")
        assert cursor.fetchone() == (decimal.Decimal("69.12"),)
        experiment.record("direct_parallel_scan_verified")
        parallel_insert = ("INSERT /*+ enable_parallel_dml parallel(2) */ INTO records "
                           "SELECT tenant_id,id+10,label,amount,created,nullable_value FROM records WHERE id<=2")
        cursor.execute("BEGIN")
        cursor.execute(parallel_insert)
        cursor.execute("ROLLBACK")
        cursor.execute("SELECT COUNT(*) FROM records")
        assert cursor.fetchone() == (2,)
        cursor.execute(parallel_insert)
        cursor.execute("SELECT COUNT(*),SUM(amount) FROM records")
        assert cursor.fetchone() == (4, decimal.Decimal("138.24"))
        cursor.execute("DELETE FROM records WHERE id>10")
        experiment.record("direct_parallel_dml_verified")
        cursor.execute("CREATE INDEX records_label ON records(label)")
        cursor.execute("SELECT id FROM records FORCE INDEX(records_label) WHERE label='second'")
        assert cursor.fetchall() == ((2,),)
        cursor.execute("CREATE UNIQUE INDEX records_unique ON records(tenant_id,label)")
        try:
            cursor.execute("INSERT INTO records VALUES(1,3,'first',1,'2026-09-15',NULL)")
        except pymysql.MySQLError as error:
            assert error.args[0] == 1062, error
        else:
            raise AssertionError("unique index did not reject a duplicate")
        cursor.execute("UPDATE records SET label='changed' WHERE tenant_id=1 AND id=2")
        cursor.execute("SELECT id FROM records FORCE INDEX(records_label) WHERE label='changed'")
        assert cursor.fetchall() == ((2,),)
        experiment.record("direct_indexes_verified")
        cursor.execute("CREATE TABLE lob_records(id INT PRIMARY KEY, payload MEDIUMBLOB, text_value MEDIUMTEXT)")
        cursor.execute("INSERT INTO lob_records VALUES(1,%s,%s)", (b"blob-value", "text-value"))
        cursor.execute("SELECT payload,text_value FROM lob_records WHERE id=1")
        assert cursor.fetchone() == (b"blob-value", "text-value")
        experiment.record("direct_lob_schema_visibility_verified")
        cursor.execute("DROP TABLE lob_records")
        cursor.execute("ALTER TABLE records ADD COLUMN revision INT DEFAULT 7")
        cursor.execute("SELECT revision FROM records WHERE tenant_id=1 AND id=1")
        assert cursor.fetchone() == (7,)
        cursor.execute("DROP INDEX records_label ON records")
        cursor.execute("DROP INDEX records_unique ON records")
        cursor.execute("DROP TABLE records")
        cursor.execute("DROP DATABASE ddl_check")
        experiment.record("direct_general_ddl_verified")


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
    failures = []
    for name, check in [("management_concurrency", management_concurrency_probe),
                        ("authentication", authentication_probe), ("lifecycle", lifecycle_probe), ("ddl", ddl_probe)]:
        try:
            check(experiment, port)
        except Exception as error:
            traceback.print_exc()
            failures.append((name, repr(error)))
            experiment.record("direct_case_failed", case=name, error=repr(error))
    assert not failures, failures
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
