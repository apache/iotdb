# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import importlib
from datetime import date, datetime, time
from decimal import Decimal
from types import SimpleNamespace

import pytest
import numpy as np

import iotdb.dbapi as dbapi
from iotdb.dbapi.Cursor import Cursor
from iotdb.dbapi.Parameters import format_operation
from iotdb.utils.Field import Field
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.RowRecord import RowRecord
from iotdb.utils.SessionDataSet import SessionDataSet
from iotdb.utils.exception import StatementExecutionException


class FakeConnection(object):
    is_close = False


class FakeDataSet(object):
    def __init__(self, rows):
        self.rows = iter(rows)
        self.closed = False

    def get_column_names(self):
        return ["name", "value"]

    def get_column_types(self):
        return [TSDataType.STRING, TSDataType.INT32]

    def next_tuple(self):
        return next(self.rows, None)

    def close_operation_handle(self):
        self.closed = True


class FakeSession(object):
    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error
        self.statements = []

    def execute_statement(self, statement):
        self.statements.append(statement)
        if self.error is not None:
            raise self.error
        return self.result


def test_module_interface():
    assert dbapi.apilevel == "2.0"
    assert dbapi.threadsafety == 1
    assert dbapi.paramstyle == "pyformat"
    assert issubclass(dbapi.ProgrammingError, dbapi.DatabaseError)
    assert issubclass(dbapi.ConnectionError, dbapi.OperationalError)
    assert dbapi.Date(2026, 9, 16) == date(2026, 9, 16)
    assert dbapi.Time(1, 2, 3) == time(1, 2, 3)
    assert dbapi.Timestamp(2026, 9, 16, 1, 2, 3) == datetime(2026, 9, 16, 1, 2, 3)
    assert dbapi.Binary(bytearray((0, 255))) == b"\x00\xff"
    assert dbapi.STRING == TSDataType.STRING
    assert dbapi.NUMBER == TSDataType.INT64
    assert dbapi.DATETIME == TSDataType.DATE


def test_parameter_rendering_and_literal_percent_escapes():
    sql = format_operation(
        "INSERT INTO t VALUES (%(text)s, %(null)s, %(flag)s, %(number)s, "
        "%(decimal)s, %(binary)s, %(date)s) -- %%(ignored)s\n"
        "/* %%s */ SELECT 'literal %%s', 10 %% 3, %%",
        {
            "text": "IoTDB's value",
            "null": None,
            "flag": True,
            "number": 2.5,
            "decimal": Decimal("3.14"),
            "binary": b"\x00\xff",
            "date": date(2026, 9, 16),
        },
    )
    assert "'IoTDB''s value', NULL, TRUE, 2.5, 3.14" in sql
    assert "X'00FF', '2026-09-16'" in sql
    assert "-- %(ignored)s" in sql
    assert "/* %s */" in sql
    assert "'literal %s', 10 % 3, %" in sql


@pytest.mark.parametrize(
    "operation, parameters",
    [
        ("SELECT %s, %(name)s", (1, 2)),
        ("SELECT %(name)s", {}),
        ("SELECT %s", (1, 2)),
        ("SELECT 1", (1,)),
    ],
)
def test_parameter_mismatches_raise_programming_error(operation, parameters):
    with pytest.raises(dbapi.ProgrammingError):
        format_operation(operation, parameters)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), object()])
def test_unsupported_parameter_values_raise_data_error(value):
    with pytest.raises(dbapi.DataError):
        format_operation("SELECT %s", (value,))


def test_cursor_streams_rows_and_releases_result():
    data_set = FakeDataSet([("d1", 1), ("d2", 2)])
    session = FakeSession(data_set)
    cursor = Cursor(FakeConnection(), session)

    assert cursor.description is None
    assert cursor.execute("SELECT * FROM t WHERE name = %s", ("d1",)) is None
    assert session.statements == ["SELECT * FROM t WHERE name = 'd1'"]
    assert cursor.description[0][1] == dbapi.STRING
    assert cursor.description[1][1] == dbapi.NUMBER
    assert cursor.rowcount == -1
    assert cursor.fetchone() == ("d1", 1)
    assert cursor.fetchmany(0) == []
    assert cursor.fetchall() == [("d2", 2)]
    assert data_set.closed
    assert cursor.fetchone() is None
    assert cursor.fetchall() == []


def test_session_data_set_tuple_uses_stable_python_types():
    data_set = SessionDataSet.__new__(SessionDataSet)
    data_set.iotdb_rpc_data_set = SimpleNamespace(ignore_timestamp=False)
    data_set.next = lambda: RowRecord(
        np.int64(1),
        [
            Field(TSDataType.BOOLEAN, np.bool_(True)),
            Field(TSDataType.INT32, np.int32(2)),
            Field(TSDataType.DOUBLE, np.float64(3.5)),
            Field(TSDataType.STRING, b"value"),
            Field(TSDataType.BLOB, b"\x00\xff"),
            Field(TSDataType.INT64, None),
        ],
    )

    row = data_set.next_tuple()
    assert row == (1, True, 2, 3.5, "value", b"\x00\xff", None)
    assert tuple(type(value) for value in row[:4]) == (int, bool, int, float)


def test_cursor_state_and_executemany_query_errors():
    cursor = Cursor(FakeConnection(), FakeSession())
    with pytest.raises(dbapi.ProgrammingError):
        cursor.fetchone()
    with pytest.raises(dbapi.ProgrammingError):
        cursor.arraysize = -1

    result = FakeDataSet([(1, 2)])
    cursor = Cursor(FakeConnection(), FakeSession(result))
    with pytest.raises(dbapi.NotSupportedError):
        cursor.executemany("SELECT %s", [(1,)])
    assert result.closed

    cursor.close()
    with pytest.raises(dbapi.InterfaceError):
        cursor.execute("SELECT 1")


@pytest.mark.parametrize(
    "status_code, expected_exception",
    [
        (300, dbapi.NotSupportedError),
        (550, dbapi.ProgrammingError),
        (614, dbapi.DataError),
        (305, dbapi.InternalError),
        (720, dbapi.OperationalError),
        (301, dbapi.DatabaseError),
    ],
)
def test_statement_status_is_mapped_to_dbapi_exception(status_code, expected_exception):
    status = SimpleNamespace(code=status_code, message="failed")
    session = FakeSession(error=StatementExecutionException(status))
    cursor = Cursor(FakeConnection(), session)
    with pytest.raises(expected_exception):
        cursor.execute("SELECT 1")


def test_connection_defaults_to_table_and_closes_cursors(monkeypatch):
    connection_module = importlib.import_module("iotdb.dbapi.Connection")

    class SessionStub(object):
        DEFAULT_USER = "root"
        DEFAULT_PASSWORD = "root"
        DEFAULT_FETCH_SIZE = 1024
        DEFAULT_ZONE_ID = "UTC"
        instances = []

        def __init__(self, *args, **kwargs):
            self.sql_dialect = None
            self.database = None
            self.closed = False
            self.instances.append(self)

        def open(self, enable_compression):
            self.enable_compression = enable_compression

        def close(self):
            self.closed = True

    monkeypatch.setattr(connection_module, "Session", SessionStub)
    connection = connection_module.Connection("localhost", 6667, database="db1")
    session = SessionStub.instances[-1]
    assert session.sql_dialect == "table"
    assert session.database == "db1"

    cursor = connection.cursor()
    connection.close()
    assert session.closed
    with pytest.raises(dbapi.InterfaceError):
        cursor.fetchone()
    with pytest.raises(dbapi.InterfaceError):
        connection.cursor()


def test_invalid_dialect_is_an_interface_error():
    connection_module = importlib.import_module("iotdb.dbapi.Connection")
    with pytest.raises(dbapi.InterfaceError):
        connection_module.Connection("localhost", 6667, sql_dialect="invalid")


@pytest.mark.parametrize("literal", ["'100%%'", '"a%%b"', "`a%%b`"])
def test_pyformat_percent_literals(literal):
    sql = "SELECT " + literal + " WHERE v=%(value)s"
    assert format_operation(sql, {"value": "50%"}) == (
        "SELECT " + literal.replace("%%", "%") + " WHERE v='50%'"
    )


def test_percent_escapes_without_parameters():
    assert format_operation("SELECT '100%%'", {}) == "SELECT '100%'"
    assert format_operation("SELECT '100%%'", None) == "SELECT '100%%'"
    assert format_operation("SELECT %s -- 100%%", ("50%%",)) == "SELECT '50%%' -- 100%"


@pytest.mark.parametrize(
    "precision,value",
    [("ms", 123), ("us", 123456), ("ns", 123456789), ("ns", 123456000)],
)
def test_timestamp_fetch_and_bind_preserve_precision(precision, value):
    import pandas as pd

    data_set = SessionDataSet.__new__(SessionDataSet)
    data_set.iotdb_rpc_data_set = SimpleNamespace(ignore_timestamp=True)
    data_set.next = lambda: RowRecord(
        0, [Field(TSDataType.TIMESTAMP, value, timezone="UTC", precision=precision)]
    )
    timestamp = data_set.next_tuple()[0]
    assert isinstance(timestamp, datetime)
    assert (
        pd.Timestamp(timestamp).value
        == pd.Timestamp(value, unit=precision, tz="UTC").value
    )
    if precision == "ns" and value % 1000:
        assert ".123456789" in format_operation("SELECT %s", (timestamp,))
    else:
        assert type(timestamp) is datetime


@pytest.mark.parametrize("method", ["execute_statement", "execute_non_query_statement"])
def test_use_database_survives_reconnect(monkeypatch, method):
    from iotdb.Session import Session
    from iotdb.thrift.common.ttypes import TSStatus

    session = Session.init_from_node_urls(["localhost:6667"])
    session.sql_dialect = "table"
    session.database = "db_a"
    response = SimpleNamespace(status=TSStatus(code=200), columns=None, database="db_b")
    client = SimpleNamespace(
        executeStatementV2=lambda req: response,
        executeUpdateStatementV2=lambda req: response,
    )
    session._Session__client = client
    getattr(session, method)("USE db_b")

    reconnect_databases = []

    def init_connection(endpoint):
        reconnect_databases.append(session.database)
        return SimpleNamespace(
            client=client, session_id=2, statement_id=2, transport=None
        )

    monkeypatch.setattr(session, "init_connection", init_connection)
    assert session.reconnect()
    assert reconnect_databases == ["db_b"]

    response.status = TSStatus(code=701, message="invalid database")
    response.database = "db_missing"
    with pytest.raises(StatementExecutionException):
        getattr(session, method)("USE db_missing")
    assert session.database == "db_b"


def test_execute_statement_sends_fetch_size_and_timeout():
    from iotdb.Session import Session
    from iotdb.thrift.common.ttypes import TSStatus

    session = Session("localhost", 6667, fetch_size=2)
    requests = []

    def execute(request):
        requests.append(request)
        return SimpleNamespace(status=TSStatus(code=200), columns=None, database=None)

    session._Session__client = SimpleNamespace(executeStatementV2=execute)
    session.execute_statement("SHOW DATABASES", timeout=1234)
    assert requests[0].fetchSize == 2
    assert requests[0].timeout == 1234


def test_legacy_sqlalchemy_mode_keeps_the_dialect_adapter(monkeypatch):
    connection_module = importlib.import_module("iotdb.dbapi.Connection")

    class SessionStub(object):
        DEFAULT_USER = "root"
        DEFAULT_PASSWORD = "root"
        DEFAULT_FETCH_SIZE = 1024
        DEFAULT_ZONE_ID = "UTC"

        def __init__(self, *args, **kwargs):
            pass

        def open(self, enable_compression):
            pass

        def execute_statement(self, statement):
            return None

        def close(self):
            pass

    monkeypatch.setattr(connection_module, "Session", SessionStub)
    connection = dbapi.connect("localhost", 6667, sqlalchemy_mode=True)
    cursor = connection.cursor()
    assert cursor.execute("SELECT 1") is None
    connection.close()


def test_sqlalchemy_cursor_restores_compiler_time_markers():
    data_set = FakeDataSet([(100, "a", 1), (200, "b", 2)])
    session = FakeSession(data_set)
    cursor = Cursor(FakeConnection(), session, sqlalchemy_mode=True)
    cursor.execute(
        "SELECT name, value\n FROM Time Index 1\n FROM Time Name event_time"
    )

    assert session.statements == ["SELECT name, value"]
    assert cursor.description == (
        ("name", None, None, None, None, None, None),
        ("event_time", None, None, None, None, None, None),
        ("value", None, None, None, None, None, None),
    )
    assert cursor.fetchone() == ("a", 100, 1)
    assert cursor.fetchone() == ("b", 200, 2)


def test_cursor_preserves_session_column_order_and_rows():
    class OrderedDataSet(FakeDataSet):
        def get_column_names(self):
            return ["value", "Time", "name", "value"]

        def get_column_types(self):
            return [
                TSDataType.INT32,
                TSDataType.INT64,
                TSDataType.STRING,
                TSDataType.DOUBLE,
            ]

    rows = [(1, 10, "a", 1.5), (2, 20, "b", 2.5), (3, 30, "c", 3.5)]
    data_set = OrderedDataSet(rows)
    session = FakeSession(data_set)
    cursor = Cursor(FakeConnection(), session, False)
    sql = "SELECT value, Time, name, value FROM readings\nWHERE name <> 'other'"
    cursor.execute(sql)
    assert session.statements == [sql]
    assert [column[0] for column in cursor.description] == data_set.get_column_names()
    assert [column[1] for column in cursor.description] == [
        dbapi.NUMBER,
        dbapi.NUMBER,
        dbapi.STRING,
        dbapi.NUMBER,
    ]
    assert cursor.fetchone() == rows[0]
    assert cursor.fetchmany(1) == rows[1:2]
    assert cursor.fetchall() == rows[2:]
    assert data_set.closed


def test_named_parameters_allow_unused_keys_and_repeat_values():
    params = {"name": "a'b%", "unused": object()}
    assert (
        format_operation("SELECT %(name)s, %(name)s", params)
        == "SELECT 'a''b%', 'a''b%'"
    )
    assert format_operation("SELECT 1", params) == "SELECT 1"
    assert params["name"] == "a'b%"


@pytest.mark.parametrize(
    "placeholder", ["%d", "%r", "%a", "%10s", "%.2s", "%(v).2s", "%(v)d", "%", "%()s"]
)
def test_non_string_formatting_is_rejected(placeholder):
    params = {"v": "a'b"} if "(" in placeholder else ("a'b",)
    with pytest.raises(dbapi.ProgrammingError):
        format_operation("SELECT " + placeholder, params)


@pytest.mark.parametrize("params", ["value", b"value", 1, iter([1])])
def test_invalid_parameter_containers_raise_dbapi_error(params):
    with pytest.raises(dbapi.ProgrammingError):
        format_operation("SELECT %s", params)


@pytest.mark.parametrize(
    "sql,params",
    [
        ("SELECT %(missing)s", {}),
        ("SELECT %s", ()),
        ("SELECT %s", {"v": 1}),
        ("SELECT %(v)s", (1,)),
        ("SELECT %s, %(v)s", {"v": 1}),
    ],
)
def test_missing_or_mixed_parameters_raise_dbapi_error(sql, params):
    with pytest.raises(dbapi.ProgrammingError):
        format_operation(sql, params)


def test_exception_hierarchy_matches_pep249():
    assert issubclass(dbapi.Warning, Exception)
    assert issubclass(dbapi.Error, Exception)
    assert not issubclass(dbapi.Warning, dbapi.Error)
    assert issubclass(dbapi.InterfaceError, dbapi.Error)
    assert issubclass(dbapi.DatabaseError, dbapi.Error)
    for name in (
        "DataError",
        "OperationalError",
        "IntegrityError",
        "InternalError",
        "ProgrammingError",
        "NotSupportedError",
    ):
        assert issubclass(getattr(dbapi, name), dbapi.DatabaseError)


def test_tick_constructors_and_type_code_comparisons():
    import time as clock

    ticks = 1700000000
    local = clock.localtime(ticks)
    assert dbapi.DateFromTicks(ticks) == date(*local[:3])
    assert dbapi.TimeFromTicks(ticks) == time(*local[3:6])
    assert dbapi.TimestampFromTicks(ticks) == datetime(*local[:6])
    groups = [
        (dbapi.STRING, [TSDataType.TEXT, TSDataType.STRING]),
        (dbapi.BINARY, [TSDataType.BLOB]),
        (
            dbapi.NUMBER,
            [
                TSDataType.BOOLEAN,
                TSDataType.INT32,
                TSDataType.INT64,
                TSDataType.FLOAT,
                TSDataType.DOUBLE,
            ],
        ),
        (dbapi.DATETIME, [TSDataType.DATE, TSDataType.TIMESTAMP]),
    ]
    for category, codes in groups:
        for code in codes:
            assert category == code.value
            assert code.value == category
            assert not category != code.value
            assert category != -1
    assert dbapi.ROWID != TSDataType.INT64.value
    assert dbapi.Binary(memoryview(b"\x00\xff")) == b"\x00\xff"


def test_description_and_empty_result_contract():
    cursor = Cursor(FakeConnection(), FakeSession(FakeDataSet([])))
    assert cursor.description is None
    assert cursor.rowcount == -1
    cursor.execute("SELECT name, value FROM t")
    description = cursor.description
    assert description == (
        ("name", TSDataType.STRING.value, None, None, None, None, None),
        ("value", TSDataType.INT32.value, None, None, None, None, None),
    )
    assert cursor.fetchone() is None
    assert cursor.fetchmany() == []
    assert cursor.fetchall() == []
    assert cursor.description == description
    assert cursor.rowcount == -1
    for name in ("description", "rowcount"):
        with pytest.raises(AttributeError):
            setattr(cursor, name, None)


@pytest.mark.parametrize("fetch", ["fetchone", "fetchmany", "fetchall"])
def test_fetch_requires_a_result_set(fetch):
    cursor = Cursor(FakeConnection(), FakeSession())
    with pytest.raises(dbapi.Error):
        getattr(cursor, fetch)()
    cursor.execute("CREATE TABLE t (v INT32 FIELD)")
    assert cursor.description is None
    with pytest.raises(dbapi.Error):
        getattr(cursor, fetch)()


def test_arraysize_and_buffer_hints():
    cursor = Cursor(
        FakeConnection(), FakeSession(FakeDataSet([(1, 2), (3, 4), (5, 6), (7, 8)]))
    )
    assert cursor.arraysize == 1
    assert cursor.setinputsizes([None, dbapi.STRING, 100]) is None
    assert cursor.setoutputsize(100, 0) is None
    cursor.execute("SELECT * FROM t")
    assert cursor.fetchmany() == [(1, 2)]
    cursor.arraysize = 2
    assert cursor.fetchmany() == [(3, 4), (5, 6)]
    assert cursor.fetchmany() == [(7, 8)]
    assert cursor.fetchmany() == []


def test_reexecute_and_errors_discard_previous_results():
    result = FakeDataSet([(1, 2), (3, 4)])
    session = FakeSession(result)
    cursor = Cursor(FakeConnection(), session)
    cursor.execute("SELECT * FROM t")
    assert cursor.fetchone() == (1, 2)
    session.result = None
    cursor.execute("INSERT INTO t VALUES (%s)", (1,))
    assert result.closed
    assert cursor.description is None
    assert cursor.rowcount == -1
    with pytest.raises(dbapi.ProgrammingError):
        cursor.fetchone()
    session.result = FakeDataSet([(5, 6)])
    cursor.execute("SELECT * FROM t")
    with pytest.raises(dbapi.ProgrammingError):
        cursor.execute("SELECT %s", ())
    assert session.result.closed
    assert cursor.description is None
    with pytest.raises(dbapi.ProgrammingError):
        cursor.fetchone()


def test_executemany_and_closed_cursor_methods():
    session = FakeSession()
    cursor = Cursor(FakeConnection(), session)
    assert cursor.executemany("INSERT INTO t VALUES (%s)", [(1,), (2,)]) is None
    assert session.statements == [
        "INSERT INTO t VALUES (1)",
        "INSERT INTO t VALUES (2)",
    ]
    assert cursor.rowcount == -1
    assert cursor.description is None
    cursor.close()
    for method, args in [
        ("execute", ("SELECT 1",)),
        ("executemany", ("SELECT %s", [])),
        ("fetchone", ()),
        ("fetchmany", ()),
        ("fetchall", ()),
        ("setinputsizes", ([],)),
        ("setoutputsize", (10,)),
    ]:
        with pytest.raises(dbapi.Error):
            getattr(cursor, method)(*args)


def test_no_transaction_commit_and_unsupported_rollback(monkeypatch):
    from unittest.mock import Mock

    module = importlib.import_module("iotdb.dbapi.Connection")
    session = Mock()
    monkeypatch.setattr(module, "Session", Mock(return_value=session))
    connection = dbapi.connect("localhost", 6667)
    cursor = connection.cursor()
    assert connection.commit() is None
    with pytest.raises(dbapi.NotSupportedError):
        connection.rollback()
    connection.close()
    session.close.assert_called_once()
    for method in (
        connection.cursor,
        connection.commit,
        connection.rollback,
        cursor.fetchone,
    ):
        with pytest.raises(dbapi.InterfaceError):
            method()


@pytest.mark.parametrize(
    "code,expected",
    [
        (526, dbapi.OperationalError),
        (600, dbapi.OperationalError),
        (709, dbapi.OperationalError),
        (523, dbapi.InternalError),
        (507, dbapi.DatabaseError),
    ],
)
def test_resource_failures_are_not_programming_errors(code, expected):
    session = FakeSession(
        error=StatementExecutionException(SimpleNamespace(code=code, message="failed"))
    )
    cursor = Cursor(FakeConnection(), session)
    with pytest.raises(expected) as caught:
        cursor.execute("SELECT * FROM t")
    assert type(caught.value) is expected
