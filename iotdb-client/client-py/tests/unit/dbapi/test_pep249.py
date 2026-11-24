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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import date, datetime, time
import pytest

import iotdb.dbapi as dbapi
from iotdb.dbapi.Cursor import Cursor
from iotdb.utils.IoTDBConstants import TSDataType


class FakeConnection:
    is_close = False


class FakeDataSet:
    def __init__(self, rows):
        self.rows = iter(rows)
        self.closed = False

    def get_column_names(self):
        return ["device", "value"]

    def get_column_types(self):
        return [TSDataType.STRING, TSDataType.INT32]

    def next_tuple(self):
        return next(self.rows, None)

    def close_operation_handle(self):
        self.closed = True


class FakeSession:
    def __init__(self, result=None):
        self.result = result
        self.statements = []

    def execute_statement(self, statement, timeout=0, fetch_size=None):
        self.statements.append(statement)
        return self.result


@pytest.fixture
def cursor_with_rows():
    data_set = FakeDataSet([("d1", 10), ("d2", 20)])
    session = FakeSession(data_set)
    return Cursor(FakeConnection(), session), session, data_set


def test_pep249_module_attributes_and_connect(monkeypatch):
    from importlib import import_module

    connection_module = import_module("iotdb.dbapi.Connection")

    class SessionStub:
        DEFAULT_USER = "root"
        DEFAULT_PASSWORD = "root"
        DEFAULT_FETCH_SIZE = 1024
        DEFAULT_ZONE_ID = "UTC"

        def __init__(self, *args, **kwargs):
            self.sql_dialect = None
            self.database = None
            self.closed = False

        def open(self, enable_compression):
            pass

        def close(self):
            self.closed = True

    monkeypatch.setattr(connection_module, "Session", SessionStub)
    assert dbapi.apilevel == "2.0"
    assert dbapi.threadsafety == 1
    assert dbapi.paramstyle == "pyformat"

    connection = dbapi.connect("localhost", 6667)
    assert connection.is_close is False
    connection.close()


def test_pep249_exception_hierarchy():
    assert issubclass(dbapi.Warning, Exception)
    assert issubclass(dbapi.Error, Exception)
    assert not issubclass(dbapi.Warning, dbapi.Error)
    assert issubclass(dbapi.InterfaceError, dbapi.Error)
    assert issubclass(dbapi.DatabaseError, dbapi.Error)
    for exception_type in (
        dbapi.DataError,
        dbapi.OperationalError,
        dbapi.IntegrityError,
        dbapi.InternalError,
        dbapi.ProgrammingError,
        dbapi.NotSupportedError,
    ):
        assert issubclass(exception_type, dbapi.DatabaseError)


def test_pep249_connection_methods_and_cursor_cleanup(monkeypatch):
    from importlib import import_module

    from unittest.mock import Mock

    connection_module = import_module("iotdb.dbapi.Connection")
    session = Mock()
    session.execute_statement.return_value = None
    monkeypatch.setattr(connection_module, "Session", Mock(return_value=session))
    connection = dbapi.connect("localhost", 6667)
    cursor = connection.cursor()

    assert connection.commit() is None
    with pytest.raises(dbapi.NotSupportedError):
        connection.rollback()
    connection.close()
    session.close.assert_called_once()
    with pytest.raises(dbapi.InterfaceError):
        cursor.fetchone()
    with pytest.raises(dbapi.InterfaceError):
        connection.cursor()


def test_pep249_cursor_metadata_fetching_and_buffer_hints(cursor_with_rows):
    cursor, session, data_set = cursor_with_rows

    assert cursor.arraysize == 1
    assert cursor.description is None
    assert cursor.rowcount == -1
    assert cursor.setinputsizes([dbapi.STRING, dbapi.NUMBER]) is None
    assert cursor.setoutputsize(1024, 1) is None

    assert cursor.execute("SELECT device, value FROM readings") is None
    assert session.statements == ["SELECT device, value FROM readings"]
    assert cursor.description == (
        ("device", TSDataType.STRING.value, None, None, None, None, None),
        ("value", TSDataType.INT32.value, None, None, None, None, None),
    )
    cursor.arraysize = 2
    assert cursor.fetchmany() == [("d1", 10), ("d2", 20)]
    assert cursor.fetchone() is None
    assert cursor.fetchall() == []
    assert data_set.closed


def test_pep249_execute_parameters_and_executemany():
    session = FakeSession()
    cursor = Cursor(FakeConnection(), session)

    assert cursor.execute("SELECT %s", ("d1",)) is None
    assert cursor.executemany("INSERT INTO t VALUES (%s)", [(1,), (2,)]) is None
    assert session.statements == [
        "SELECT 'd1'",
        "INSERT INTO t VALUES (1)",
        "INSERT INTO t VALUES (2)",
    ]


def test_pep249_fetch_requires_query_result():
    cursor = Cursor(FakeConnection(), FakeSession())
    for method in (cursor.fetchone, cursor.fetchmany, cursor.fetchall):
        with pytest.raises(dbapi.ProgrammingError):
            method()

    cursor.execute("CREATE TABLE t (value INT32 FIELD)")
    for method in (cursor.fetchone, cursor.fetchmany, cursor.fetchall):
        with pytest.raises(dbapi.ProgrammingError):
            method()


def test_pep249_optional_cursor_methods_and_iteration(cursor_with_rows):
    cursor, _, _ = cursor_with_rows
    assert not hasattr(cursor, "callproc")
    assert not hasattr(cursor, "nextset")
    assert not hasattr(cursor, "scroll")

    cursor.execute("SELECT device, value FROM readings")
    with pytest.warns(Warning):
        iterator = iter(cursor)
    assert next(iterator) == ("d1", 10)


def test_pep249_type_constructors_and_type_categories():
    assert dbapi.Date(2026, 11, 23) == date(2026, 11, 23)
    assert dbapi.Time(12, 30, 0) == time(12, 30, 0)
    assert dbapi.Timestamp(2026, 11, 23, 12, 30, 0) == datetime(2026, 11, 23, 12, 30, 0)
    assert dbapi.Binary(bytearray((0, 255))) == b"\x00\xff"

    type_groups = (
        (dbapi.STRING, (TSDataType.TEXT, TSDataType.STRING)),
        (dbapi.BINARY, (TSDataType.BLOB,)),
        (
            dbapi.NUMBER,
            (
                TSDataType.BOOLEAN,
                TSDataType.INT32,
                TSDataType.INT64,
                TSDataType.FLOAT,
                TSDataType.DOUBLE,
            ),
        ),
        (dbapi.DATETIME, (TSDataType.DATE, TSDataType.TIMESTAMP)),
    )
    for type_object, data_types in type_groups:
        for data_type in data_types:
            assert type_object == data_type.value
    assert dbapi.ROWID != TSDataType.INT64.value


def test_pep249_tick_constructors():
    import time as clock

    ticks = 1700000000
    local_time = clock.localtime(ticks)
    assert dbapi.DateFromTicks(ticks) == date(*local_time[:3])
    assert dbapi.TimeFromTicks(ticks) == time(*local_time[3:6])
    assert dbapi.TimestampFromTicks(ticks) == datetime(*local_time[:6])


def test_pep249_context_managers_close_objects(monkeypatch):
    from importlib import import_module

    from unittest.mock import Mock

    connection_module = import_module("iotdb.dbapi.Connection")
    session = Mock()
    session.execute_statement.return_value = None
    monkeypatch.setattr(connection_module, "Session", Mock(return_value=session))

    with dbapi.connect("localhost", 6667) as connection:
        with connection.cursor() as cursor:
            assert cursor.execute("CREATE TABLE t (value INT32 FIELD)") is None
    assert connection.is_close
    with pytest.raises(dbapi.InterfaceError):
        cursor.execute("SELECT 1")
