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

from datetime import date, datetime, timezone

import pytest

from iotdb.dbapi import DatabaseError, NotSupportedError, ProgrammingError, connect
from tests.integration.iotdb_container import IoTDBContainer


def test_table_cursor_contract_and_parameters():
    with IoTDBContainer("iotdb:dev") as database:
        connection = connect(
            database.get_container_host_ip(), database.get_exposed_port(6667)
        )
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE dbapi_table")
        cursor.execute("USE dbapi_table")
        cursor.execute(
            "CREATE TABLE readings("
            "device STRING TAG, value INT32 FIELD, note STRING FIELD)"
        )
        cursor.executemany(
            "INSERT INTO readings(time, device, value, note) VALUES (%s, %s, %s, %s)",
            [(1, "d1", 10, "IoTDB's value"), (2, "d2", 20, None)],
        )

        cursor.execute("SELECT device, value, note FROM readings ORDER BY time")
        assert cursor.description is not None
        assert len(cursor.description) == 3
        assert cursor.fetchone() == ("d1", 10, "IoTDB's value")
        assert cursor.fetchmany(0) == []
        assert cursor.fetchall() == [("d2", 20, None)]
        assert cursor.fetchone() is None
        assert cursor.rowcount == -1

        cursor.execute("SELECT * FROM readings")
        with pytest.raises(NotSupportedError):
            cursor.executemany("SELECT * FROM readings WHERE value = %s", [(10,)])
        with pytest.raises((ProgrammingError, DatabaseError)):
            cursor.execute("SELECT FROM readings")

        cursor.execute("DROP DATABASE dbapi_table")
        cursor.close()
        connection.close()


def test_tree_cursor_contract_and_parameters():
    with IoTDBContainer("iotdb:dev") as database:
        connection = connect(
            database.get_container_host_ip(),
            database.get_exposed_port(6667),
            sql_dialect="tree",
            zone_id="UTC",
        )
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE root.dbapi_tree")
        cursor.execute(
            "CREATE TIMESERIES root.dbapi_tree.device.temperature "
            "WITH DATATYPE=FLOAT, ENCODING=RLE"
        )
        cursor.executemany(
            "INSERT INTO root.dbapi_tree.device(timestamp, temperature) VALUES (%s, %s)",
            [(1, 0.3), (2, 0.4)],
        )

        cursor.execute("SELECT temperature FROM root.dbapi_tree.device")
        assert cursor.fetchone() == (
            datetime(1970, 1, 1, microsecond=1000, tzinfo=timezone.utc),
            pytest.approx(0.3),
        )
        assert cursor.fetchall() == [
            (
                datetime(1970, 1, 1, microsecond=2000, tzinfo=timezone.utc),
                pytest.approx(0.4),
            )
        ]
        with pytest.raises(ProgrammingError):
            cursor.fetchmany(-1)

        cursor.execute("DELETE DATABASE root.dbapi_tree")
        cursor.close()
        connection.close()


def test_table_paging_types_and_multiple_cursors():
    with IoTDBContainer("iotdb:dev") as database:
        with connect(
            database.get_container_host_ip(),
            database.get_exposed_port(6667),
            fetch_size=2,
            zone_id="UTC",
        ) as connection:
            cursor = connection.cursor()
            cursor.execute("CREATE DATABASE dbapi_paging")
            cursor.execute("USE dbapi_paging")
            try:
                cursor.execute(
                    "CREATE TABLE readings(device STRING TAG, value INT64 FIELD, "
                    "flag BOOLEAN FIELD, day DATE FIELD, stamp TIMESTAMP FIELD, payload BLOB FIELD)"
                )
                stamp = datetime(2026, 9, 17, 1, 2, 3, 123000, tzinfo=timezone.utc)
                day = date(2026, 9, 17)
                rows = [
                    (
                        i,
                        "d1",
                        i if i % 2 else None,
                        bool(i % 2),
                        day,
                        stamp,
                        b"\x00\xff",
                    )
                    for i in range(9)
                ]
                cursor.executemany(
                    "INSERT INTO readings(time, device, value, flag, day, stamp, payload) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    rows,
                )
                cursor.execute(
                    "SELECT value, flag, day, stamp, payload FROM readings ORDER BY time"
                )
                other = connection.cursor()
                other.execute("SELECT count(*) FROM readings")
                assert other.fetchone() == (9,)
                other.close()
                result = cursor.fetchmany(3) + cursor.fetchall()
                assert result == [row[2:] for row in rows]
                assert cursor.fetchone() is None
            finally:
                cursor.execute("DROP DATABASE dbapi_paging")


@pytest.mark.parametrize("dialect", ["table", "tree"])
def test_bound_values_round_trip_without_manual_escaping(dialect):
    from iotdb.dbapi import STRING

    with IoTDBContainer("iotdb:dev") as database:
        with connect(
            database.get_container_host_ip(),
            database.get_exposed_port(6667),
            sql_dialect=dialect,
            fetch_size=2,
        ) as connection:
            cursor = connection.cursor()
            name = "dbapi_parameters" if dialect == "table" else "root.dbapi_parameters"
            cursor.execute("CREATE DATABASE " + name)
            try:
                if dialect == "table":
                    cursor.execute("USE " + name)
                    cursor.execute("CREATE TABLE readings(note STRING FIELD)")
                    target = "readings"
                else:
                    target = name + ".d"
                    cursor.execute(
                        "CREATE TIMESERIES "
                        + target
                        + ".note WITH DATATYPE=TEXT, ENCODING=PLAIN"
                    )
                values = [
                    "O'Reilly",
                    "",
                    "%s %(name)s %%",
                    "a\\b\nc -- /* */",
                    "中文🙂",
                    "x'); SELECT 1 --",
                ]
                cursor.executemany(
                    "INSERT INTO " + target + "(time, note) VALUES (%s, %s)",
                    list(enumerate(values, start=1)),
                )
                cursor.execute("SELECT note FROM " + target + " ORDER BY time")
                assert cursor.description[-1][1] == STRING
                assert [row[-1] for row in cursor.fetchall()] == values
                # Block comments are table SQL syntax; tree SQL rejects them.
                comment = " /* %%s 100%% */" if dialect == "table" else ""
                cursor.execute(
                    "SELECT note FROM " + target + " WHERE time=%(time)s" + comment,
                    {"time": 1, "unused": object()},
                )
                assert cursor.fetchone()[-1] == values[0]
                assert cursor.fetchone() is None
                cursor.execute(
                    "SELECT note FROM "
                    + target
                    + " WHERE note='%%s %%(name)s %%%%' AND time=%s",
                    (3,),
                )
                assert cursor.fetchone()[-1] == values[2]
                with pytest.raises(NotSupportedError):
                    connection.rollback()
                cursor.execute("SELECT note FROM " + target + " WHERE time=1")
                assert cursor.fetchone()[-1] == values[0]
            finally:
                cursor.execute(
                    ("DROP DATABASE " if dialect == "table" else "DELETE DATABASE ")
                    + name
                )
