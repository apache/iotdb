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
#

from iotdb.table_session import TableSession, TableSessionConfig
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.Tablet import Tablet, ColumnType
from datetime import date
from .iotdb_container import IoTDBContainer

# Test query data
def test_query_data():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        config = TableSessionConfig(
            node_urls=[f"{db.get_container_host_ip()}:{db.get_exposed_port(6667)}"]
        )
        session = TableSession(config)

        # Preparation before testing
        session.execute_non_query_statement(
            "create database test_insert_relational_tablet_tablet"
        )
        session.execute_non_query_statement("use test_insert_relational_tablet_tablet")
        session.execute_non_query_statement(
            "create table table_b("
            "tag1 STRING TAG, tag2 STRING TAG, tag3 STRING TAG, "
            "attr1 string attribute, attr2 string attribute, attr3 string attribute,"
            "BOOLEAN BOOLEAN FIELD, INT32 INT32 FIELD, INT64 INT64 FIELD, FLOAT FLOAT FIELD, DOUBLE DOUBLE FIELD,"
            "TEXT TEXT FIELD, TIMESTAMP TIMESTAMP FIELD, DATE DATE FIELD, BLOB BLOB FIELD, STRING STRING FIELD)"
        )

        # 1、General scenario
        expect = 10
        table_name = "table_b"
        column_names = [
            "tag1",
            "tag2",
            "tag3",
            "attr1",
            "attr2",
            "attr3",
            "BOOLEAN",
            "INT32",
            "INT64",
            "FLOAT",
            "DOUBLE",
            "TEXT",
            "TIMESTAMP",
            "DATE",
            "BLOB",
            "STRING",
        ]
        data_types = [
            TSDataType.STRING,
            TSDataType.STRING,
            TSDataType.STRING,
            TSDataType.STRING,
            TSDataType.STRING,
            TSDataType.STRING,
            TSDataType.BOOLEAN,
            TSDataType.INT32,
            TSDataType.INT64,
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
            TSDataType.TEXT,
            TSDataType.TIMESTAMP,
            TSDataType.DATE,
            TSDataType.BLOB,
            TSDataType.STRING,
        ]
        column_types = [
            ColumnType.TAG,
            ColumnType.TAG,
            ColumnType.TAG,
            ColumnType.ATTRIBUTE,
            ColumnType.ATTRIBUTE,
            ColumnType.ATTRIBUTE,
            ColumnType.FIELD,
            ColumnType.FIELD,
            ColumnType.FIELD,
            ColumnType.FIELD,
            ColumnType.FIELD,
            ColumnType.FIELD,
            ColumnType.FIELD,
            ColumnType.FIELD,
            ColumnType.FIELD,
            ColumnType.FIELD,
        ]
        timestamps = []
        values = []
        for row_b in range(10):
            timestamps.append(row_b)
        values.append(
            [
                "tag1：" + str(row_b),
                "tag2：" + str(row_b),
                "tag3：" + str(row_b),
                "attr1:" + str(row_b),
                "attr2:" + str(row_b),
                "attr3:" + str(row_b),
                False,
                0,
                0,
                0.0,
                0.0,
                "1234567890",
                0,
                date(1970, 1, 1),
                "1234567890".encode("utf-8"),
                "1234567890",
            ]
        )
        values.append(
            [
                "tag1：" + str(row_b),
                "tag2：" + str(row_b),
                "tag3：" + str(row_b),
                "attr1:" + str(row_b),
                "attr2:" + str(row_b),
                "attr3:" + str(row_b),
                True,
                -2147483648,
                -9223372036854775808,
                -0.12345678,
                -0.12345678901234567,
                "abcdefghijklmnopqrstuvwsyz",
                -9223372036854775808,
                date(1000, 1, 1),
                "abcdefghijklmnopqrstuvwsyz".encode("utf-8"),
                "abcdefghijklmnopqrstuvwsyz",
            ]
        )
        values.append(
            [
                "tag1：" + str(row_b),
                "tag2：" + str(row_b),
                "tag3：" + str(row_b),
                "attr1:" + str(row_b),
                "attr2:" + str(row_b),
                "attr3:" + str(row_b),
                True,
                2147483647,
                9223372036854775807,
                0.123456789,
                0.12345678901234567,
                "!@#$%^&*()_+}{|:'`~-=[];,./<>?~",
                9223372036854775807,
                date(9999, 12, 31),
                "!@#$%^&*()_+}{|:`~-=[];,./<>?~".encode("utf-8"),
                "!@#$%^&*()_+}{|:`~-=[];,./<>?~",
            ]
        )
        values.append(
            [
                "tag1：" + str(row_b),
                "tag2：" + str(row_b),
                "tag3：" + str(row_b),
                "attr1:" + str(row_b),
                "attr2:" + str(row_b),
                "attr3:" + str(row_b),
                True,
                1,
                1,
                1.0,
                1.0,
                "没问题",
                1,
                date(1970, 1, 1),
                "没问题".encode("utf-8"),
                "没问题",
            ]
        )
        values.append(
            [
                "tag1：" + str(row_b),
                "tag2：" + str(row_b),
                "tag3：" + str(row_b),
                "attr1:" + str(row_b),
                "attr2:" + str(row_b),
                "attr3:" + str(row_b),
                True,
                -1,
                -1,
                1.1234567,
                1.1234567890123456,
                "！@#￥%……&*（）——|：“《》？·【】、；‘，。/",
                11,
                date(1970, 1, 1),
                "！@#￥%……&*（）——|：“《》？·【】、；‘，。/".encode("utf-8"),
                "！@#￥%……&*（）——|：“《》？·【】、；‘，。/",
            ]
        )
        values.append(
            [
                "tag1：" + str(row_b),
                "tag2：" + str(row_b),
                "tag3：" + str(row_b),
                "attr1:" + str(row_b),
                "attr2:" + str(row_b),
                "attr3:" + str(row_b),
                True,
                10,
                11,
                4.123456,
                4.123456789012345,
                "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题",
                11,
                date(1970, 1, 1),
                "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题".encode(
                    "utf-8"
                ),
                "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题",
            ]
        )
        values.append(
            [
                "tag1：" + str(row_b),
                "tag2：" + str(row_b),
                "tag3：" + str(row_b),
                "attr1:" + str(row_b),
                "attr2:" + str(row_b),
                "attr3:" + str(row_b),
                True,
                -10,
                -11,
                12.12345,
                12.12345678901234,
                "test01",
                11,
                date(1970, 1, 1),
                "Hello, World!".encode("utf-8"),
                "string01",
            ]
        )
        values.append(
            [
                "tag1：" + str(row_b),
                "tag2：" + str(row_b),
                "tag3：" + str(row_b),
                "attr1:" + str(row_b),
                "attr2:" + str(row_b),
                "attr3:" + str(row_b),
                None,
                None,
                None,
                None,
                None,
                "",
                None,
                date(1970, 1, 1),
                "".encode("utf-8"),
                "",
            ]
        )
        values.append(
            [
                "tag1：" + str(row_b),
                "tag2：" + str(row_b),
                "tag3：" + str(row_b),
                "attr1:" + str(row_b),
                "attr2:" + str(row_b),
                "attr3:" + str(row_b),
                True,
                -0,
                -0,
                -0.0,
                -0.0,
                "    ",
                11,
                date(1970, 1, 1),
                "    ".encode("utf-8"),
                "    ",
            ]
        )
        values.append(
            [
                "tag1：" + str(row_b),
                "tag2：" + str(row_b),
                "tag3：" + str(row_b),
                "attr1:" + str(row_b),
                "attr2:" + str(row_b),
                "attr3:" + str(row_b),
                True,
                10,
                11,
                1.1,
                10011.1,
                "test01",
                11,
                date(1970, 1, 1),
                "Hello, World!".encode("utf-8"),
                "string01",
            ]
        )
        tablet = Tablet(
            table_name, column_names, data_types, values, timestamps, column_types
        )
        session.insert(tablet)
        # Calculate the number of rows in the actual time series
        actual = 0
        with session.execute_query_statement(
            "select * from table_b"
        ) as session_data_set:
            print(session_data_set.get_column_names())
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        # Determine whether it meets expectations
        assert expect == actual

        with session.execute_query_statement(
            "select * from table_b"
        ) as session_data_set:
            df = session_data_set.todf()
        # Determine whether it meets expectations
        assert expect == actual

        with session.execute_query_statement(
            "select " + ','.join(column_names) + " from table_b"
        ) as session_data_set:
            assert session_data_set.get_column_names() == column_names
            while session_data_set.has_next():
                rowRecord = session_data_set.next()
                actual = actual + 1
        # Determine whether it meets expectations
        assert expect == actual

        session.close()
