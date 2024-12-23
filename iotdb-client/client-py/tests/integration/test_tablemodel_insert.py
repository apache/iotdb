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

import numpy as np
from iotdb.table_session import TableSession, TableSessionConfig
from iotdb.utils.BitMap import BitMap
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.Tablet import Tablet, ColumnType
from iotdb.utils.NumpyTablet import NumpyTablet
from datetime import date
from .iotdb_container import IoTDBContainer


# Test inserting tablet data
def test_insert_use_tablet():
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
            "create table table_a("
            '"地区" STRING ID, "厂号" STRING ID, "设备号" STRING ID, '
            '"日期" string attribute, "时间" string attribute, "负责人" string attribute,'
            '"测点1" BOOLEAN MEASUREMENT, "测点2" INT32 MEASUREMENT, "测点3" INT64 MEASUREMENT, "测点4" FLOAT MEASUREMENT, "测点5" DOUBLE MEASUREMENT,'
            '"测点6" TEXT MEASUREMENT, "测点7" TIMESTAMP MEASUREMENT, "测点8" DATE MEASUREMENT, "测点9" BLOB MEASUREMENT, "测点10" STRING MEASUREMENT)'
        )
        session.execute_non_query_statement(
            "create table table_b("
            "id1 STRING ID, id2 STRING ID, id3 STRING ID, "
            "attr1 string attribute, attr2 string attribute, attr3 string attribute,"
            "BOOLEAN BOOLEAN MEASUREMENT, INT32 INT32 MEASUREMENT, INT64 INT64 MEASUREMENT, FLOAT FLOAT MEASUREMENT, DOUBLE DOUBLE MEASUREMENT,"
            "TEXT TEXT MEASUREMENT, TIMESTAMP TIMESTAMP MEASUREMENT, DATE DATE MEASUREMENT, BLOB BLOB MEASUREMENT, STRING STRING MEASUREMENT)"
        )

        # 1、General scenario
        expect = 10
        table_name = "table_b"
        column_names = [
            "id1",
            "id2",
            "id3",
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
            ColumnType.ID,
            ColumnType.ID,
            ColumnType.ID,
            ColumnType.ATTRIBUTE,
            ColumnType.ATTRIBUTE,
            ColumnType.ATTRIBUTE,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
        ]
        timestamps = []
        values = []
        for row_b in range(10):
            timestamps.append(row_b)
        values.append(
            [
                "id1：" + str(row_b),
                "id2：" + str(row_b),
                "id3：" + str(row_b),
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
                "id1：" + str(row_b),
                "id2：" + str(row_b),
                "id3：" + str(row_b),
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
                "id1：" + str(row_b),
                "id2：" + str(row_b),
                "id3：" + str(row_b),
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
                "id1：" + str(row_b),
                "id2：" + str(row_b),
                "id3：" + str(row_b),
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
                "id1：" + str(row_b),
                "id2：" + str(row_b),
                "id3：" + str(row_b),
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
                "id1：" + str(row_b),
                "id2：" + str(row_b),
                "id3：" + str(row_b),
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
                "id1：" + str(row_b),
                "id2：" + str(row_b),
                "id3：" + str(row_b),
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
                "id1：" + str(row_b),
                "id2：" + str(row_b),
                "id3：" + str(row_b),
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
                "id1：" + str(row_b),
                "id2：" + str(row_b),
                "id3：" + str(row_b),
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
                "id1：" + str(row_b),
                "id2：" + str(row_b),
                "id3：" + str(row_b),
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

        # 2、Including Chinese scenes
        expect = 10
        table_name = "table_a"
        column_names = [
            "地区",
            "厂号",
            "设备号",
            "日期",
            "时间",
            "负责人",
            "测点1",
            "测点2",
            "测点3",
            "测点4",
            "测点5",
            "测点6",
            "测点7",
            "测点8",
            "测点9",
            "测点10",
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
            ColumnType.ID,
            ColumnType.ID,
            ColumnType.ID,
            ColumnType.ATTRIBUTE,
            ColumnType.ATTRIBUTE,
            ColumnType.ATTRIBUTE,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
        ]
        timestamps = []
        values = []
        for row_a in range(10):
            timestamps.append(row_a)
        values.append(
            [
                "id1：" + str(row_a),
                "id2：" + str(row_a),
                "id3：" + str(row_a),
                "attr1:" + str(row_a),
                "attr2:" + str(row_a),
                "attr3:" + str(row_a),
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
                "id1：" + str(row_a),
                "id2：" + str(row_a),
                "id3：" + str(row_a),
                "attr1:" + str(row_a),
                "attr2:" + str(row_a),
                "attr3:" + str(row_a),
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
                "id1：" + str(row_a),
                "id2：" + str(row_a),
                "id3：" + str(row_a),
                "attr1:" + str(row_a),
                "attr2:" + str(row_a),
                "attr3:" + str(row_a),
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
                "id1：" + str(row_a),
                "id2：" + str(row_a),
                "id3：" + str(row_a),
                "attr1:" + str(row_a),
                "attr2:" + str(row_a),
                "attr3:" + str(row_a),
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
                "id1：" + str(row_a),
                "id2：" + str(row_a),
                "id3：" + str(row_a),
                "attr1:" + str(row_a),
                "attr2:" + str(row_a),
                "attr3:" + str(row_a),
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
                "id1：" + str(row_a),
                "id2：" + str(row_a),
                "id3：" + str(row_a),
                "attr1:" + str(row_a),
                "attr2:" + str(row_a),
                "attr3:" + str(row_a),
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
                "id1：" + str(row_a),
                "id2：" + str(row_a),
                "id3：" + str(row_a),
                "attr1:" + str(row_a),
                "attr2:" + str(row_a),
                "attr3:" + str(row_a),
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
                "id1：" + str(row_a),
                "id2：" + str(row_a),
                "id3：" + str(row_a),
                "attr1:" + str(row_a),
                "attr2:" + str(row_a),
                "attr3:" + str(row_a),
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
                "id1：" + str(row_a),
                "id2：" + str(row_a),
                "id3：" + str(row_a),
                "attr1:" + str(row_a),
                "attr2:" + str(row_a),
                "attr3:" + str(row_a),
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
                "id1：" + str(row_a),
                "id2：" + str(row_a),
                "id3：" + str(row_a),
                "attr1:" + str(row_a),
                "attr2:" + str(row_a),
                "attr3:" + str(row_a),
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
            "select * from table_a"
        ) as session_data_set:
            print(session_data_set.get_column_names())
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        # Determine whether it meets expectations
        assert expect == actual

        session.close()


# Test inserting NumpyTablet data
def test_insert_relational_tablet_use_numpy_tablet():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        config = TableSessionConfig(
            node_urls=[f"{db.get_container_host_ip()}:{db.get_exposed_port(6667)}"]
        )
        session = TableSession(config)

        # Preparation before testing
        session.execute_non_query_statement(
            "create database test_insert_relational_tablet_use_numpy_tablet"
        )
        session.execute_non_query_statement(
            "use test_insert_relational_tablet_use_numpy_tablet"
        )
        session.execute_non_query_statement(
            "create table table_b("
            "id1 STRING ID, id2 STRING ID, id3 STRING ID, "
            "attr1 string attribute, attr2 string attribute, attr3 string attribute,"
            "BOOLEAN BOOLEAN MEASUREMENT, INT32 INT32 MEASUREMENT, INT64 INT64 MEASUREMENT, FLOAT FLOAT MEASUREMENT, DOUBLE DOUBLE MEASUREMENT,"
            "TEXT TEXT MEASUREMENT, TIMESTAMP TIMESTAMP MEASUREMENT, DATE DATE MEASUREMENT, BLOB BLOB MEASUREMENT, STRING STRING MEASUREMENT)"
        )
        session.execute_non_query_statement(
            "create table table_d("
            "id1 STRING ID, id2 STRING ID, id3 STRING ID, "
            "attr1 string attribute, attr2 string attribute, attr3 string attribute,"
            "BOOLEAN BOOLEAN MEASUREMENT, INT32 INT32 MEASUREMENT, INT64 INT64 MEASUREMENT, FLOAT FLOAT MEASUREMENT, DOUBLE DOUBLE MEASUREMENT,"
            "TEXT TEXT MEASUREMENT, TIMESTAMP TIMESTAMP MEASUREMENT, DATE DATE MEASUREMENT, BLOB BLOB MEASUREMENT, STRING STRING MEASUREMENT)"
        )

        # 1、No null
        expect = 10
        table_name = "table_b"
        column_names = [
            "id1",
            "id2",
            "id3",
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
            ColumnType.ID,
            ColumnType.ID,
            ColumnType.ID,
            ColumnType.ATTRIBUTE,
            ColumnType.ATTRIBUTE,
            ColumnType.ATTRIBUTE,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
        ]
        np_timestamps = np.arange(0, 10, dtype=np.dtype(">i8"))
        np_values = [
            np.array(["id1:{}".format(i) for i in range(0, 10)]),
            np.array(["id2:{}".format(i) for i in range(0, 10)]),
            np.array(["id3:{}".format(i) for i in range(0, 10)]),
            np.array(["attr1:{}".format(i) for i in range(0, 10)]),
            np.array(["attr2:{}".format(i) for i in range(0, 10)]),
            np.array(["attr3:{}".format(i) for i in range(0, 10)]),
            np.array(
                [False, True, False, True, False, True, False, True, False, True],
                TSDataType.BOOLEAN.np_dtype(),
            ),
            np.array(
                [0, -2147483648, 2147483647, 1, -1, 10, -10, -0, 10, 10],
                TSDataType.INT32.np_dtype(),
            ),
            np.array(
                [
                    0,
                    -9223372036854775808,
                    9223372036854775807,
                    1,
                    -1,
                    10,
                    -10,
                    -0,
                    10,
                    10,
                ],
                TSDataType.INT64.np_dtype(),
            ),
            np.array(
                [
                    0.0,
                    -0.12345678,
                    0.123456789,
                    1.0,
                    1.1234567,
                    4.123456,
                    12.12345,
                    -0.0,
                    1.1,
                    1.1,
                ],
                TSDataType.FLOAT.np_dtype(),
            ),
            np.array(
                [
                    0.0,
                    -0.12345678901234567,
                    0.12345678901234567,
                    1.0,
                    1.1234567890123456,
                    4.123456789012345,
                    12.12345678901234,
                    -0.0,
                    1.1,
                    1.1,
                ],
                TSDataType.DOUBLE.np_dtype(),
            ),
            np.array(
                [
                    "1234567890",
                    "abcdefghijklmnopqrstuvwsyz",
                    "!@#$%^&*()_+}{|:'`~-=[];,./<>?~",
                    "没问题",
                    "！@#￥%……&*（）——|：“《》？·【】、；‘，。/",
                    "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题",
                    "",
                    "    ",
                    "test01",
                    "test01",
                ],
                TSDataType.TEXT.np_dtype(),
            ),
            np.array(
                [
                    0,
                    -9223372036854775808,
                    9223372036854775807,
                    1,
                    -1,
                    10,
                    -10,
                    -0,
                    10,
                    10,
                ],
                TSDataType.TIMESTAMP.np_dtype(),
            ),
            np.array(
                [
                    date(1970, 1, 1),
                    date(1000, 1, 1),
                    date(9999, 12, 31),
                    date(1970, 1, 1),
                    date(1970, 1, 1),
                    date(1970, 1, 1),
                    date(1970, 1, 1),
                    date(1970, 1, 1),
                    date(1970, 1, 1),
                    date(1970, 1, 1),
                ],
                TSDataType.DATE.np_dtype(),
            ),
            np.array(
                [
                    "1234567890".encode("utf-8"),
                    "abcdefghijklmnopqrstuvwsyz".encode("utf-8"),
                    "!@#$%^&*()_+}{|:`~-=[];,./<>?~".encode("utf-8"),
                    "没问题".encode("utf-8"),
                    "！@#￥%……&*（）——|：“《》？·【】、；‘，。/".encode("utf-8"),
                    "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题".encode(
                        "utf-8"
                    ),
                    "".encode("utf-8"),
                    "   ".encode("utf-8"),
                    "1234567890".encode("utf-8"),
                    "1234567890".encode("utf-8"),
                ],
                TSDataType.BLOB.np_dtype(),
            ),
            np.array(
                [
                    "1234567890",
                    "abcdefghijklmnopqrstuvwsyz",
                    "!@#$%^&*()_+}{|:'`~-=[];,./<>?~",
                    "没问题",
                    "！@#￥%……&*（）——|：“《》？·【】、；‘，。/",
                    "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题",
                    "",
                    "    ",
                    "test01",
                    "test01",
                ],
                TSDataType.STRING.np_dtype(),
            ),
        ]
        np_tablet = NumpyTablet(
            table_name,
            column_names,
            data_types,
            np_values,
            np_timestamps,
            column_types=column_types,
        )
        session.insert(np_tablet)
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

        # 2、Contains null
        expect = 10
        table_name = "table_d"
        column_names = [
            "id1",
            "id2",
            "id3",
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
            ColumnType.ID,
            ColumnType.ID,
            ColumnType.ID,
            ColumnType.ATTRIBUTE,
            ColumnType.ATTRIBUTE,
            ColumnType.ATTRIBUTE,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
            ColumnType.MEASUREMENT,
        ]
        np_timestamps = np.arange(0, 10, dtype=np.dtype(">i8"))
        np_values = [
            np.array(["id1:{}".format(i) for i in range(0, 10)]),
            np.array(["id2:{}".format(i) for i in range(0, 10)]),
            np.array(["id3:{}".format(i) for i in range(0, 10)]),
            np.array(["attr1:{}".format(i) for i in range(0, 10)]),
            np.array(["attr2:{}".format(i) for i in range(0, 10)]),
            np.array(["attr3:{}".format(i) for i in range(0, 10)]),
            np.array(
                [False, True, False, True, False, True, False, True, False, True],
                TSDataType.BOOLEAN.np_dtype(),
            ),
            np.array(
                [0, -2147483648, 2147483647, 1, -1, 10, -10, -0, 10, 10],
                TSDataType.INT32.np_dtype(),
            ),
            np.array(
                [
                    0,
                    -9223372036854775808,
                    9223372036854775807,
                    1,
                    -1,
                    10,
                    -10,
                    -0,
                    10,
                    10,
                ],
                TSDataType.INT64.np_dtype(),
            ),
            np.array(
                [
                    0.0,
                    -0.12345678,
                    0.123456789,
                    1.0,
                    1.1234567,
                    4.123456,
                    12.12345,
                    -0.0,
                    1.1,
                    1.1,
                ],
                TSDataType.FLOAT.np_dtype(),
            ),
            np.array(
                [
                    0.0,
                    -0.12345678901234567,
                    0.12345678901234567,
                    1.0,
                    1.1234567890123456,
                    4.123456789012345,
                    12.12345678901234,
                    -0.0,
                    1.1,
                    1.1,
                ],
                TSDataType.DOUBLE.np_dtype(),
            ),
            np.array(
                [
                    "1234567890",
                    "abcdefghijklmnopqrstuvwsyz",
                    "!@#$%^&*()_+}{|:'`~-=[];,./<>?~",
                    "没问题",
                    "！@#￥%……&*（）——|：“《》？·【】、；‘，。/",
                    "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题",
                    "",
                    "    ",
                    "test01",
                    "test01",
                ],
                TSDataType.TEXT.np_dtype(),
            ),
            np.array(
                [
                    0,
                    -9223372036854775808,
                    9223372036854775807,
                    1,
                    -1,
                    10,
                    -10,
                    -0,
                    10,
                    10,
                ],
                TSDataType.TIMESTAMP.np_dtype(),
            ),
            np.array(
                [
                    date(1970, 1, 1),
                    date(1000, 1, 1),
                    date(9999, 12, 31),
                    date(1970, 1, 1),
                    date(1970, 1, 1),
                    date(1970, 1, 1),
                    date(1970, 1, 1),
                    date(1970, 1, 1),
                    date(1970, 1, 1),
                    date(1970, 1, 1),
                ],
                TSDataType.DATE.np_dtype(),
            ),
            np.array(
                [
                    "1234567890".encode("utf-8"),
                    "abcdefghijklmnopqrstuvwsyz".encode("utf-8"),
                    "!@#$%^&*()_+}{|:`~-=[];,./<>?~".encode("utf-8"),
                    "没问题".encode("utf-8"),
                    "！@#￥%……&*（）——|：“《》？·【】、；‘，。/".encode("utf-8"),
                    "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题".encode(
                        "utf-8"
                    ),
                    "".encode("utf-8"),
                    "   ".encode("utf-8"),
                    "1234567890".encode("utf-8"),
                    "1234567890".encode("utf-8"),
                ],
                TSDataType.BLOB.np_dtype(),
            ),
            np.array(
                [
                    "1234567890",
                    "abcdefghijklmnopqrstuvwsyz",
                    "!@#$%^&*()_+}{|:'`~-=[];,./<>?~",
                    "没问题",
                    "！@#￥%……&*（）——|：“《》？·【】、；‘，。/",
                    "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题",
                    "",
                    "    ",
                    "test01",
                    "test01",
                ],
                TSDataType.STRING.np_dtype(),
            ),
        ]
        np_bitmaps_ = []
        for i in range(len(column_types)):
            np_bitmaps_.append(BitMap(len(np_timestamps)))
        np_bitmaps_[5].mark(9)
        np_bitmaps_[6].mark(8)
        np_bitmaps_[7].mark(9)
        np_bitmaps_[8].mark(8)
        np_bitmaps_[9].mark(9)
        np_bitmaps_[10].mark(8)
        np_bitmaps_[11].mark(9)
        np_bitmaps_[12].mark(8)
        np_bitmaps_[13].mark(9)
        np_bitmaps_[14].mark(8)
        np_tablet = NumpyTablet(
            table_name,
            column_names,
            data_types,
            np_values,
            np_timestamps,
            bitmaps=np_bitmaps_,
            column_types=column_types,
        )
        session.insert(np_tablet)
        # Calculate the number of rows in the actual time series
        actual = 0
        with session.execute_query_statement(
            "select * from table_d"
        ) as session_data_set:
            print(session_data_set.get_column_names())
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        # Determine whether it meets expectations
        assert expect == actual

        session.close()


# Test automatic creation
def test_insert_relational_tablet_auto_create():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        config = TableSessionConfig(
            node_urls=[f"{db.get_container_host_ip()}:{db.get_exposed_port(6667)}"]
        )
        session = TableSession(config)

        # Preparation before testing
        session.execute_non_query_statement(
            "create database test_insert_relational_tablet_use_numpy_tablet"
        )
        session.execute_non_query_statement(
            "use test_insert_relational_tablet_use_numpy_tablet"
        )

        # 1、Test inserting tablet data(Insert 10 times)
        for i in range(1, 10):
            table_name = "t" + str(i)
            column_names = [
                "id1",
                "id2",
                "id3",
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
                ColumnType.ID,
                ColumnType.ID,
                ColumnType.ID,
                ColumnType.ATTRIBUTE,
                ColumnType.ATTRIBUTE,
                ColumnType.ATTRIBUTE,
                ColumnType.MEASUREMENT,
                ColumnType.MEASUREMENT,
                ColumnType.MEASUREMENT,
                ColumnType.MEASUREMENT,
                ColumnType.MEASUREMENT,
                ColumnType.MEASUREMENT,
                ColumnType.MEASUREMENT,
                ColumnType.MEASUREMENT,
                ColumnType.MEASUREMENT,
                ColumnType.MEASUREMENT,
            ]
            timestamps = []
            values = []
            for row in range(10):
                timestamps.append(row)
            values.append(
                [
                    "id1：" + str(row),
                    "id2：" + str(row),
                    "id3：" + str(row),
                    "attr1:" + str(row),
                    "attr2:" + str(row),
                    "attr3:" + str(row),
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
                    "id1：" + str(row),
                    "id2：" + str(row),
                    "id3：" + str(row),
                    "attr1:" + str(row),
                    "attr2:" + str(row),
                    "attr3:" + str(row),
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
                    "id1：" + str(row),
                    "id2：" + str(row),
                    "id3：" + str(row),
                    "attr1:" + str(row),
                    "attr2:" + str(row),
                    "attr3:" + str(row),
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
                    "id1：" + str(row),
                    "id2：" + str(row),
                    "id3：" + str(row),
                    "attr1:" + str(row),
                    "attr2:" + str(row),
                    "attr3:" + str(row),
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
                    "id1：" + str(row),
                    "id2：" + str(row),
                    "id3：" + str(row),
                    "attr1:" + str(row),
                    "attr2:" + str(row),
                    "attr3:" + str(row),
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
                    "id1：" + str(row),
                    "id2：" + str(row),
                    "id3：" + str(row),
                    "attr1:" + str(row),
                    "attr2:" + str(row),
                    "attr3:" + str(row),
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
                    "id1：" + str(row),
                    "id2：" + str(row),
                    "id3：" + str(row),
                    "attr1:" + str(row),
                    "attr2:" + str(row),
                    "attr3:" + str(row),
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
                    "id1：" + str(row),
                    "id2：" + str(row),
                    "id3：" + str(row),
                    "attr1:" + str(row),
                    "attr2:" + str(row),
                    "attr3:" + str(row),
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
                    "id1：" + str(row),
                    "id2：" + str(row),
                    "id3：" + str(row),
                    "attr1:" + str(row),
                    "attr2:" + str(row),
                    "attr3:" + str(row),
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
                    "id1：" + str(row),
                    "id2：" + str(row),
                    "id3：" + str(row),
                    "attr1:" + str(row),
                    "attr2:" + str(row),
                    "attr3:" + str(row),
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

        # 2、Test inserting NumpyTablet data(Insert 10 times)
        for i in range(1, 10):
            table_name = "t" + str(i)
            column_names = [
                "id1",
                "id2",
                "id3",
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
                ColumnType.ID,
                ColumnType.ID,
                ColumnType.ID,
                ColumnType.ATTRIBUTE,
                ColumnType.ATTRIBUTE,
                ColumnType.ATTRIBUTE,
                ColumnType.MEASUREMENT,
                ColumnType.MEASUREMENT,
                ColumnType.MEASUREMENT,
                ColumnType.MEASUREMENT,
                ColumnType.MEASUREMENT,
                ColumnType.MEASUREMENT,
                ColumnType.MEASUREMENT,
                ColumnType.MEASUREMENT,
                ColumnType.MEASUREMENT,
                ColumnType.MEASUREMENT,
            ]
            np_timestamps = np.arange(0, 10, dtype=np.dtype(">i8"))
            np_values = [
                np.array(["id1:{}".format(i) for i in range(0, 10)]),
                np.array(["id2:{}".format(i) for i in range(0, 10)]),
                np.array(["id3:{}".format(i) for i in range(0, 10)]),
                np.array(["attr1:{}".format(i) for i in range(0, 10)]),
                np.array(["attr2:{}".format(i) for i in range(0, 10)]),
                np.array(["attr3:{}".format(i) for i in range(0, 10)]),
                np.array(
                    [False, True, False, True, False, True, False, True, False, True],
                    TSDataType.BOOLEAN.np_dtype(),
                ),
                np.array(
                    [0, -2147483648, 2147483647, 1, -1, 10, -10, -0, 10, 10],
                    TSDataType.INT32.np_dtype(),
                ),
                np.array(
                    [
                        0,
                        -9223372036854775808,
                        9223372036854775807,
                        1,
                        -1,
                        10,
                        -10,
                        -0,
                        10,
                        10,
                    ],
                    TSDataType.INT64.np_dtype(),
                ),
                np.array(
                    [
                        0.0,
                        -0.12345678,
                        0.123456789,
                        1.0,
                        1.1234567,
                        4.123456,
                        12.12345,
                        -0.0,
                        1.1,
                        1.1,
                    ],
                    TSDataType.FLOAT.np_dtype(),
                ),
                np.array(
                    [
                        0.0,
                        -0.12345678901234567,
                        0.12345678901234567,
                        1.0,
                        1.1234567890123456,
                        4.123456789012345,
                        12.12345678901234,
                        -0.0,
                        1.1,
                        1.1,
                    ],
                    TSDataType.DOUBLE.np_dtype(),
                ),
                np.array(
                    [
                        "1234567890",
                        "abcdefghijklmnopqrstuvwsyz",
                        "!@#$%^&*()_+}{|:'`~-=[];,./<>?~",
                        "没问题",
                        "！@#￥%……&*（）——|：“《》？·【】、；‘，。/",
                        "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题",
                        "",
                        "    ",
                        "test01",
                        "test01",
                    ],
                    TSDataType.TEXT.np_dtype(),
                ),
                np.array(
                    [
                        0,
                        -9223372036854775808,
                        9223372036854775807,
                        1,
                        -1,
                        10,
                        -10,
                        -0,
                        10,
                        10,
                    ],
                    TSDataType.TIMESTAMP.np_dtype(),
                ),
                np.array(
                    [
                        date(1970, 1, 1),
                        date(1000, 1, 1),
                        date(9999, 12, 31),
                        date(1970, 1, 1),
                        date(1970, 1, 1),
                        date(1970, 1, 1),
                        date(1970, 1, 1),
                        date(1970, 1, 1),
                        date(1970, 1, 1),
                        date(1970, 1, 1),
                    ],
                    TSDataType.DATE.np_dtype(),
                ),
                np.array(
                    [
                        "1234567890".encode("utf-8"),
                        "abcdefghijklmnopqrstuvwsyz".encode("utf-8"),
                        "!@#$%^&*()_+}{|:`~-=[];,./<>?~".encode("utf-8"),
                        "没问题".encode("utf-8"),
                        "！@#￥%……&*（）——|：“《》？·【】、；‘，。/".encode("utf-8"),
                        "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题".encode(
                            "utf-8"
                        ),
                        "".encode("utf-8"),
                        "   ".encode("utf-8"),
                        "1234567890".encode("utf-8"),
                        "1234567890".encode("utf-8"),
                    ],
                    TSDataType.BLOB.np_dtype(),
                ),
                np.array(
                    [
                        "1234567890",
                        "abcdefghijklmnopqrstuvwsyz",
                        "!@#$%^&*()_+}{|:'`~-=[];,./<>?~",
                        "没问题",
                        "！@#￥%……&*（）——|：“《》？·【】、；‘，。/",
                        "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题",
                        "",
                        "    ",
                        "test01",
                        "test01",
                    ],
                    TSDataType.STRING.np_dtype(),
                ),
            ]
            np_tablet = NumpyTablet(
                table_name,
                column_names,
                data_types,
                np_values,
                np_timestamps,
                column_types=column_types,
            )
            session.insert(np_tablet)

        session.close()
