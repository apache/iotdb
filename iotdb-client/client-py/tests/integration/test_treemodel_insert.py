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
from .iotdb_container import IoTDBContainer
from iotdb.Session import Session
from iotdb.utils.Tablet import Tablet
from datetime import date
from iotdb.utils.BitMap import BitMap
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.NumpyTablet import NumpyTablet


# Test writing a tablet data to a non aligned time series
def test_insert_tablet():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open()

        # Preparation before testing
        session.set_storage_group("root.test1.g1")
        ts_path_lst = [
            "root.test1.g1.d1.BOOLEAN",
            "root.test1.g1.d1.INT32",
            "root.test1.g1.d1.INT64",
            "root.test1.g1.d1.FLOAT",
            "root.test1.g1.d1.DOUBLE",
            "root.test1.g1.d1.TEXT",
            "root.test1.g1.d1.TS",
            "root.test1.g1.d1.DATE",
            "root.test1.g1.d1.BLOB",
            "root.test1.g1.d1.STRING",
            "root.test1.g1.d2.BOOLEAN",
            "root.test1.g1.d2.INT32",
            "root.test1.g1.d2.INT64",
            "root.test1.g1.d2.FLOAT",
            "root.test1.g1.d2.DOUBLE",
            "root.test1.g1.d2.TEXT",
            "root.test1.g1.d2.TS",
            "root.test1.g1.d2.DATE",
            "root.test1.g1.d2.BLOB",
            "root.test1.g1.d2.STRING",
            "root.test1.g1.d3.BOOLEAN",
            "root.test1.g1.d3.INT32",
            "root.test1.g1.d3.INT64",
            "root.test1.g1.d3.FLOAT",
            "root.test1.g1.d3.DOUBLE",
            "root.test1.g1.d3.TEXT",
            "root.test1.g1.d3.TS",
            "root.test1.g1.d3.DATE",
            "root.test1.g1.d3.BLOB",
            "root.test1.g1.d3.STRING",
        ]
        data_type_lst = [
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
        encoding_lst = [
            TSEncoding.RLE,
            TSEncoding.CHIMP,
            TSEncoding.ZIGZAG,
            TSEncoding.RLBE,
            TSEncoding.SPRINTZ,
            TSEncoding.DICTIONARY,
            TSEncoding.TS_2DIFF,
            TSEncoding.CHIMP,
            TSEncoding.PLAIN,
            TSEncoding.DICTIONARY,
            TSEncoding.RLE,
            TSEncoding.CHIMP,
            TSEncoding.ZIGZAG,
            TSEncoding.RLBE,
            TSEncoding.SPRINTZ,
            TSEncoding.DICTIONARY,
            TSEncoding.TS_2DIFF,
            TSEncoding.CHIMP,
            TSEncoding.PLAIN,
            TSEncoding.DICTIONARY,
            TSEncoding.RLE,
            TSEncoding.CHIMP,
            TSEncoding.ZIGZAG,
            TSEncoding.RLBE,
            TSEncoding.SPRINTZ,
            TSEncoding.DICTIONARY,
            TSEncoding.TS_2DIFF,
            TSEncoding.CHIMP,
            TSEncoding.PLAIN,
            TSEncoding.DICTIONARY,
        ]
        compressor_lst = [
            Compressor.UNCOMPRESSED,
            Compressor.SNAPPY,
            Compressor.LZ4,
            Compressor.GZIP,
            Compressor.ZSTD,
            Compressor.LZMA2,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.UNCOMPRESSED,
            Compressor.SNAPPY,
            Compressor.LZ4,
            Compressor.GZIP,
            Compressor.ZSTD,
            Compressor.LZMA2,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.UNCOMPRESSED,
            Compressor.SNAPPY,
            Compressor.LZ4,
            Compressor.GZIP,
            Compressor.ZSTD,
            Compressor.LZMA2,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
        ]
        session.create_multi_time_series(
            ts_path_lst,
            data_type_lst,
            encoding_lst,
            compressor_lst,
            props_lst=None,
            tags_lst=None,
            attributes_lst=None,
            alias_lst=None,
        )

        # 1、Inserting tablet data
        expect = 10
        device_id = "root.test1.g1.d1"
        measurements_ = [
            "BOOLEAN",
            "INT32",
            "INT64",
            "FLOAT",
            "DOUBLE",
            "TEXT",
            "TS",
            "DATE",
            "BLOB",
            "STRING",
        ]
        data_types_ = [
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
        values_ = [
            [
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
            ],
            [
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
            ],
            [
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
            ],
            [
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
            ],
            [
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
            ],
            [
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
            ],
            [
                True,
                -10,
                -11,
                12.12345,
                12.12345678901234,
                "test101",
                11,
                date(1970, 1, 1),
                "Hello, World!".encode("utf-8"),
                "string01",
            ],
            [
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
            ],
            [
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
            ],
            [
                True,
                10,
                11,
                1.1,
                10011.1,
                "test101",
                11,
                date(1970, 1, 1),
                "Hello, World!".encode("utf-8"),
                "string01",
            ],
        ]
        timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        tablet_ = Tablet(device_id, measurements_, data_types_, values_, timestamps_)
        session.insert_tablet(tablet_)
        # Calculate the number of rows in the actual time series
        actual = 0
        with session.execute_query_statement(
            "select * from root.test1.g1.d1"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        # Determine whether it meets expectations
        assert expect == actual

        # 2、Inserting Numpy Tablet data (No null)
        expect = 10
        device_id = "root.test1.g1.d2"
        measurements_ = [
            "BOOLEAN",
            "INT32",
            "INT64",
            "FLOAT",
            "DOUBLE",
            "TEXT",
            "TS",
            "DATE",
            "BLOB",
            "STRING",
        ]
        data_types_ = [
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
        np_values_ = [
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
                    "test101",
                    "test101",
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
                    "test101",
                    "test101",
                ],
                TSDataType.STRING.np_dtype(),
            ),
        ]
        np_timestamps_ = np.array(
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype()
        )
        np_tablet_ = NumpyTablet(
            device_id, measurements_, data_types_, np_values_, np_timestamps_
        )
        session.insert_tablet(np_tablet_)
        # Calculate the number of rows in the actual time series
        actual = 0
        with session.execute_query_statement(
            "select * from root.test1.g1.d2"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        # Determine whether it meets expectations
        assert expect == actual

        # 3、Inserting Numpy Tablet data (Contains null)
        expect = 10
        device_id = "root.test1.g1.d3"
        measurements_ = [
            "BOOLEAN",
            "INT32",
            "INT64",
            "FLOAT",
            "DOUBLE",
            "TEXT",
            "TS",
            "DATE",
            "BLOB",
            "STRING",
        ]
        data_types_ = [
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
        np_values_ = [
            np.array(
                [False, True, False, True, False, True, False, True, False, False],
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
        np_timestamps_ = np.array(
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype()
        )
        np_bitmaps_ = []
        for i in range(len(measurements_)):
            np_bitmaps_.append(BitMap(len(np_timestamps_)))
        np_bitmaps_[0].mark(9)
        np_bitmaps_[1].mark(8)
        np_bitmaps_[2].mark(9)
        np_bitmaps_[4].mark(8)
        np_bitmaps_[5].mark(9)
        np_bitmaps_[6].mark(8)
        np_bitmaps_[7].mark(9)
        np_bitmaps_[8].mark(8)
        np_bitmaps_[9].mark(9)
        np_tablet_ = NumpyTablet(
            device_id,
            measurements_,
            data_types_,
            np_values_,
            np_timestamps_,
            np_bitmaps_,
        )
        session.insert_tablet(np_tablet_)
        # Calculate the number of rows in the actual time series
        actual = 0
        with session.execute_query_statement(
            "select * from root.test1.g1.d3"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        # Determine whether it meets expectations
        assert expect == actual
        session.close()


# Test writing a tablet data to align the time series
def test_insert_aligned_tablet():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open()

        # Preparation before testing
        session.set_storage_group("root.test2.g3")
        device_id = "root.test2.g3.d1"
        measurements_lst = [
            "BOOLEAN",
            "INT32",
            "INT64",
            "FLOAT",
            "DOUBLE",
            "TEXT",
            "TS",
            "DATE",
            "BLOB",
            "STRING",
        ]
        data_type_lst = [
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
        encoding_lst = [
            TSEncoding.RLE,
            TSEncoding.CHIMP,
            TSEncoding.ZIGZAG,
            TSEncoding.RLBE,
            TSEncoding.SPRINTZ,
            TSEncoding.DICTIONARY,
            TSEncoding.TS_2DIFF,
            TSEncoding.CHIMP,
            TSEncoding.PLAIN,
            TSEncoding.DICTIONARY,
        ]
        compressor_lst = [
            Compressor.UNCOMPRESSED,
            Compressor.SNAPPY,
            Compressor.LZ4,
            Compressor.GZIP,
            Compressor.ZSTD,
            Compressor.LZMA2,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
        ]
        session.create_aligned_time_series(
            device_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst
        )
        device_id = "root.test2.g3.d2"
        measurements_lst = [
            "BOOLEAN",
            "INT32",
            "INT64",
            "FLOAT",
            "DOUBLE",
            "TEXT",
            "TS",
            "DATE",
            "BLOB",
            "STRING",
        ]
        data_type_lst = [
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
        encoding_lst = [
            TSEncoding.RLE,
            TSEncoding.CHIMP,
            TSEncoding.ZIGZAG,
            TSEncoding.RLBE,
            TSEncoding.SPRINTZ,
            TSEncoding.DICTIONARY,
            TSEncoding.TS_2DIFF,
            TSEncoding.CHIMP,
            TSEncoding.PLAIN,
            TSEncoding.DICTIONARY,
        ]
        compressor_lst = [
            Compressor.UNCOMPRESSED,
            Compressor.SNAPPY,
            Compressor.LZ4,
            Compressor.GZIP,
            Compressor.ZSTD,
            Compressor.LZMA2,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
        ]
        session.create_aligned_time_series(
            device_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst
        )
        device_id = "root.test2.g3.d3"
        measurements_lst = [
            "BOOLEAN",
            "INT32",
            "INT64",
            "FLOAT",
            "DOUBLE",
            "TEXT",
            "TS",
            "DATE",
            "BLOB",
            "STRING",
        ]
        data_type_lst = [
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
        encoding_lst = [
            TSEncoding.RLE,
            TSEncoding.CHIMP,
            TSEncoding.ZIGZAG,
            TSEncoding.RLBE,
            TSEncoding.SPRINTZ,
            TSEncoding.DICTIONARY,
            TSEncoding.TS_2DIFF,
            TSEncoding.CHIMP,
            TSEncoding.PLAIN,
            TSEncoding.DICTIONARY,
        ]
        compressor_lst = [
            Compressor.UNCOMPRESSED,
            Compressor.SNAPPY,
            Compressor.LZ4,
            Compressor.GZIP,
            Compressor.ZSTD,
            Compressor.LZMA2,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
        ]
        session.create_aligned_time_series(
            device_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst
        )

        # 1、Inserting tablet data
        expect = 10
        device_id = "root.test2.g3.d1"
        measurements_ = [
            "BOOLEAN",
            "INT32",
            "INT64",
            "FLOAT",
            "DOUBLE",
            "TEXT",
            "TS",
            "DATE",
            "BLOB",
            "STRING",
        ]
        data_types_ = [
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
        values_ = [
            [
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
            ],
            [
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
            ],
            [
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
            ],
            [
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
            ],
            [
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
            ],
            [
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
            ],
            [
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
            ],
            [
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
            ],
            [
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
            ],
            [
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
            ],
        ]
        timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        tablet_ = Tablet(device_id, measurements_, data_types_, values_, timestamps_)
        session.insert_aligned_tablet(tablet_)
        # Calculate the number of rows in the actual time series
        actual = 0
        with session.execute_query_statement(
            "select * from root.test2.g3.d1"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        # Determine whether it meets expectations
        assert expect == actual

        # 2、Inserting Numpy Tablet data (No null)
        expect = 10
        device_id = "root.test2.g3.d2"
        measurements_ = [
            "BOOLEAN",
            "INT32",
            "INT64",
            "FLOAT",
            "DOUBLE",
            "TEXT",
            "TS",
            "DATE",
            "BLOB",
            "STRING",
        ]
        data_types_ = [
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
        np_values_ = [
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
        np_timestamps_ = np.array(
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype()
        )
        np_tablet_ = NumpyTablet(
            device_id, measurements_, data_types_, np_values_, np_timestamps_
        )
        session.insert_aligned_tablet(np_tablet_)
        # Calculate the number of rows in the actual time series
        actual = 0
        with session.execute_query_statement(
            "select * from root.test2.g3.d2"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        # Determine whether it meets expectations
        assert expect == actual

        # 3、Inserting Numpy Tablet data (Contains null)
        expect = 10
        device_id = "root.test2.g3.d3"
        measurements_ = [
            "BOOLEAN",
            "INT32",
            "INT64",
            "FLOAT",
            "DOUBLE",
            "TEXT",
            "TS",
            "DATE",
            "BLOB",
            "STRING",
        ]
        data_types_ = [
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
        np_values_ = [
            np.array(
                [False, True, False, True, False, True, False, True, False, False],
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
        np_timestamps_ = np.array(
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype()
        )
        np_bitmaps_ = []
        for i in range(len(measurements_)):
            np_bitmaps_.append(BitMap(len(np_timestamps_)))
        np_bitmaps_[0].mark(9)
        np_bitmaps_[1].mark(8)
        np_bitmaps_[2].mark(9)
        np_bitmaps_[4].mark(8)
        np_bitmaps_[5].mark(9)
        np_bitmaps_[6].mark(8)
        np_bitmaps_[7].mark(9)
        np_bitmaps_[8].mark(8)
        np_bitmaps_[9].mark(9)
        np_tablet_ = NumpyTablet(
            device_id,
            measurements_,
            data_types_,
            np_values_,
            np_timestamps_,
            np_bitmaps_,
        )
        session.insert_aligned_tablet(np_tablet_)
        # Calculate the number of rows in the actual time series
        actual = 0
        with session.execute_query_statement(
            "select * from root.test2.g3.d3"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        # Determine whether it meets expectations
        assert expect == actual
        session.close()


# Test writing multiple tablet data to non aligned time series
def test_insert_tablets():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open()

        # Preparation before testing
        session.set_storage_group("root.test3.g1")
        session.set_storage_group("root.test3.g2")
        ts_path_lst = [
            "root.test3.g1.d1.BOOLEAN",
            "root.test3.g1.d1.INT32",
            "root.test3.g1.d1.INT64",
            "root.test3.g1.d1.FLOAT",
            "root.test3.g1.d1.DOUBLE",
            "root.test3.g1.d1.TEXT",
            "root.test3.g1.d1.TS",
            "root.test3.g1.d1.DATE",
            "root.test3.g1.d1.BLOB",
            "root.test3.g1.d1.STRING",
            "root.test3.g1.d2.BOOLEAN",
            "root.test3.g1.d2.INT32",
            "root.test3.g1.d2.INT64",
            "root.test3.g1.d2.FLOAT",
            "root.test3.g1.d2.DOUBLE",
            "root.test3.g1.d2.TEXT",
            "root.test3.g1.d2.TS",
            "root.test3.g1.d2.DATE",
            "root.test3.g1.d2.BLOB",
            "root.test3.g1.d2.STRING",
            "root.test3.g1.d3.BOOLEAN",
            "root.test3.g1.d3.INT32",
            "root.test3.g1.d3.INT64",
            "root.test3.g1.d3.FLOAT",
            "root.test3.g1.d3.DOUBLE",
            "root.test3.g1.d3.TEXT",
            "root.test3.g1.d3.TS",
            "root.test3.g1.d3.DATE",
            "root.test3.g1.d3.BLOB",
            "root.test3.g1.d3.STRING",
            "root.test3.g2.d1.BOOLEAN",
            "root.test3.g2.d1.INT32",
            "root.test3.g2.d1.INT64",
            "root.test3.g2.d1.FLOAT",
            "root.test3.g2.d1.DOUBLE",
            "root.test3.g2.d1.TEXT",
            "root.test3.g2.d1.TS",
            "root.test3.g2.d1.DATE",
            "root.test3.g2.d1.BLOB",
            "root.test3.g2.d1.STRING",
            "root.test3.g2.d2.BOOLEAN",
            "root.test3.g2.d2.INT32",
            "root.test3.g2.d2.INT64",
            "root.test3.g2.d2.FLOAT",
            "root.test3.g2.d2.DOUBLE",
            "root.test3.g2.d2.TEXT",
            "root.test3.g2.d2.TS",
            "root.test3.g2.d2.DATE",
            "root.test3.g2.d2.BLOB",
            "root.test3.g2.d2.STRING",
            "root.test3.g2.d3.BOOLEAN",
            "root.test3.g2.d3.INT32",
            "root.test3.g2.d3.INT64",
            "root.test3.g2.d3.FLOAT",
            "root.test3.g2.d3.DOUBLE",
            "root.test3.g2.d3.TEXT",
            "root.test3.g2.d3.TS",
            "root.test3.g2.d3.DATE",
            "root.test3.g2.d3.BLOB",
            "root.test3.g2.d3.STRING",
        ]
        data_type_lst = [
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
        encoding_lst = [
            TSEncoding.RLE,
            TSEncoding.CHIMP,
            TSEncoding.ZIGZAG,
            TSEncoding.RLBE,
            TSEncoding.SPRINTZ,
            TSEncoding.DICTIONARY,
            TSEncoding.TS_2DIFF,
            TSEncoding.CHIMP,
            TSEncoding.PLAIN,
            TSEncoding.DICTIONARY,
            TSEncoding.RLE,
            TSEncoding.CHIMP,
            TSEncoding.ZIGZAG,
            TSEncoding.RLBE,
            TSEncoding.SPRINTZ,
            TSEncoding.DICTIONARY,
            TSEncoding.TS_2DIFF,
            TSEncoding.CHIMP,
            TSEncoding.PLAIN,
            TSEncoding.DICTIONARY,
            TSEncoding.RLE,
            TSEncoding.CHIMP,
            TSEncoding.ZIGZAG,
            TSEncoding.RLBE,
            TSEncoding.SPRINTZ,
            TSEncoding.DICTIONARY,
            TSEncoding.TS_2DIFF,
            TSEncoding.CHIMP,
            TSEncoding.PLAIN,
            TSEncoding.DICTIONARY,
            TSEncoding.RLE,
            TSEncoding.CHIMP,
            TSEncoding.ZIGZAG,
            TSEncoding.RLBE,
            TSEncoding.SPRINTZ,
            TSEncoding.DICTIONARY,
            TSEncoding.TS_2DIFF,
            TSEncoding.CHIMP,
            TSEncoding.PLAIN,
            TSEncoding.DICTIONARY,
            TSEncoding.RLE,
            TSEncoding.CHIMP,
            TSEncoding.ZIGZAG,
            TSEncoding.RLBE,
            TSEncoding.SPRINTZ,
            TSEncoding.DICTIONARY,
            TSEncoding.TS_2DIFF,
            TSEncoding.CHIMP,
            TSEncoding.PLAIN,
            TSEncoding.DICTIONARY,
            TSEncoding.RLE,
            TSEncoding.CHIMP,
            TSEncoding.ZIGZAG,
            TSEncoding.RLBE,
            TSEncoding.SPRINTZ,
            TSEncoding.DICTIONARY,
            TSEncoding.TS_2DIFF,
            TSEncoding.CHIMP,
            TSEncoding.PLAIN,
            TSEncoding.DICTIONARY,
        ]
        compressor_lst = [
            Compressor.UNCOMPRESSED,
            Compressor.SNAPPY,
            Compressor.LZ4,
            Compressor.GZIP,
            Compressor.ZSTD,
            Compressor.LZMA2,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.UNCOMPRESSED,
            Compressor.SNAPPY,
            Compressor.LZ4,
            Compressor.GZIP,
            Compressor.ZSTD,
            Compressor.LZMA2,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.UNCOMPRESSED,
            Compressor.SNAPPY,
            Compressor.LZ4,
            Compressor.GZIP,
            Compressor.ZSTD,
            Compressor.LZMA2,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.UNCOMPRESSED,
            Compressor.SNAPPY,
            Compressor.LZ4,
            Compressor.GZIP,
            Compressor.ZSTD,
            Compressor.LZMA2,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.UNCOMPRESSED,
            Compressor.SNAPPY,
            Compressor.LZ4,
            Compressor.GZIP,
            Compressor.ZSTD,
            Compressor.LZMA2,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.UNCOMPRESSED,
            Compressor.SNAPPY,
            Compressor.LZ4,
            Compressor.GZIP,
            Compressor.ZSTD,
            Compressor.LZMA2,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
        ]
        session.create_multi_time_series(
            ts_path_lst,
            data_type_lst,
            encoding_lst,
            compressor_lst,
            props_lst=None,
            tags_lst=None,
            attributes_lst=None,
            alias_lst=None,
        )

        # 1、Inserting tablet data
        expect = 30
        measurements_ = [
            "BOOLEAN",
            "INT32",
            "INT64",
            "FLOAT",
            "DOUBLE",
            "TEXT",
            "TS",
            "DATE",
            "BLOB",
            "STRING",
        ]
        data_types = [
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
        values_ = [
            [
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
            ],
            [
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
            ],
            [
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
            ],
            [
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
            ],
            [
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
            ],
            [
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
            ],
            [
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
            ],
            [
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
            ],
            [
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
            ],
            [
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
            ],
        ]
        timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        device1_id = "root.test3.g1.d1"
        device2_id = "root.test3.g1.d2"
        device3_id = "root.test3.g1.d3"
        tablet1_ = Tablet(device1_id, measurements_, data_types, values_, timestamps_)
        tablet2_ = Tablet(device2_id, measurements_, data_types, values_, timestamps_)
        tablet3_ = Tablet(device3_id, measurements_, data_types, values_, timestamps_)
        tablet_lst = [tablet1_, tablet2_, tablet3_]
        session.insert_tablets(tablet_lst)
        # Calculate the number of rows in the actual time series
        actual = 0
        with session.execute_query_statement(
            "select * from root.test3.g1.d1"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        with session.execute_query_statement(
            "select * from root.test3.g1.d2"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        with session.execute_query_statement(
            "select * from root.test3.g1.d3"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        # Determine whether it meets expectations
        assert expect == actual

        # 2、Inserting Numpy Tablet data
        expect = 30
        measurements_ = [
            "BOOLEAN",
            "INT32",
            "INT64",
            "FLOAT",
            "DOUBLE",
            "TEXT",
            "TS",
            "DATE",
            "BLOB",
            "STRING",
        ]
        data_types_ = [
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
        np_values_ = [
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
        np_timestamps_ = np.array(
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype()
        )
        device1_id = "root.test3.g2.d1"
        device2_id = "root.test3.g2.d2"
        device3_id = "root.test3.g2.d3"
        np_tablet1_ = NumpyTablet(
            device1_id, measurements_, data_types_, np_values_, np_timestamps_
        )
        np_tablet2_ = NumpyTablet(
            device2_id, measurements_, data_types_, np_values_, np_timestamps_
        )
        np_tablet3_ = NumpyTablet(
            device3_id, measurements_, data_types_, np_values_, np_timestamps_
        )
        np_tablet_list = [np_tablet1_, np_tablet2_, np_tablet3_]

        session.insert_tablets(np_tablet_list)
        # Calculate the number of rows in the actual time series
        actual = 0
        with session.execute_query_statement(
            "select * from root.test3.g2.d1"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        with session.execute_query_statement(
            "select * from root.test3.g2.d2"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        with session.execute_query_statement(
            "select * from root.test3.g2.d3"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1

        # Determine whether it meets expectations
        assert expect == actual
        session.close()


# Test writing multiple tablet data to aligned time series
def test_insert_aligned_tablets():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open()

        # Preparation before testing
        session.set_storage_group("root.test4.g1")
        session.set_storage_group("root.test4.g2")
        measurements_lst = [
            "BOOLEAN",
            "INT32",
            "INT64",
            "FLOAT",
            "DOUBLE",
            "TEXT",
            "TS",
            "DATE",
            "BLOB",
            "STRING",
        ]
        data_type_lst = [
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
        encoding_lst = [
            TSEncoding.RLE,
            TSEncoding.CHIMP,
            TSEncoding.ZIGZAG,
            TSEncoding.RLBE,
            TSEncoding.SPRINTZ,
            TSEncoding.DICTIONARY,
            TSEncoding.TS_2DIFF,
            TSEncoding.CHIMP,
            TSEncoding.PLAIN,
            TSEncoding.DICTIONARY,
        ]
        compressor_lst = [
            Compressor.UNCOMPRESSED,
            Compressor.SNAPPY,
            Compressor.LZ4,
            Compressor.GZIP,
            Compressor.ZSTD,
            Compressor.LZMA2,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
        ]
        device11_id = "root.test4.g1.d1"
        session.create_aligned_time_series(
            device11_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst
        )
        device12_id = "root.test4.g1.d2"
        session.create_aligned_time_series(
            device12_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst
        )
        device13_id = "root.test4.g1.d3"
        session.create_aligned_time_series(
            device13_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst
        )
        device21_id = "root.test4.g2.d1"
        session.create_aligned_time_series(
            device21_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst
        )
        device22_id = "root.test4.g2.d2"
        session.create_aligned_time_series(
            device22_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst
        )
        device23_id = "root.test4.g2.d3"
        session.create_aligned_time_series(
            device23_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst
        )

        # 1、Inserting tablet data
        expect = 30

        measurements_ = [
            "BOOLEAN",
            "INT32",
            "INT64",
            "FLOAT",
            "DOUBLE",
            "TEXT",
            "TS",
            "DATE",
            "BLOB",
            "STRING",
        ]
        data_types = [
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
        values_ = [
            [
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
            ],
            [
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
            ],
            [
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
            ],
            [
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
            ],
            [
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
            ],
            [
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
            ],
            [
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
            ],
            [
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
            ],
            [
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
            ],
            [
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
            ],
        ]
        timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        device1_id = "root.test4.g1.d1"
        device2_id = "root.test4.g1.d2"
        device3_id = "root.test4.g1.d3"
        tablet1_ = Tablet(device1_id, measurements_, data_types, values_, timestamps_)
        tablet2_ = Tablet(device2_id, measurements_, data_types, values_, timestamps_)
        tablet3_ = Tablet(device3_id, measurements_, data_types, values_, timestamps_)
        tablet_lst = [tablet1_, tablet2_, tablet3_]
        session.insert_aligned_tablets(tablet_lst)

        # Calculate the number of rows in the actual time series
        actual = 0
        with session.execute_query_statement(
            "select * from root.test4.g1.d1"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        with session.execute_query_statement(
            "select * from root.test4.g1.d2"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        with session.execute_query_statement(
            "select * from root.test4.g1.d3"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        # Determine whether it meets expectations
        assert expect == actual

        # 2、Inserting Numpy Tablet data
        expect = 30

        measurements_ = [
            "BOOLEAN",
            "INT32",
            "INT64",
            "FLOAT",
            "DOUBLE",
            "TEXT",
            "TS",
            "DATE",
            "BLOB",
            "STRING",
        ]
        data_types_ = [
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
        np_values_ = [
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
        np_timestamps_ = np.array(
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype()
        )
        device1_id = "root.test4.g2.d1"
        device2_id = "root.test4.g2.d2"
        device_id = "root.test4.g2.d3"
        np_tablet1_ = NumpyTablet(
            device1_id, measurements_, data_types_, np_values_, np_timestamps_
        )
        np_tablet2_ = NumpyTablet(
            device2_id, measurements_, data_types_, np_values_, np_timestamps_
        )
        np_tablet3_ = NumpyTablet(
            device_id, measurements_, data_types_, np_values_, np_timestamps_
        )
        np_tablet_list = [np_tablet1_, np_tablet2_, np_tablet3_]
        session.insert_aligned_tablets(np_tablet_list)

        # Calculate the number of rows in the actual time series
        actual = 0
        with session.execute_query_statement(
            "select * from root.test4.g2.d1"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        with session.execute_query_statement(
            "select * from root.test4.g2.d2"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        with session.execute_query_statement(
            "select * from root.test4.g2.d3"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        # Determine whether it meets expectations
        assert expect == actual
        session.close()


# Test writing a Record data to a non aligned time series
def test_insert_record():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open()

        # Preparation before testing
        session.set_storage_group("root.test5.g1")
        ts_path_lst = [
            "root.test5.g1.d1.BOOLEAN",
            "root.test5.g1.d1.INT32",
            "root.test5.g1.d1.INT64",
            "root.test5.g1.d1.FLOAT",
            "root.test5.g1.d1.DOUBLE",
            "root.test5.g1.d1.TEXT",
            "root.test5.g1.d1.TS",
            "root.test5.g1.d1.DATE",
            "root.test5.g1.d1.BLOB",
            "root.test5.g1.d1.STRING",
        ]
        data_type_lst = [
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
        encoding_lst = [
            TSEncoding.RLE,
            TSEncoding.CHIMP,
            TSEncoding.ZIGZAG,
            TSEncoding.RLBE,
            TSEncoding.SPRINTZ,
            TSEncoding.DICTIONARY,
            TSEncoding.TS_2DIFF,
            TSEncoding.CHIMP,
            TSEncoding.PLAIN,
            TSEncoding.DICTIONARY,
        ]
        compressor_lst = [
            Compressor.UNCOMPRESSED,
            Compressor.SNAPPY,
            Compressor.LZ4,
            Compressor.GZIP,
            Compressor.ZSTD,
            Compressor.LZMA2,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
        ]
        session.create_multi_time_series(
            ts_path_lst,
            data_type_lst,
            encoding_lst,
            compressor_lst,
            props_lst=None,
            tags_lst=None,
            attributes_lst=None,
            alias_lst=None,
        )

        expect = 9
        session.insert_record(
            "root.test5.g1.d1",
            1,
            [
                "BOOLEAN",
                "INT32",
                "INT64",
                "FLOAT",
                "DOUBLE",
                "TEXT",
                "TS",
                "DATE",
                "BLOB",
                "STRING",
            ],
            [
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
            ],
            [
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
            ],
        )
        session.insert_record(
            "root.test5.g1.d1",
            2,
            [
                "BOOLEAN",
                "INT32",
                "INT64",
                "FLOAT",
                "DOUBLE",
                "TEXT",
                "TS",
                "DATE",
                "BLOB",
                "STRING",
            ],
            [
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
            ],
            [
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
            ],
        )
        session.insert_record(
            "root.test5.g1.d1",
            3,
            [
                "BOOLEAN",
                "INT32",
                "INT64",
                "FLOAT",
                "DOUBLE",
                "TEXT",
                "TS",
                "DATE",
                "BLOB",
                "STRING",
            ],
            [
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
            ],
            [
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
            ],
        )
        session.insert_record(
            "root.test5.g1.d1",
            4,
            [
                "BOOLEAN",
                "INT32",
                "INT64",
                "FLOAT",
                "DOUBLE",
                "TEXT",
                "TS",
                "DATE",
                "BLOB",
                "STRING",
            ],
            [
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
            ],
            [
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
            ],
        )
        session.insert_record(
            "root.test5.g1.d1",
            5,
            [
                "BOOLEAN",
                "INT32",
                "INT64",
                "FLOAT",
                "DOUBLE",
                "TEXT",
                "TS",
                "DATE",
                "BLOB",
                "STRING",
            ],
            [
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
            ],
            [
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
            ],
        )
        session.insert_record(
            "root.test5.g1.d1",
            6,
            [
                "BOOLEAN",
                "INT32",
                "INT64",
                "FLOAT",
                "DOUBLE",
                "TEXT",
                "TS",
                "DATE",
                "BLOB",
                "STRING",
            ],
            [
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
            ],
            [
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
            ],
        )
        session.insert_record(
            "root.test5.g1.d1",
            7,
            [
                "BOOLEAN",
                "INT32",
                "INT64",
                "FLOAT",
                "DOUBLE",
                "TEXT",
                "TS",
                "DATE",
                "BLOB",
                "STRING",
            ],
            [
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
            ],
            [
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
            ],
        )
        session.insert_record(
            "root.test5.g1.d1",
            9,
            [
                "BOOLEAN",
                "INT32",
                "INT64",
                "FLOAT",
                "DOUBLE",
                "TEXT",
                "TS",
                "DATE",
                "BLOB",
                "STRING",
            ],
            [
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
            ],
            [
                True,
                -0,
                -0,
                -0.0,
                -0.0,
                "test01",
                11,
                date(1970, 1, 1),
                "Hello, World!".encode("utf-8"),
                "string01",
            ],
        )
        session.insert_record(
            "root.test5.g1.d1",
            10,
            [
                "BOOLEAN",
                "INT32",
                "INT64",
                "FLOAT",
                "DOUBLE",
                "TEXT",
                "TS",
                "DATE",
                "BLOB",
                "STRING",
            ],
            [
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
            ],
            [
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
            ],
        )
        # Calculate the number of rows in the actual time series
        actual = 0
        with session.execute_query_statement(
            "select * from root.test5.g1.d1"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        # Determine whether it meets expectations
        assert expect == actual
        session.close()


# Test writing a Record data to align the time series
def test_insert_aligned_record():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open()

        # Preparation before testing
        session.set_storage_group("root.test6.g1")
        measurements_lst = [
            "BOOLEAN",
            "INT32",
            "INT64",
            "FLOAT",
            "DOUBLE",
            "TEXT",
            "TS",
            "DATE",
            "BLOB",
            "STRING",
        ]
        data_type_lst = [
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
        encoding_lst = [
            TSEncoding.RLE,
            TSEncoding.CHIMP,
            TSEncoding.ZIGZAG,
            TSEncoding.RLBE,
            TSEncoding.SPRINTZ,
            TSEncoding.DICTIONARY,
            TSEncoding.TS_2DIFF,
            TSEncoding.CHIMP,
            TSEncoding.PLAIN,
            TSEncoding.DICTIONARY,
        ]
        compressor_lst = [
            Compressor.UNCOMPRESSED,
            Compressor.SNAPPY,
            Compressor.LZ4,
            Compressor.GZIP,
            Compressor.ZSTD,
            Compressor.LZMA2,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
        ]
        device11_id = "root.test6.g1.d1"
        session.create_aligned_time_series(
            device11_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst
        )

        expect = 9
        session.insert_aligned_record(
            "root.test6.g1.d1",
            1,
            [
                "BOOLEAN",
                "INT32",
                "INT64",
                "FLOAT",
                "DOUBLE",
                "TEXT",
                "TS",
                "DATE",
                "BLOB",
                "STRING",
            ],
            [
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
            ],
            [
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
            ],
        )
        session.insert_aligned_record(
            "root.test6.g1.d1",
            2,
            [
                "BOOLEAN",
                "INT32",
                "INT64",
                "FLOAT",
                "DOUBLE",
                "TEXT",
                "TS",
                "DATE",
                "BLOB",
                "STRING",
            ],
            [
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
            ],
            [
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
            ],
        )
        session.insert_aligned_record(
            "root.test6.g1.d1",
            3,
            [
                "BOOLEAN",
                "INT32",
                "INT64",
                "FLOAT",
                "DOUBLE",
                "TEXT",
                "TS",
                "DATE",
                "BLOB",
                "STRING",
            ],
            [
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
            ],
            [
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
            ],
        )
        session.insert_aligned_record(
            "root.test6.g1.d1",
            4,
            [
                "BOOLEAN",
                "INT32",
                "INT64",
                "FLOAT",
                "DOUBLE",
                "TEXT",
                "TS",
                "DATE",
                "BLOB",
                "STRING",
            ],
            [
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
            ],
            [
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
            ],
        )
        session.insert_aligned_record(
            "root.test6.g1.d1",
            5,
            [
                "BOOLEAN",
                "INT32",
                "INT64",
                "FLOAT",
                "DOUBLE",
                "TEXT",
                "TS",
                "DATE",
                "BLOB",
                "STRING",
            ],
            [
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
            ],
            [
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
            ],
        )
        session.insert_aligned_record(
            "root.test6.g1.d1",
            6,
            [
                "BOOLEAN",
                "INT32",
                "INT64",
                "FLOAT",
                "DOUBLE",
                "TEXT",
                "TS",
                "DATE",
                "BLOB",
                "STRING",
            ],
            [
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
            ],
            [
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
            ],
        )
        session.insert_aligned_record(
            "root.test6.g1.d1",
            7,
            [
                "BOOLEAN",
                "INT32",
                "INT64",
                "FLOAT",
                "DOUBLE",
                "TEXT",
                "TS",
                "DATE",
                "BLOB",
                "STRING",
            ],
            [
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
            ],
            [
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
            ],
        )
        session.insert_aligned_record(
            "root.test6.g1.d1",
            9,
            [
                "BOOLEAN",
                "INT32",
                "INT64",
                "FLOAT",
                "DOUBLE",
                "TEXT",
                "TS",
                "DATE",
                "BLOB",
                "STRING",
            ],
            [
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
            ],
            [
                True,
                -0,
                -0,
                -0.0,
                -0.0,
                "test01",
                11,
                date(1970, 1, 1),
                "Hello, World!".encode("utf-8"),
                "string01",
            ],
        )
        session.insert_aligned_record(
            "root.test6.g1.d1",
            10,
            [
                "BOOLEAN",
                "INT32",
                "INT64",
                "FLOAT",
                "DOUBLE",
                "TEXT",
                "TS",
                "DATE",
                "BLOB",
                "STRING",
            ],
            [
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
            ],
            [
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
            ],
        )
        # Calculate the number of rows in the actual time series
        actual = 0
        with session.execute_query_statement(
            "select * from root.test6.g1.d1"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        # Determine whether it meets expectations
        assert expect == actual
        session.close()


# Test writing multiple Record data to non aligned time series
def test_insert_records():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open()

        # Preparation before testing
        session.set_storage_group("root.test7.g1")
        ts_path_lst = [
            "root.test7.g1.d1.BOOLEAN",
            "root.test7.g1.d1.INT32",
            "root.test7.g1.d1.INT64",
            "root.test7.g1.d1.FLOAT",
            "root.test7.g1.d1.DOUBLE",
            "root.test7.g1.d1.TEXT",
            "root.test7.g1.d1.TS",
            "root.test7.g1.d1.DATE",
            "root.test7.g1.d1.BLOB",
            "root.test7.g1.d1.STRING",
            "root.test7.g1.d2.BOOLEAN",
            "root.test7.g1.d2.INT32",
            "root.test7.g1.d2.INT64",
            "root.test7.g1.d2.FLOAT",
            "root.test7.g1.d2.DOUBLE",
            "root.test7.g1.d2.TEXT",
            "root.test7.g1.d2.TS",
            "root.test7.g1.d2.DATE",
            "root.test7.g1.d2.BLOB",
            "root.test7.g1.d2.STRING",
            "root.test7.g1.d3.BOOLEAN",
            "root.test7.g1.d3.INT32",
            "root.test7.g1.d3.INT64",
            "root.test7.g1.d3.FLOAT",
            "root.test7.g1.d3.DOUBLE",
            "root.test7.g1.d3.TEXT",
            "root.test7.g1.d3.TS",
            "root.test7.g1.d3.DATE",
            "root.test7.g1.d3.BLOB",
            "root.test7.g1.d3.STRING",
        ]
        data_type_lst = [
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
        encoding_lst = [
            TSEncoding.RLE,
            TSEncoding.CHIMP,
            TSEncoding.ZIGZAG,
            TSEncoding.RLBE,
            TSEncoding.SPRINTZ,
            TSEncoding.DICTIONARY,
            TSEncoding.TS_2DIFF,
            TSEncoding.CHIMP,
            TSEncoding.PLAIN,
            TSEncoding.DICTIONARY,
            TSEncoding.RLE,
            TSEncoding.CHIMP,
            TSEncoding.ZIGZAG,
            TSEncoding.RLBE,
            TSEncoding.SPRINTZ,
            TSEncoding.DICTIONARY,
            TSEncoding.TS_2DIFF,
            TSEncoding.CHIMP,
            TSEncoding.PLAIN,
            TSEncoding.DICTIONARY,
            TSEncoding.RLE,
            TSEncoding.CHIMP,
            TSEncoding.ZIGZAG,
            TSEncoding.RLBE,
            TSEncoding.SPRINTZ,
            TSEncoding.DICTIONARY,
            TSEncoding.TS_2DIFF,
            TSEncoding.CHIMP,
            TSEncoding.PLAIN,
            TSEncoding.DICTIONARY,
        ]
        compressor_lst = [
            Compressor.UNCOMPRESSED,
            Compressor.SNAPPY,
            Compressor.LZ4,
            Compressor.GZIP,
            Compressor.ZSTD,
            Compressor.LZMA2,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.UNCOMPRESSED,
            Compressor.SNAPPY,
            Compressor.LZ4,
            Compressor.GZIP,
            Compressor.ZSTD,
            Compressor.LZMA2,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.UNCOMPRESSED,
            Compressor.SNAPPY,
            Compressor.LZ4,
            Compressor.GZIP,
            Compressor.ZSTD,
            Compressor.LZMA2,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
        ]
        session.create_multi_time_series(
            ts_path_lst,
            data_type_lst,
            encoding_lst,
            compressor_lst,
            props_lst=None,
            tags_lst=None,
            attributes_lst=None,
            alias_lst=None,
        )

        expect = 9
        session.insert_records(
            [
                "root.test7.g1.d1",
                "root.test7.g1.d2",
                "root.test7.g1.d3",
                "root.test7.g1.d1",
                "root.test7.g1.d2",
                "root.test7.g1.d3",
                "root.test7.g1.d1",
                "root.test7.g1.d2",
                "root.test7.g1.d3",
            ],
            [1, 2, 3, 4, 5, 6, 7, 8, 9],
            [
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
            ],
            [
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
            ],
            [
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
                    True,
                    -0,
                    -0,
                    -0.0,
                    -0.0,
                    "test01",
                    11,
                    date(1970, 1, 1),
                    "Hello, World!".encode("utf-8"),
                    "string01",
                ],
                [
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
                ],
            ],
        )
        # Calculate the number of rows in the actual time series
        actual = 0
        with session.execute_query_statement(
            "select * from root.test7.g1.d1"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        with session.execute_query_statement(
            "select * from root.test7.g1.d2"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        with session.execute_query_statement(
            "select * from root.test7.g1.d3"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        # Determine whether it meets expectations
        assert expect == actual
        session.close()


# Test writing multiple Record data to aligned time series
def test_insert_aligned_records():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open()

        # Preparation before testing
        session.set_storage_group("root.test8.g1")
        measurements_lst = [
            "BOOLEAN",
            "INT32",
            "INT64",
            "FLOAT",
            "DOUBLE",
            "TEXT",
            "TS",
            "DATE",
            "BLOB",
            "STRING",
        ]
        data_type_lst = [
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
        encoding_lst = [
            TSEncoding.RLE,
            TSEncoding.CHIMP,
            TSEncoding.ZIGZAG,
            TSEncoding.RLBE,
            TSEncoding.SPRINTZ,
            TSEncoding.DICTIONARY,
            TSEncoding.TS_2DIFF,
            TSEncoding.CHIMP,
            TSEncoding.PLAIN,
            TSEncoding.DICTIONARY,
        ]
        compressor_lst = [
            Compressor.UNCOMPRESSED,
            Compressor.SNAPPY,
            Compressor.LZ4,
            Compressor.GZIP,
            Compressor.ZSTD,
            Compressor.LZMA2,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
        ]
        device11_id = "root.test8.g1.d1"
        session.create_aligned_time_series(
            device11_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst
        )
        device11_id = "root.test8.g1.d2"
        session.create_aligned_time_series(
            device11_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst
        )
        device11_id = "root.test8.g1.d3"
        session.create_aligned_time_series(
            device11_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst
        )

        expect = 9
        session.insert_aligned_records(
            [
                "root.test8.g1.d1",
                "root.test8.g1.d2",
                "root.test8.g1.d3",
                "root.test8.g1.d1",
                "root.test8.g1.d2",
                "root.test8.g1.d3",
                "root.test8.g1.d1",
                "root.test8.g1.d2",
                "root.test8.g1.d3",
            ],
            [1, 2, 3, 4, 5, 6, 7, 8, 9],
            [
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
            ],
            [
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
            ],
            [
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
                    True,
                    -0,
                    -0,
                    -0.0,
                    -0.0,
                    "test01",
                    11,
                    date(1970, 1, 1),
                    "Hello, World!".encode("utf-8"),
                    "string01",
                ],
                [
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
                ],
            ],
        )
        # Calculate the number of rows in the actual time series
        actual = 0
        with session.execute_query_statement(
            "select * from root.test8.g1.d1"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        with session.execute_query_statement(
            "select * from root.test8.g1.d2"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        with session.execute_query_statement(
            "select * from root.test8.g1.d3"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        # Determine whether it meets expectations
        assert expect == actual
        session.close()


# Test inserting multiple Record data belonging to the same non aligned device
def test_insert_records_of_one_device():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open()

        # Preparation before testing
        session.set_storage_group("root.test9.g1")
        ts_path_lst = [
            "root.test9.g1.d1.BOOLEAN",
            "root.test9.g1.d1.INT32",
            "root.test9.g1.d1.INT64",
            "root.test9.g1.d1.FLOAT",
            "root.test9.g1.d1.DOUBLE",
            "root.test9.g1.d1.TEXT",
            "root.test9.g1.d1.TS",
            "root.test9.g1.d1.DATE",
            "root.test9.g1.d1.BLOB",
            "root.test9.g1.d1.STRING",
        ]
        data_type_lst = [
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
        encoding_lst = [
            TSEncoding.RLE,
            TSEncoding.CHIMP,
            TSEncoding.ZIGZAG,
            TSEncoding.RLBE,
            TSEncoding.SPRINTZ,
            TSEncoding.DICTIONARY,
            TSEncoding.TS_2DIFF,
            TSEncoding.CHIMP,
            TSEncoding.PLAIN,
            TSEncoding.DICTIONARY,
        ]
        compressor_lst = [
            Compressor.UNCOMPRESSED,
            Compressor.SNAPPY,
            Compressor.LZ4,
            Compressor.GZIP,
            Compressor.ZSTD,
            Compressor.LZMA2,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
        ]
        session.create_multi_time_series(
            ts_path_lst,
            data_type_lst,
            encoding_lst,
            compressor_lst,
            props_lst=None,
            tags_lst=None,
            attributes_lst=None,
            alias_lst=None,
        )

        expect = 9
        session.insert_records_of_one_device(
            "root.test9.g1.d1",
            [1, 2, 3, 4, 5, 6, 7, 8, 9],
            [
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
            ],
            [
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
            ],
            [
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
                    True,
                    -0,
                    -0,
                    -0.0,
                    -0.0,
                    "test01",
                    11,
                    date(1970, 1, 1),
                    "Hello, World!".encode("utf-8"),
                    "string01",
                ],
                [
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
                ],
            ],
        )
        # Calculate the number of rows in the actual time series
        actual = 0
        with session.execute_query_statement(
            "select * from root.test9.g1.d1"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        # Determine whether it meets expectations
        assert expect == actual
        session.close()


# Test inserting multiple Record data belonging to the same aligned device
def test_insert_aligned_records_of_one_device():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open()

        # Preparation before testing
        session.set_storage_group("root.test10.g1")
        measurements_lst = [
            "BOOLEAN",
            "INT32",
            "INT64",
            "FLOAT",
            "DOUBLE",
            "TEXT",
            "TS",
            "DATE",
            "BLOB",
            "STRING",
        ]
        data_type_lst = [
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
        encoding_lst = [
            TSEncoding.RLE,
            TSEncoding.CHIMP,
            TSEncoding.ZIGZAG,
            TSEncoding.RLBE,
            TSEncoding.SPRINTZ,
            TSEncoding.DICTIONARY,
            TSEncoding.TS_2DIFF,
            TSEncoding.CHIMP,
            TSEncoding.PLAIN,
            TSEncoding.DICTIONARY,
        ]
        compressor_lst = [
            Compressor.UNCOMPRESSED,
            Compressor.SNAPPY,
            Compressor.LZ4,
            Compressor.GZIP,
            Compressor.ZSTD,
            Compressor.LZMA2,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
        ]
        device11_id = "root.test10.g1.d1"
        session.create_aligned_time_series(
            device11_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst
        )
        expect = 9
        session.insert_aligned_records_of_one_device(
            "root.test10.g1.d1",
            [1, 2, 3, 4, 5, 6, 7, 8, 9],
            [
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
                [
                    "BOOLEAN",
                    "INT32",
                    "INT64",
                    "FLOAT",
                    "DOUBLE",
                    "TEXT",
                    "TS",
                    "DATE",
                    "BLOB",
                    "STRING",
                ],
            ],
            [
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
            ],
            [
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
                    True,
                    -0,
                    -0,
                    -0.0,
                    -0.0,
                    "test01",
                    11,
                    date(1970, 1, 1),
                    "Hello, World!".encode("utf-8"),
                    "string01",
                ],
                [
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
                ],
            ],
        )
        # Calculate the number of rows in the actual time series
        actual = 0
        with session.execute_query_statement(
            "select * from root.test10.g1.d1"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        # Determine whether it meets expectations
        assert expect == actual
        session.close()


# Test writes with type inference
def test_insert_str_record():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open()

        # Preparation before testing
        session.set_storage_group("root.test11.g1")
        ts_path_lst = [
            "root.test9.g1.d1.BOOLEAN",
            "root.test9.g1.d1.INT32",
            "root.test9.g1.d1.INT64",
            "root.test9.g1.d1.FLOAT",
            "root.test9.g1.d1.DOUBLE",
            "root.test9.g1.d1.TEXT",
            "root.test9.g1.d1.TS",
            "root.test9.g1.d1.DATE",
            "root.test9.g1.d1.BLOB",
            "root.test9.g1.d1.STRING",
        ]
        data_type_lst = [
            TSDataType.TEXT,
            TSDataType.DOUBLE,
            TSDataType.DOUBLE,
            TSDataType.DOUBLE,
            TSDataType.DOUBLE,
            TSDataType.TEXT,
            TSDataType.DOUBLE,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
        ]
        encoding_lst = [
            TSEncoding.PLAIN,
            TSEncoding.PLAIN,
            TSEncoding.PLAIN,
            TSEncoding.PLAIN,
            TSEncoding.PLAIN,
            TSEncoding.PLAIN,
            TSEncoding.PLAIN,
            TSEncoding.PLAIN,
            TSEncoding.PLAIN,
            TSEncoding.PLAIN,
        ]
        compressor_lst = [
            Compressor.UNCOMPRESSED,
            Compressor.SNAPPY,
            Compressor.LZ4,
            Compressor.GZIP,
            Compressor.ZSTD,
            Compressor.LZMA2,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
            Compressor.GZIP,
        ]
        session.create_multi_time_series(
            ts_path_lst,
            data_type_lst,
            encoding_lst,
            compressor_lst,
            props_lst=None,
            tags_lst=None,
            attributes_lst=None,
            alias_lst=None,
        )

        expect = 1
        session.insert_str_record(
            "root.test11.g1.d1",
            1,
            [
                "BOOLEAN",
                "INT32",
                "INT64",
                "FLOAT",
                "DOUBLE",
                "TEXT",
                "TS",
                "DATE",
                "BLOB",
                "STRING",
            ],
            [
                "true",
                "10",
                "11",
                "11.11",
                "10011.1",
                "test01",
                "11",
                "1970-01-01",
                'b"x12x34"',
                "string01",
            ],
        )

        # Calculate the number of rows in the actual time series
        actual = 0
        with session.execute_query_statement(
            "select * from root.test11.g1.d1"
        ) as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        # Determine whether it meets expectations
        assert expect == actual
        session.close()


# Test automatic create
def test_insert_auto_create():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open()

        expect = 360
        # Test non aligned tablet writing
        num = 0
        for i in range(1, 10):
            device_id = "root.repeat.g1.fd_a" + str(num)
            num = num + 1
            measurements_ = [
                "BOOLEAN",
                "INT32",
                "INT64",
                "FLOAT",
                "DOUBLE",
                "TEXT",
                "TS",
                "DATE",
                "BLOB",
                "STRING",
            ]
            data_types_ = [
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
            values_ = [
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
            ]
            timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
            tablet_ = Tablet(
                device_id, measurements_, data_types_, values_, timestamps_
            )
            session.insert_tablet(tablet_)

        # Test non aligned numpy tablet writing
        num = 0
        for i in range(1, 10):
            device_id = "root.repeat.g1.fd_b" + str(num)
            num = num + 1
            measurements_ = [
                "BOOLEAN",
                "INT32",
                "INT64",
                "FLOAT",
                "DOUBLE",
                "TEXT",
                "TS",
                "DATE",
                "BLOB",
                "STRING",
            ]
            data_types_ = [
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
            np_values_ = [
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
            np_timestamps_ = np.array(
                [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype()
            )
            np_tablet_ = NumpyTablet(
                device_id, measurements_, data_types_, np_values_, np_timestamps_
            )
            session.insert_tablet(np_tablet_)

        # Test aligning tablet writing
        num = 0
        for i in range(1, 10):
            device_id = "root.repeat.g1.d_c" + str(num)
            num = num + 1
            measurements_ = [
                "BOOLEAN",
                "INT32",
                "INT64",
                "FLOAT",
                "DOUBLE",
                "TEXT",
                "TS",
                "DATE",
                "BLOB",
                "STRING",
            ]
            data_types_ = [
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
            values_ = [
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
                [
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
                ],
            ]
            timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
            tablet_ = Tablet(
                device_id, measurements_, data_types_, values_, timestamps_
            )
            session.insert_aligned_tablet(tablet_)

        # Test alignment of numpy tablet writing
        num = 0
        for i in range(1, 10):
            device_id = "root.repeat.g1.d_d" + str(num)
            num = num + 1
            measurements_ = [
                "BOOLEAN",
                "INT32",
                "INT64",
                "FLOAT",
                "DOUBLE",
                "TEXT",
                "TS",
                "DATE",
                "BLOB",
                "STRING",
            ]
            data_types_ = [
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
            np_values_ = [
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
            np_timestamps_ = np.array(
                [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype()
            )
            np_tablet_ = NumpyTablet(
                device_id, measurements_, data_types_, np_values_, np_timestamps_
            )
            session.insert_aligned_tablet(np_tablet_)

        # Calculate the number of rows in the actual time series
        actual = 0
        with session.execute_query_statement("show timeseries") as session_data_set:
            session_data_set.set_fetch_size(1024)
            while session_data_set.has_next():
                print(session_data_set.next())
                actual = actual + 1
        # Determine whether it meets expectations
        assert expect == actual
        session.close()
