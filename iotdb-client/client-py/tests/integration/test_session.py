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

# Uncomment the following line to use apache-iotdb module installed by pip3
import numpy as np

from iotdb.Session import Session
from iotdb.SessionPool import PoolConfig, create_session_pool
from iotdb.utils.BitMap import BitMap
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.NumpyTablet import NumpyTablet
from iotdb.utils.Tablet import Tablet
from .iotdb_container import IoTDBContainer

# whether the test has passed
final_flag = True
failed_count = 0


def test_fail():
    global failed_count
    global final_flag
    final_flag = False
    failed_count += 1


def print_message(message):
    print("*********")
    print(message)
    print("*********")
    assert False


def test_session():
    session_test()


def test_session_pool():
    session_test(True)


def session_test(use_session_pool=False):
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer

        if use_session_pool:
            pool_config = PoolConfig(
                db.get_container_host_ip(),
                db.get_exposed_port(6667),
                "root",
                "root",
                None,
                1024,
                "Asia/Shanghai",
                3,
            )
            session_pool = create_session_pool(pool_config, 1, 3000)
            session = session_pool.get_session()
        else:
            session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open(False)

        if not session.is_open():
            print("can't open session")
            exit(1)

        # set and delete databases
        session.set_storage_group("root.sg_test_01")
        session.set_storage_group("root.sg_test_02")
        session.set_storage_group("root.sg_test_03")
        session.set_storage_group("root.sg_test_04")

        if session.delete_storage_group("root.sg_test_02") < 0:
            test_fail()
            print_message("delete database failed")

        if session.delete_storage_groups(["root.sg_test_03", "root.sg_test_04"]) < 0:
            test_fail()
            print_message("delete databases failed")

        # setting time series.
        session.create_time_series(
            "root.sg_test_01.d_01.s_01",
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            Compressor.SNAPPY,
        )
        session.create_time_series(
            "root.sg_test_01.d_01.s_02",
            TSDataType.INT32,
            TSEncoding.PLAIN,
            Compressor.SNAPPY,
        )
        session.create_time_series(
            "root.sg_test_01.d_01.s_03",
            TSDataType.INT64,
            TSEncoding.PLAIN,
            Compressor.SNAPPY,
        )
        session.create_time_series(
            "root.sg_test_01.d_02.s_01",
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            Compressor.SNAPPY,
            None,
            {"tag1": "v1"},
            {"description": "v1"},
            "temperature",
        )

        # setting multiple time series once.
        ts_path_lst_ = [
            "root.sg_test_01.d_01.s_04",
            "root.sg_test_01.d_01.s_05",
            "root.sg_test_01.d_01.s_06",
            "root.sg_test_01.d_01.s_07",
            "root.sg_test_01.d_01.s_08",
            "root.sg_test_01.d_01.s_09",
        ]
        data_type_lst_ = [
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
            TSDataType.TEXT,
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
            TSDataType.TEXT,
        ]
        encoding_lst_ = [TSEncoding.PLAIN for _ in range(len(data_type_lst_))]
        compressor_lst_ = [Compressor.SNAPPY for _ in range(len(data_type_lst_))]
        session.create_multi_time_series(
            ts_path_lst_, data_type_lst_, encoding_lst_, compressor_lst_
        )
        ts_path_lst_ = [
            "root.sg_test_01.d_02.s_04",
            "root.sg_test_01.d_02.s_05",
            "root.sg_test_01.d_02.s_06",
            "root.sg_test_01.d_02.s_07",
            "root.sg_test_01.d_02.s_08",
            "root.sg_test_01.d_02.s_09",
        ]
        data_type_lst_ = [
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
            TSDataType.TEXT,
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
            TSDataType.TEXT,
        ]
        encoding_lst_ = [TSEncoding.PLAIN for _ in range(len(data_type_lst_))]
        compressor_lst_ = [Compressor.SNAPPY for _ in range(len(data_type_lst_))]
        tags_lst_ = [{"tag2": "v2"} for _ in range(len(data_type_lst_))]
        attributes_lst_ = [{"description": "v2"} for _ in range(len(data_type_lst_))]
        session.create_multi_time_series(
            ts_path_lst_,
            data_type_lst_,
            encoding_lst_,
            compressor_lst_,
            None,
            tags_lst_,
            attributes_lst_,
            None,
        )

        # delete time series
        if (
            session.delete_time_series(
                [
                    "root.sg_test_01.d_01.s_07",
                    "root.sg_test_01.d_01.s_08",
                    "root.sg_test_01.d_01.s_09",
                ]
            )
            < 0
        ):
            test_fail()
            print_message("delete time series failed")

        # checking time series
        # s_07 expecting False
        if session.check_time_series_exists("root.sg_test_01.d_01.s_07"):
            test_fail()
            print_message("root.sg_test_01.d_01.s_07 shouldn't exist")

        # s_03 expecting True
        if not session.check_time_series_exists("root.sg_test_01.d_01.s_03"):
            test_fail()
            print_message("root.sg_test_01.d_01.s_03 should exist")
        # d_02.s_01 expecting True
        if not session.check_time_series_exists("root.sg_test_01.d_02.s_01"):
            test_fail()
            print_message("root.sg_test_01.d_02.s_01 should exist")
        # d_02.s_06 expecting True
        if not session.check_time_series_exists("root.sg_test_01.d_02.s_06"):
            test_fail()
            print_message("root.sg_test_01.d_02.s_06 should exist")

        # insert one record into the database.
        measurements_ = ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"]
        values_ = [False, 10, 11, 1.1, 10011.1, "test_record"]
        data_types_ = [
            TSDataType.BOOLEAN,
            TSDataType.INT32,
            TSDataType.INT64,
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
            TSDataType.TEXT,
        ]
        if (
            session.insert_record(
                "root.sg_test_01.d_01", 1, measurements_, data_types_, values_
            )
            < 0
        ):
            test_fail()
            print_message("insert record failed")

        # insert multiple records into database
        measurements_list_ = [
            ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"],
            ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"],
        ]
        values_list_ = [
            [False, 22, 33, 4.4, 55.1, "test_records01"],
            [True, 77, 88, 1.25, 8.125, bytes("test_records02", "utf-8")],
        ]
        data_type_list_ = [data_types_, data_types_]
        device_ids_ = ["root.sg_test_01.d_01", "root.sg_test_01.d_02"]
        if (
            session.insert_records(
                device_ids_, [2, 3], measurements_list_, data_type_list_, values_list_
            )
            < 0
        ):
            test_fail()
            print_message("insert records failed")

        # insert one tablet into the database.
        values_ = [
            [False, 10, 11, 1.1, 10011.1, "test01"],
            [True, 100, 11111, 1.25, 101.0, "test02"],
            [False, 100, 1, 188.1, 688.25, "test03"],
            [True, 0, 0, 0, 6.25, "test04"],
        ]  # Non-ASCII text will cause error since bytes can only hold 0-128 nums.
        timestamps_ = [4, 5, 6, 7]
        tablet_ = Tablet(
            "root.sg_test_01.d_01", measurements_, data_types_, values_, timestamps_
        )

        if session.insert_tablet(tablet_) < 0:
            test_fail()
            print_message("insert tablet failed")

        # insert one numpy tablet into the database.
        np_values_ = [
            np.array([False, True, False, True], np.dtype(">?")),
            np.array([10, 100, 100, 0], np.dtype(">i4")),
            np.array([11, 11111, 1, 0], np.dtype(">i8")),
            np.array([1.1, 1.25, 188.1, 0], np.dtype(">f4")),
            np.array([10011.1, 101.0, 688.25, 6.25], np.dtype(">f8")),
            np.array(["test01", "test02", "test03", "test04"]),
        ]
        np_timestamps_ = np.array([1, 2, 3, 4], np.dtype(">i8"))
        np_tablet_ = NumpyTablet(
            "root.sg_test_01.d_02",
            measurements_,
            data_types_,
            np_values_,
            np_timestamps_,
        )
        if session.insert_tablet(np_tablet_) < 0:
            test_fail()
            print_message("insert numpy tablet failed")

        # insert multiple tablets into database
        tablet_01 = Tablet(
            "root.sg_test_01.d_01", measurements_, data_types_, values_, [8, 9, 10, 11]
        )
        tablet_02 = Tablet(
            "root.sg_test_01.d_02",
            measurements_,
            data_types_,
            values_,
            [12, 13, 14, 15],
        )
        if session.insert_tablets([tablet_01, tablet_02]) < 0:
            test_fail()
            print_message("insert tablets failed")

        # insert one tablet with empty cells into the database.
        values_ = [
            [None, 10, 11, 1.1, 10011.1, "test01"],
            [True, None, 11111, 1.25, 101.0, "test02"],
            [False, 100, 1, None, 688.25, "test03"],
            [True, 0, 0, 0, None, None],
        ]  # Non-ASCII text will cause error since bytes can only hold 0-128 nums.
        timestamps_ = [20, 21, 22, 23]
        tablet_ = Tablet(
            "root.sg_test_01.d_01", measurements_, data_types_, values_, timestamps_
        )
        if session.insert_tablet(tablet_) < 0:
            test_fail()
            print_message("insert tablet with empty cells failed")

        # insert one numpy tablet with empty cells into the database.
        np_values_ = [
            np.array([False, True, False, True], np.dtype(">?")),
            np.array([10, 0, 100, 0], np.dtype(">i4")),
            np.array([11, 11111, 0, 0], np.dtype(">i8")),
            np.array([1.1, 1.25, 188.1, 0], np.dtype(">f4")),
            np.array([10011.1, 101.0, 688.25, 0], np.dtype(">f8")),
            np.array(["test01", "test02", "test03", ""]),
        ]
        np_timestamps_ = np.array([30, 31, 32, 33], np.dtype(">i8"))
        np_bitmaps_ = []
        for _ in range(len(measurements_)):
            np_bitmaps_.append(BitMap(len(np_timestamps_)))
        np_bitmaps_[0].mark(0)
        np_bitmaps_[1].mark(1)
        np_bitmaps_[2].mark(2)
        np_bitmaps_[4].mark(3)
        np_bitmaps_[5].mark(3)
        np_tablet_ = NumpyTablet(
            "root.sg_test_01.d_01",
            measurements_,
            data_types_,
            np_values_,
            np_timestamps_,
            np_bitmaps_,
        )
        if session.insert_tablet(np_tablet_) < 0:
            test_fail()
            print_message("insert numpy tablet with empty cells failed")

        # insert records of one device
        time_list = [1, 2, 3]
        measurements_list = [
            ["s_01", "s_02", "s_03"],
            ["s_01", "s_02", "s_03"],
            ["s_01", "s_02", "s_03"],
        ]
        data_types_list = [
            [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64],
            [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64],
            [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64],
        ]
        values_list = [[False, 22, 33], [True, 1, 23], [False, 15, 26]]

        if (
            session.insert_records_of_one_device(
                "root.sg_test_01.d_01",
                time_list,
                measurements_list,
                data_types_list,
                values_list,
            )
            < 0
        ):
            test_fail()
            print_message("insert records of one device failed")

        # execute non-query sql statement
        try:
            session.execute_non_query_statement(
                "insert into root.sg_test_01.d_01(timestamp, s_02) values(16, 188)"
            )
        except Exception:
            test_fail()
            print_message(
                "execute 'insert into root.sg_test_01.d_01(timestamp, s_02) values(16, 188)' failed"
            )

        # execute sql query statement
        session_data_set = session.execute_query_statement(
            "select * from root.sg_test_01.d_01"
        )
        session_data_set.set_fetch_size(1024)
        expect_count = 20
        actual_count = 0
        while session_data_set.has_next():
            print(session_data_set.next())
            actual_count += 1
        session_data_set.close_operation_handle()

        if actual_count != expect_count:
            test_fail()
            print_message(
                "query count mismatch: expect count: "
                + str(expect_count)
                + " actual count: "
                + str(actual_count)
            )

        # close session connection.
        session.close()


if final_flag:
    print("All executions done!!")
else:
    print("Some test failed, please have a check")
    print("failed count: ", failed_count)
    exit(1)
