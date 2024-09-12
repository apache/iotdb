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
from datetime import date
from iotdb.Session import Session
from iotdb.utils.BitMap import BitMap
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.Tablet import Tablet
from iotdb.utils.NumpyTablet import NumpyTablet

# creating session connection.
ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"
# session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8", enable_redirection=True)
session = Session.init_from_node_urls(
    node_urls=["127.0.0.1:6667", "127.0.0.1:6668", "127.0.0.1:6669"],
    user="root",
    password="root",
    fetch_size=1024,
    zone_id="UTC+8",
    enable_redirection=True,
)
session.open(False)

# create and delete databases
session.set_storage_group("root.sg_test_01")
session.set_storage_group("root.sg_test_02")
session.set_storage_group("root.sg_test_03")
session.set_storage_group("root.sg_test_04")
session.delete_storage_group("root.sg_test_02")
session.delete_storage_groups(["root.sg_test_03", "root.sg_test_04"])

# setting time series.
session.create_time_series(
    "root.sg_test_01.d_01.s_01", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY
)
session.create_time_series(
    "root.sg_test_01.d_01.s_02", TSDataType.INT32, TSEncoding.PLAIN, Compressor.SNAPPY
)
session.create_time_series(
    "root.sg_test_01.d_01.s_03", TSDataType.INT64, TSEncoding.PLAIN, Compressor.SNAPPY
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
session.delete_time_series(
    [
        "root.sg_test_01.d_01.s_07",
        "root.sg_test_01.d_01.s_08",
        "root.sg_test_01.d_01.s_09",
    ]
)

# checking time series
print(
    "s_07 expecting False, checking result: ",
    session.check_time_series_exists("root.sg_test_01.d_01.s_07"),
)
print(
    "s_03 expecting True, checking result: ",
    session.check_time_series_exists("root.sg_test_01.d_01.s_03"),
)
print(
    "d_02.s_01 expecting True, checking result: ",
    session.check_time_series_exists("root.sg_test_01.d_02.s_01"),
)
print(
    "d_02.s_06 expecting True, checking result: ",
    session.check_time_series_exists("root.sg_test_01.d_02.s_06"),
)

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
session.insert_record("root.sg_test_01.d_01", 1, measurements_, data_types_, values_)

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
device_ids_ = ["root.sg_test_01.d_01", "root.sg_test_01.d_01"]
session.insert_records(
    device_ids_, [2, 3], measurements_list_, data_type_list_, values_list_
)

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
session.insert_tablet(tablet_)

# insert one numpy tablet into the database.
np_values_ = [
    np.array([False, True, False, True], TSDataType.BOOLEAN.np_dtype()),
    np.array([10, 100, 100, 0], TSDataType.INT32.np_dtype()),
    np.array([11, 11111, 1, 0], TSDataType.INT64.np_dtype()),
    np.array([1.1, 1.25, 188.1, 0], TSDataType.FLOAT.np_dtype()),
    np.array([10011.1, 101.0, 688.25, 6.25], TSDataType.DOUBLE.np_dtype()),
    np.array(["test01", "test02", "test03", "test04"], TSDataType.TEXT.np_dtype()),
]
np_timestamps_ = np.array([1, 2, 3, 4], TSDataType.INT64.np_dtype())
np_tablet_ = NumpyTablet(
    "root.sg_test_01.d_02", measurements_, data_types_, np_values_, np_timestamps_
)
session.insert_tablet(np_tablet_)

# insert one unsorted numpy tablet into the database.
np_values_unsorted = [
    np.array([False, False, False, True, True], np.dtype(">?")),
    np.array([0, 10, 100, 1000, 10000], np.dtype(">i4")),
    np.array([1, 11, 111, 1111, 11111], np.dtype(">i8")),
    np.array([1.1, 1.25, 188.1, 0, 8.999], np.dtype(">f4")),
    np.array([10011.1, 101.0, 688.25, 6.25, 8, 776], np.dtype(">f8")),
    np.array(["test09", "test08", "test07", "test06", "test05"]),
]
np_timestamps_unsorted = np.array([9, 8, 7, 6, 5], np.dtype(">i8"))
np_tablet_unsorted = NumpyTablet(
    "root.sg_test_01.d_02",
    measurements_,
    data_types_,
    np_values_unsorted,
    np_timestamps_unsorted,
)

# insert one numpy tablet into the database.
np_values_ = [
    np.array([False, True, False, True], TSDataType.BOOLEAN.np_dtype()),
    np.array([10, 100, 100, 0], TSDataType.INT32.np_dtype()),
    np.array([11, 11111, 1, 0], TSDataType.INT64.np_dtype()),
    np.array([1.1, 1.25, 188.1, 0], TSDataType.FLOAT.np_dtype()),
    np.array([10011.1, 101.0, 688.25, 6.25], TSDataType.DOUBLE.np_dtype()),
    np.array(["test01", "test02", "test03", "test04"]),
]
np_timestamps_ = np.array([98, 99, 100, 101], TSDataType.INT64.np_dtype())
np_bitmaps_ = []
for i in range(len(measurements_)):
    np_bitmaps_.append(BitMap(len(np_timestamps_)))
np_bitmaps_[0].mark(0)
np_bitmaps_[1].mark(1)
np_bitmaps_[2].mark(2)
np_bitmaps_[4].mark(3)
np_bitmaps_[5].mark(3)
np_tablet_with_none = NumpyTablet(
    "root.sg_test_01.d_02",
    measurements_,
    data_types_,
    np_values_,
    np_timestamps_,
    np_bitmaps_,
)
session.insert_tablet(np_tablet_with_none)


session.insert_tablet(np_tablet_unsorted)
print(np_tablet_unsorted.get_timestamps())
for value in np_tablet_unsorted.get_values():
    print(value)

# insert multiple tablets into database
tablet_01 = Tablet(
    "root.sg_test_01.d_01", measurements_, data_types_, values_, [8, 9, 10, 11]
)
tablet_02 = Tablet(
    "root.sg_test_01.d_01", measurements_, data_types_, values_, [12, 13, 14, 15]
)
session.insert_tablets([tablet_01, tablet_02])

# insert one tablet with empty cells into the database.
values_ = [
    [None, 10, 11, 1.1, 10011.1, "test01"],
    [True, None, 11111, 1.25, 101.0, "test02"],
    [False, 100, 1, None, 688.25, "test03"],
    [True, 0, 0, 0, 6.25, None],
]  # Non-ASCII text will cause error since bytes can only hold 0-128 nums.
timestamps_ = [16, 17, 18, 19]
tablet_ = Tablet(
    "root.sg_test_01.d_01", measurements_, data_types_, values_, timestamps_
)
session.insert_tablet(tablet_)

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

session.insert_records_of_one_device(
    "root.sg_test_01.d_01", time_list, measurements_list, data_types_list, values_list
)

# execute non-query sql statement
session.execute_non_query_statement(
    "insert into root.sg_test_01.d_01(timestamp, s_02) values(16, 188)"
)

# execute sql query statement
with session.execute_query_statement(
    "select * from root.sg_test_01.d_01"
) as session_data_set:
    session_data_set.set_fetch_size(1024)
    while session_data_set.has_next():
        print(session_data_set.next())
# execute sql query statement
with session.execute_query_statement(
    "select s_01, s_02, s_03, s_04, s_05, s_06 from root.sg_test_01.d_02"
) as session_data_set:
    session_data_set.set_fetch_size(1024)
    while session_data_set.has_next():
        print(session_data_set.next())

# execute statement
with session.execute_statement(
    "select * from root.sg_test_01.d_01"
) as session_data_set:
    while session_data_set.has_next():
        print(session_data_set.next())

session.execute_statement(
    "insert into root.sg_test_01.d_01(timestamp, s_02) values(16, 188)"
)

# insert string records of one device
time_list = [1, 2, 3]
measurements_list = [
    ["s_01", "s_02", "s_03"],
    ["s_01", "s_02", "s_03"],
    ["s_01", "s_02", "s_03"],
]
values_list = [["False", "22", "33"], ["True", "1", "23"], ["False", "15", "26"]]

session.insert_string_records_of_one_device(
    "root.sg_test_01.d_03",
    time_list,
    measurements_list,
    values_list,
)

with session.execute_raw_data_query(
    ["root.sg_test_01.d_03.s_01", "root.sg_test_01.d_03.s_02"], 1, 4
) as session_data_set:
    session_data_set.set_fetch_size(1024)
    while session_data_set.has_next():
        print(session_data_set.next())

with session.execute_last_data_query(
    ["root.sg_test_01.d_03.s_01", "root.sg_test_01.d_03.s_02"], 0
) as session_data_set:
    session_data_set.set_fetch_size(1024)
    while session_data_set.has_next():
        print(session_data_set.next())

# insert tablet with new data types
measurements_new_type = ["s_01", "s_02", "s_03", "s_04"]
data_types_new_type = [
    TSDataType.DATE,
    TSDataType.TIMESTAMP,
    TSDataType.BLOB,
    TSDataType.STRING,
]
values_new_type = [
    [date(2024, 1, 1), 1, b"\x12\x34", "test01"],
    [date(2024, 1, 2), 2, b"\x12\x34", "test02"],
    [date(2024, 1, 3), 3, b"\x12\x34", "test03"],
    [date(2024, 1, 4), 4, b"\x12\x34", "test04"],
]
timestamps_new_type = [1, 2, 3, 4]
tablet_new_type = Tablet(
    "root.sg_test_01.d_04",
    measurements_new_type,
    data_types_new_type,
    values_new_type,
    timestamps_new_type,
)
session.insert_tablet(tablet_new_type)
np_values_new_type = [
    np.array([date(2024, 2, 4), date(2024, 3, 4), date(2024, 4, 4), date(2024, 5, 4)]),
    np.array([5, 6, 7, 8], TSDataType.INT64.np_dtype()),
    np.array([b"\x12\x34", b"\x12\x34", b"\x12\x34", b"\x12\x34"]),
    np.array(["test01", "test02", "test03", "test04"]),
]
np_timestamps_new_type = np.array([5, 6, 7, 8], TSDataType.INT64.np_dtype())
np_tablet_new_type = NumpyTablet(
    "root.sg_test_01.d_04",
    measurements_new_type,
    data_types_new_type,
    np_values_new_type,
    np_timestamps_new_type,
)
session.insert_tablet(np_tablet_new_type)
with session.execute_query_statement(
    "select s_01,s_02,s_03,s_04 from root.sg_test_01.d_04"
) as dataset:
    print(dataset.get_column_names())
    while dataset.has_next():
        print(dataset.next())

with session.execute_query_statement(
    "select s_01,s_02,s_03,s_04 from root.sg_test_01.d_04"
) as dataset:
    df = dataset.todf()
    print(df.to_string())

# delete database
session.delete_storage_group("root.sg_test_01")

# close session connection.
session.close()

print("All executions done!!")
