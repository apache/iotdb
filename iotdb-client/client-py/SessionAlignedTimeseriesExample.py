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

from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.Tablet import Tablet

# creating session connection.
ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"
session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
session.open(False)

# set and delete databases
session.set_storage_group("root.sg_test_01")
session.set_storage_group("root.sg_test_02")
session.set_storage_group("root.sg_test_03")
session.set_storage_group("root.sg_test_04")
session.delete_storage_group("root.sg_test_02")
session.delete_storage_groups(["root.sg_test_03", "root.sg_test_04"])

# setting aligned time series.
measurements_lst_ = [
    "s_01",
    "s_02",
    "s_03",
]
data_type_lst_ = [
    TSDataType.BOOLEAN,
    TSDataType.INT32,
    TSDataType.INT64,
]
encoding_lst_ = [TSEncoding.PLAIN for _ in range(len(data_type_lst_))]
compressor_lst_ = [Compressor.SNAPPY for _ in range(len(data_type_lst_))]
session.create_aligned_time_series(
    "root.sg_test_01.d_02",
    measurements_lst_,
    data_type_lst_,
    encoding_lst_,
    compressor_lst_,
)

# setting more aligned time series once.
measurements_lst_ = [
    "s_04",
    "s_05",
    "s_06",
    "s_07",
    "s_08",
    "s_09",
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
session.create_aligned_time_series(
    "root.sg_test_01.d_02",
    measurements_lst_,
    data_type_lst_,
    encoding_lst_,
    compressor_lst_,
)

# delete time series
session.delete_time_series(
    [
        "root.sg_test_01.d_02.s_07",
        "root.sg_test_01.d_02.s_08",
        "root.sg_test_01.d_02.s_09",
    ]
)

# checking time series
print(
    "s_07 expecting False, checking result: ",
    session.check_time_series_exists("root.sg_test_01.d_02.s_07"),
)
print(
    "s_03 expecting True, checking result: ",
    session.check_time_series_exists("root.sg_test_01.d_02.s_03"),
)

# insert one aligned record into the database.
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
session.insert_aligned_record(
    "root.sg_test_01.d_02", 1, measurements_, data_types_, values_
)

# insert multiple aligned records into database
measurements_list_ = [
    ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"],
    ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"],
]
values_list_ = [
    [False, 22, 33, 4.4, 55.1, "test_records01"],
    [True, 77, 88, 1.25, 8.125, "test_records02"],
]
data_type_list_ = [data_types_, data_types_]
device_ids_ = ["root.sg_test_01.d_02", "root.sg_test_01.d_02"]
session.insert_aligned_records(
    device_ids_, [2, 3], measurements_list_, data_type_list_, values_list_
)

# insert one aligned tablet into the database.
values_ = [
    [False, 10, 11, 1.1, 10011.1, "test01"],
    [True, 100, 11111, 1.25, 101.0, "test02"],
    [False, 100, 1, 188.1, 688.25, "test03"],
    [True, 0, 0, 0, 6.25, "test04"],
]  # Non-ASCII text will cause error since bytes can only hold 0-128 nums.
timestamps_ = [4, 5, 6, 7]
tablet_ = Tablet(
    "root.sg_test_01.d_02", measurements_, data_types_, values_, timestamps_
)
session.insert_aligned_tablet(tablet_)

# insert multiple aligned tablets into database
tablet_01 = Tablet(
    "root.sg_test_01.d_02", measurements_, data_types_, values_, [8, 9, 10, 11]
)
tablet_02 = Tablet(
    "root.sg_test_01.d_02", measurements_, data_types_, values_, [12, 13, 14, 15]
)
session.insert_aligned_tablets([tablet_01, tablet_02])

# insert one aligned tablet with empty cells into the database.
values_ = [
    [None, 10, 11, 1.1, 10011.1, "test01"],
    [True, None, 11111, 1.25, 101.0, "test02"],
    [False, 100, 1, None, 688.25, "test03"],
    [True, 0, 0, 0, 6.25, None],
]  # Non-ASCII text will cause error since bytes can only hold 0-128 nums.
timestamps_ = [16, 17, 18, 19]
tablet_ = Tablet(
    "root.sg_test_01.d_02", measurements_, data_types_, values_, timestamps_
)
session.insert_aligned_tablet(tablet_)

# insert aligned records of one device
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

session.insert_aligned_records_of_one_device(
    "root.sg_test_01.d_02", time_list, measurements_list, data_types_list, values_list
)

# execute non-query sql statement
session.execute_non_query_statement(
    "insert into root.sg_test_01.d_02(timestamp, s_02) aligned values(16, 188)"
)

# execute sql query statement
with session.execute_query_statement(
    "select * from root.sg_test_01.d_02"
) as session_data_set:
    session_data_set.set_fetch_size(1024)
    while session_data_set.has_next():
        print(session_data_set.next())

# insert aligned string record into the database.
time_list = [1, 2, 3]
measurements_list = [
    ["s_01", "s_02", "s_03"],
    ["s_01", "s_02", "s_03"],
    ["s_01", "s_02", "s_03"],
]
values_list = [["False", "22", "33"], ["True", "1", "23"], ["False", "15", "26"]]
session.insert_aligned_string_records_of_one_device(
    "root.sg_test_01.d_04",
    time_list,
    measurements_list,
    values_list,
)

# delete database
session.delete_storage_group("root.sg_test_01")

# close session connection.
session.close()

print("All executions done!!")
