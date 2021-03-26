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
from iotdb.utils.Tablet import Tablet
from iotdb.utils.IoTDBConstants import *

# whether the test has passed
final_flag = True
failed_count = 0

def test_fail(message):
  global failed_count
  global final_flag
  print("*********")
  print(message)
  print("*********")
  final_flag = False
  failed_count += 1

# creating session connection.
ip = "127.0.0.1"
port_ = "6667"
username_ = 'root'
password_ = 'root'
session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id='UTC+8')
session.open(False)

if not session.is_open():
  print("can't open session")
  exit(1)

# set and delete storage groups
session.set_storage_group("root.sg_test_01")
session.set_storage_group("root.sg_test_02")
session.set_storage_group("root.sg_test_03")
session.set_storage_group("root.sg_test_04")

if session.delete_storage_group("root.sg_test_02") < 0:
  test_fail("delete storage group failed")

if session.delete_storage_groups(["root.sg_test_03", "root.sg_test_04"]) < 0:
  test_fail("delete storage groups failed")

# setting time series.
session.create_time_series("root.sg_test_01.d_01.s_01", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY)
session.create_time_series("root.sg_test_01.d_01.s_02", TSDataType.INT32, TSEncoding.PLAIN, Compressor.SNAPPY)
session.create_time_series("root.sg_test_01.d_01.s_03", TSDataType.INT64, TSEncoding.PLAIN, Compressor.SNAPPY)

# setting multiple time series once.
ts_path_lst_ = ["root.sg_test_01.d_01.s_04", "root.sg_test_01.d_01.s_05", "root.sg_test_01.d_01.s_06",
                "root.sg_test_01.d_01.s_07", "root.sg_test_01.d_01.s_08", "root.sg_test_01.d_01.s_09"]
data_type_lst_ = [TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
                  TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT]
encoding_lst_ = [TSEncoding.PLAIN for _ in range(len(data_type_lst_))]
compressor_lst_ = [Compressor.SNAPPY for _ in range(len(data_type_lst_))]
session.create_multi_time_series(ts_path_lst_, data_type_lst_, encoding_lst_, compressor_lst_)

# delete time series
if session.delete_time_series(["root.sg_test_01.d_01.s_07", "root.sg_test_01.d_01.s_08", "root.sg_test_01.d_01.s_09"]) < 0:
  test_fail("delete time series failed")

# checking time series
# s_07 expecting False
if session.check_time_series_exists("root.sg_test_01.d_01.s_07"):
  test_fail("root.sg_test_01.d_01.s_07 shouldn't exist")

# s_03 expecting True
if not session.check_time_series_exists("root.sg_test_01.d_01.s_03"):
  test_fail("root.sg_test_01.d_01.s_03 should exist")

# insert one record into the database.
measurements_ = ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"]
values_ = [False, 10, 11, 1.1, 10011.1, "test_record"]
data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64,
               TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT]
if session.insert_record("root.sg_test_01.d_01", 1, measurements_, data_types_, values_) < 0:
  test_fail("insert record failed")

# insert multiple records into database
measurements_list_ = [["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"],
                      ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"]]
values_list_ = [[False, 22, 33, 4.4, 55.1, "test_records01"],
                [True, 77, 88, 1.25, 8.125, "test_records02"]]
data_type_list_ = [data_types_, data_types_]
device_ids_ = ["root.sg_test_01.d_01", "root.sg_test_01.d_01"]
if session.insert_records(device_ids_, [2, 3], measurements_list_, data_type_list_, values_list_) < 0:
  test_fail("insert records failed")

# insert one tablet into the database.
values_ = [[False, 10, 11, 1.1, 10011.1, "test01"],
           [True, 100, 11111, 1.25, 101.0, "test02"],
           [False, 100, 1, 188.1, 688.25, "test03"],
           [True, 0, 0, 0, 6.25, "test04"]]  # Non-ASCII text will cause error since bytes can only hold 0-128 nums.
timestamps_ = [4, 5, 6, 7]
tablet_ = Tablet("root.sg_test_01.d_01", measurements_, data_types_, values_, timestamps_)
if session.insert_tablet(tablet_) < 0:
  test_fail("insert tablet failed")

# insert multiple tablets into database
tablet_01 = Tablet("root.sg_test_01.d_01", measurements_, data_types_, values_, [8, 9, 10, 11])
tablet_02 = Tablet("root.sg_test_01.d_01", measurements_, data_types_, values_, [12, 13, 14, 15])
if session.insert_tablets([tablet_01, tablet_02]) < 0:
  test_fail("insert tablets failed")

# insert records of one device
time_list = [1, 2, 3]
measurements_list = [["s_01", "s_02", "s_03"], ["s_01", "s_02", "s_03"], ["s_01", "s_02", "s_03"]]
data_types_list = [[TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64],
                   [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64],
                   [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64]]
values_list = [[False, 22, 33], [True, 1, 23], [False, 15, 26]]

if session.insert_records_of_one_device("root.sg_test_01.d_01", time_list, measurements_list, data_types_list, values_list) < 0:
  test_fail("insert records of one device failed")

# execute non-query sql statement
if session.execute_non_query_statement("insert into root.sg_test_01.d_01(timestamp, s_02) values(16, 188)") < 0:
  test_fail("execute 'insert into root.sg_test_01.d_01(timestamp, s_02) values(16, 188)' failed")

# execute sql query statement
session_data_set = session.execute_query_statement("select * from root.sg_test_01.d_01")
session_data_set.set_fetch_size(1024)
expect_count = 16
actual_count = 0
while session_data_set.has_next():
  actual_count += 1
session_data_set.close_operation_handle()

if actual_count != expect_count:
  test_fail("query count mismatch: expect count: "
            + str(expect_count) + " actual count: " + str(actual_count))

# close session connection.
session.close()

if final_flag:
  print("All executions done!!")
else:
  print("Some test failed, please have a check")
  print("failed count: ", failed_count)
  exit(1)
