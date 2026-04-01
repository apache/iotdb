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
# session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="Asia/Shanghai", enable_redirection=True)
session = Session.init_from_node_urls(
    node_urls=["127.0.0.1:6667", "127.0.0.1:6668", "127.0.0.1:6669"],
    user="root",
    password="root",
    fetch_size=1024,
    zone_id="Asia/Shanghai",
    enable_redirection=True,
)
session.open(False)

# create and delete databases
session.set_storage_group("root.sg_test_01")

# setting time series.
session.create_time_series(
    "root.sg_test_01.d_01.s_01", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY
)

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
    "select s_01 from root.sg_test_01.d_04 limit 1"
) as dataset:
    print(dataset.get_column_names())
    while dataset.has_next():
        print(dataset.next())

with session.execute_query_statement(
    "select s_01 from root.sg_test_01.d_04 limit 1"
) as dataset:
    df = dataset.todf()
    print(df.to_string())

# delete database
session.delete_storage_group("root.sg_test_01")

# close session connection.
session.close()

print("All executions done!!")
