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
import threading

from iotdb.SessionPool import PoolConfig, SessionPool
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor

STORAGE_GROUP_NAME = "root.test"


def prepare_data():
    print("create database")
    # Get a session from the pool
    session = session_pool.get_session()

    session.set_storage_group(STORAGE_GROUP_NAME)
    session.create_time_series(
        "root.test.d0.s0", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY
    )
    session.create_time_series(
        "root.test.d0.s1", TSDataType.INT32, TSEncoding.PLAIN, Compressor.SNAPPY
    )
    session.create_time_series(
        "root.test.d0.s2", TSDataType.INT64, TSEncoding.PLAIN, Compressor.SNAPPY
    )

    # create multi time series
    # setting multiple time series once.
    ts_path_lst = [
        "root.test.d1.s0",
        "root.test.d1.s1",
        "root.test.d1.s2",
        "root.test.d1.s3",
    ]
    data_type_lst = [
        TSDataType.BOOLEAN,
        TSDataType.INT32,
        TSDataType.INT64,
        TSDataType.FLOAT,
    ]
    encoding_lst = [TSEncoding.PLAIN for _ in range(len(data_type_lst))]
    compressor_lst = [Compressor.SNAPPY for _ in range(len(data_type_lst))]
    session.create_multi_time_series(
        ts_path_lst, data_type_lst, encoding_lst, compressor_lst
    )

    # delete time series root.test.d1.s3
    session.delete_time_series(["root.test.d1.s3"])

    print("now the timeseries are:")
    # show result
    res = session.execute_query_statement("show timeseries root.test.**")
    while res.has_next():
        print(res.next())

    session_pool.put_back(session)


def insert_data(num: int):
    print("insert data for root.test.d" + str(num))
    device = ["root.test.d" + str(num)]
    measurements = ["s0", "s1", "s2"]
    values = [False, 10, 11]
    data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64]
    # Get a session from the pool
    session = session_pool.get_session()
    session.insert_records(device, [1], [measurements], [data_types], [values])

    session_pool.put_back(session)


def query_data():
    # Get a session from the pool
    session = session_pool.get_session()

    print("get data from root.test.d0")
    res = session.execute_query_statement("select * from root.test.d0")
    while res.has_next():
        print(res.next())

    print("get data from root.test.d1")
    res = session.execute_query_statement("select * from root.test.d1")
    while res.has_next():
        print(res.next())

    session_pool.put_back(session)


def delete_data():
    session = session_pool.get_session()
    session.delete_storage_group(STORAGE_GROUP_NAME)
    print("data has been deleted. now the devices are:")
    res = session.execute_statement("show devices root.test.**")
    while res.has_next():
        print(res.next())
    session_pool.put_back(session)


ip = "127.0.0.1"
port = "6667"
username = "root"
password = "root"
pool_config = PoolConfig(
    node_urls=["127.0.0.1:6667", "127.0.0.1:6668", "127.0.0.1:6669"],
    user_name=username,
    password=password,
    fetch_size=1024,
    time_zone="UTC+8",
    max_retry=3,
)
max_pool_size = 5
wait_timeout_in_ms = 3000

# Create a session pool
session_pool = SessionPool(pool_config, max_pool_size, wait_timeout_in_ms)

prepare_data()

insert_thread1 = threading.Thread(target=insert_data, args=(0,))
insert_thread2 = threading.Thread(target=insert_data, args=(1,))

insert_thread1.start()
insert_thread2.start()

insert_thread1.join()
insert_thread2.join()

query_data()
delete_data()
session_pool.close()
print("example is finished!")
