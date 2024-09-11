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

import numpy as np

from iotdb.SessionPool import PoolConfig, SessionPool
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.NumpyTablet import NumpyTablet
from iotdb.utils.Tablet import ColumnType, Tablet


def prepare_data():
    print("create database")
    # Get a session from the pool
    session = session_pool.get_session()
    session.execute_non_query_statement("CREATE DATABASE IF NOT EXISTS db1")
    session.execute_non_query_statement('USE "db1"')
    session.execute_non_query_statement(
        "CREATE TABLE table0 (id1 string id, attr1 string attribute, "
        + "m1 double "
        + "measurement)"
    )
    session.execute_non_query_statement(
        "CREATE TABLE table1 (id1 string id, attr1 string attribute, "
        + "m1 double "
        + "measurement)"
    )

    print("now the tables are:")
    # show result
    res = session.execute_query_statement("SHOW TABLES")
    while res.has_next():
        print(res.next())

    session_pool.put_back(session)


def insert_data(num: int):
    print("insert data for table" + str(num))
    # Get a session from the pool
    session = session_pool.get_session()
    column_names = [
        "id1",
        "attr1",
        "m1",
    ]
    data_types = [
        TSDataType.STRING,
        TSDataType.STRING,
        TSDataType.DOUBLE,
    ]
    column_types = [ColumnType.ID, ColumnType.ATTRIBUTE, ColumnType.MEASUREMENT]
    timestamps = []
    values = []
    for row in range(15):
        timestamps.append(row)
        values.append(["id:" + str(row), "attr:" + str(row), row * 1.0])
    tablet = Tablet(
        "table" + str(num), column_names, data_types, values, timestamps, column_types
    )
    session.insert_relational_tablet(tablet)
    session.execute_non_query_statement("FLush")

    np_timestamps = np.arange(15, 30, dtype=np.dtype(">i8"))
    np_values = [
        np.array(["id:{}".format(i) for i in range(15, 30)]),
        np.array(["attr:{}".format(i) for i in range(15, 30)]),
        np.linspace(15.0, 29.0, num=15, dtype=TSDataType.DOUBLE.np_dtype()),
    ]

    np_tablet = NumpyTablet(
        "table" + str(num),
        column_names,
        data_types,
        np_values,
        np_timestamps,
        column_types=column_types,
    )
    session.insert_relational_tablet(np_tablet)
    session_pool.put_back(session)


def query_data():
    # Get a session from the pool
    session = session_pool.get_session()

    print("get data from table0")
    res = session.execute_query_statement("select * from table0")
    while res.has_next():
        print(res.next())

    print("get data from table1")
    res = session.execute_query_statement("select * from table0")
    while res.has_next():
        print(res.next())

    session_pool.put_back(session)


def delete_data():
    session = session_pool.get_session()
    session.execute_non_query_statement("drop database db1")
    print("data has been deleted. now the databases are:")
    res = session.execute_statement("show databases")
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
    sql_dialect="table",
    database="db1",
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
