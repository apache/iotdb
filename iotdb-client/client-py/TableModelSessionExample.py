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

from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.NumpyTablet import NumpyTablet
from iotdb.utils.Tablet import ColumnType, Tablet

# creating session connection.
ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"

# don't specify database in constructor
session = Session(ip, port_, username_, password_, sql_dialect="table", database="db1")
session.open(False)

session.execute_non_query_statement("CREATE DATABASE test1")
session.execute_non_query_statement("CREATE DATABASE test2")
session.execute_non_query_statement("use test2")

# or use full qualified table name
session.execute_non_query_statement(
    "create table test1.table1("
    "region_id STRING ID, plant_id STRING ID, device_id STRING ID, "
    "model STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT) with (TTL=3600000)"
)
session.execute_non_query_statement(
    "create table table2("
    "region_id STRING ID, plant_id STRING ID, color STRING ATTRIBUTE, temperature FLOAT MEASUREMENT,"
    " speed DOUBLE MEASUREMENT) with (TTL=6600000)"
)

# show tables from current database
with session.execute_query_statement("SHOW TABLES") as session_data_set:
    print(session_data_set.get_column_names())
    while session_data_set.has_next():
        print(session_data_set.next())

# show tables by specifying another database
# using SHOW tables FROM
with session.execute_query_statement("SHOW TABLES FROM test1") as session_data_set:
    print(session_data_set.get_column_names())
    while session_data_set.has_next():
        print(session_data_set.next())

session.close()

# specify database in constructor
session = Session(
    ip, port_, username_, password_, sql_dialect="table", database="test1"
)
session.open(False)

# show tables from current database
with session.execute_query_statement("SHOW TABLES") as session_data_set:
    print(session_data_set.get_column_names())
    while session_data_set.has_next():
        print(session_data_set.next())

# change database to test2
session.execute_non_query_statement("use test2")

# show tables by specifying another database
# using SHOW tables FROM
with session.execute_query_statement("SHOW TABLES") as session_data_set:
    print(session_data_set.get_column_names())
    while session_data_set.has_next():
        print(session_data_set.next())

session.close()

# insert tablet by insert_relational_tablet
session = Session(ip, port_, username_, password_, sql_dialect="table")
session.open(False)
session.execute_non_query_statement("CREATE DATABASE IF NOT EXISTS db1")
session.execute_non_query_statement('USE "db1"')
session.execute_non_query_statement(
    "CREATE TABLE table5 (id1 string id, attr1 string attribute, "
    + "m1 double "
    + "measurement)"
)

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
tablet = Tablet("table5", column_names, data_types, values, timestamps, column_types)
session.insert_relational_tablet(tablet)

session.execute_non_query_statement("FLush")

np_timestamps = np.arange(15, 30, dtype=np.dtype(">i8"))
np_values = [
    np.array(["id:{}".format(i) for i in range(15, 30)]),
    np.array(["attr:{}".format(i) for i in range(15, 30)]),
    np.linspace(15.0, 29.0, num=15, dtype=TSDataType.DOUBLE.np_dtype()),
]

np_tablet = NumpyTablet(
    "table5",
    column_names,
    data_types,
    np_values,
    np_timestamps,
    column_types=column_types,
)
session.insert_relational_tablet(np_tablet)

with session.execute_query_statement("select * from table5 order by time") as dataset:
    print(dataset.get_column_names())
    while dataset.has_next():
        row_record = dataset.next()
        # print(row_record.get_fields()[0].get_long_value())
        # print(row_record.get_fields()[1].get_string_value())
        # print(row_record.get_fields()[2].get_string_value())
        # print(row_record.get_fields()[3].get_double_value())
        print(row_record)

with session.execute_query_statement("select * from table5 order by time") as dataset:
    df = dataset.todf()
    print(df)

# close session connection.
session.close()
