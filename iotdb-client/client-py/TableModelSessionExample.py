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

from iotdb.Session import Session

# creating session connection.
ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"

# don't specify database in constructor
session = Session(ip, port_, username_, password_, sql_dialect="table")
session.open(False)
session.execute_non_query_statement("CREATE DATABASE test1")
session.execute_non_query_statement("CREATE DATABASE test2")
session.execute_non_query_statement("use test2")

# or use full qualified table name
session.execute_non_query_statement(
    "create table test1.table1(region_id STRING ID, plant_id STRING ID, device_id STRING ID, model STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT) with (TTL=3600000)"
)
session.execute_non_query_statement(
    "create table table2(region_id STRING ID, plant_id STRING ID, color STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, speed DOUBLE MEASUREMENT) with (TTL=6600000)"
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
