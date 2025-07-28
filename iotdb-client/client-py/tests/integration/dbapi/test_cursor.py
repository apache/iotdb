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

from tests.integration.iotdb_container import IoTDBContainer
from iotdb.dbapi import connect
from iotdb.dbapi.Cursor import Cursor

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


def test_cursor():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        conn = connect(db.get_container_host_ip(), db.get_exposed_port(6667))
        cursor: Cursor = conn.cursor()

        # execute test
        cursor.execute("create database root.cursor")
        cursor.execute("create database root.cursor_s1")
        cursor.execute("delete database root.cursor_s1")
        if cursor.rowcount < 0:
            test_fail()
            print_message("execute test failed!")

        # execute with args test
        cursor.execute(
            "create timeseries root.cursor.temperature with datatype=FLOAT,encoding=RLE"
        )
        cursor.execute(
            "insert into root.cursor(timestamp,temperature) values(1,%(temperature)s)",
            {"temperature": 0.3},
        )
        cursor.execute(
            "insert into root.cursor(timestamp,temperature) values(2,%(temperature)s)",
            {"temperature": 0.4},
        )
        cursor.execute("select * from root.cursor")
        count = 2
        actual_count = 0
        for row in cursor.fetchall():
            actual_count += 1
        if count != actual_count:
            test_fail()
            print_message("execute with args test failed!")

        # executemany with args test
        args = [
            {"timestamp": 3, "temperature": 3},
            {"timestamp": 4, "temperature": 4},
            {"timestamp": 5, "temperature": 5},
            {"timestamp": 6, "temperature": 6},
            {"timestamp": 7, "temperature": 7},
        ]
        cursor.executemany(
            "insert into root.cursor(timestamp,temperature) values(%(timestamp)s,%(temperature)s)",
            args,
        )
        cursor.execute("select * from root.cursor")
        count = 7
        actual_count = 0
        for row in cursor.fetchall():
            actual_count += 1
        if count != actual_count:
            test_fail()
            print_message("executemany with args test failed!")

        # fetchmany test
        cursor.execute("select * from root.cursor")
        count = 2
        actual_count = 0
        for row in cursor.fetchmany(count):
            actual_count += 1
        if count != actual_count:
            test_fail()
            print_message("fetchmany test failed!")

        # fetchone test
        cursor.execute("select * from root.cursor")
        row = cursor.fetchone()
        if row[0] != 1:
            test_fail()
            print_message("fetchone test failed")

        cursor.execute("delete database root.cursor")
        cursor.close()
        conn.close()


if final_flag:
    print("All executions done!!")
else:
    print("Some test failed, please have a check")
    print("failed count: ", failed_count)
    exit(1)
