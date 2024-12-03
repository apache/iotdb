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


def test_connection():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        conn = connect(db.get_container_host_ip(), db.get_exposed_port(6667))
        if conn.is_close:
            print("can't create connect")
            exit(1)
        conn.close()
        if not conn.is_close:
            test_fail()
            print_message("failed to close the connection!")


if final_flag:
    print("All executions done!!")
else:
    print("Some test failed, please have a check")
    print("failed count: ", failed_count)
    exit(1)
