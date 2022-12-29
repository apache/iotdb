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
from iotdb.IoTDBContainer import IoTDBContainer

# whether the test has passed
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


def test_delete_date():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open()
        session.execute_non_query_statement("CREATE DATABASE root.str_test_01")

        if not session.is_open():
            print("can't open session")
            exit(1)

        # insert string records of one device
        time_list = [1, 2, 3]
        measurements_list = [
            ["s_01", "s_02", "s_03"],
            ["s_01", "s_02", "s_03"],
            ["s_01", "s_02", "s_03"],
        ]
        values_list = [
            ["False", "22", "33"],
            ["True", "1", "23"],
            ["False", "15", "26"],
        ]

        if (
            session.insert_string_records_of_one_device(
                "root.str_test_01.d_01",
                time_list,
                measurements_list,
                values_list,
            )
            < 0
        ):
            test_fail()
            print_message("insert string records of one device failed")

        # insert aligned string record into the database.
        time_list = [1, 2, 3]
        measurements_list = [
            ["s_01", "s_02", "s_03"],
            ["s_01", "s_02", "s_03"],
            ["s_01", "s_02", "s_03"],
        ]
        values_list = [
            ["False", "22", "33"],
            ["True", "1", "23"],
            ["False", "15", "26"],
        ]

        if (
            session.insert_aligned_string_records_of_one_device(
                "root.str_test_01.d_02",
                time_list,
                measurements_list,
                values_list,
            )
            < 0
        ):
            test_fail()
            print_message("insert aligned record of one device failed")

        # execute delete data
        session.delete_data(["root.str_test_01.d_02.s_01", "root.str_test_01.d_02.s_02"], 1)

        # execute raw data query sql statement
        session_data_set = session.execute_raw_data_query(
            ["root.str_test_01.d_02.s_01", "root.str_test_01.d_02.s_02"], 1, 4
        )
        session_data_set.set_fetch_size(1024)
        expect_count = 2
        actual_count = 0
        while session_data_set.has_next():
            print(session_data_set.next())
            actual_count += 1
        session_data_set.close_operation_handle()

        if actual_count != expect_count:
            test_fail()
            print_message(
                "query count mismatch: expect count: "
                + str(expect_count)
                + " actual count: "
                + str(actual_count)
            )
        assert actual_count == expect_count

        # execute delete data
        session.delete_data_in_range(["root.str_test_01.d_02.s_01", "root.str_test_01.d_02.s_02"], 2, 3)

        # execute raw data query sql statement
        session_data_set = session.execute_raw_data_query(
            ["root.str_test_01.d_02.s_01", "root.str_test_01.d_02.s_02"], 1, 4
        )
        session_data_set.set_fetch_size(1024)
        expect_count = 0
        actual_count = 0
        while session_data_set.has_next():
            print(session_data_set.next())
            actual_count += 1
        session_data_set.close_operation_handle()

        if actual_count != expect_count:
            test_fail()
            print_message(
                "query count mismatch: expect count: "
                + str(expect_count)
                + " actual count: "
                + str(actual_count)
            )
        assert actual_count == expect_count

        # close session connection.
        session.close()


if final_flag:
    print("All executions done!!")
else:
    print("Some test failed, please have a check")
    print("failed count: ", failed_count)
    exit(1)
