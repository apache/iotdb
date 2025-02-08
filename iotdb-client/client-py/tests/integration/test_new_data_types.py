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
from datetime import date

import numpy as np

from iotdb.Session import Session
from iotdb.SessionPool import PoolConfig, create_session_pool
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.NumpyTablet import NumpyTablet
from iotdb.utils.Tablet import Tablet
from .iotdb_container import IoTDBContainer


def test_session():
    session_test()


def test_session_pool():
    session_test(True)


def session_test(use_session_pool=False):
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer

        if use_session_pool:
            pool_config = PoolConfig(
                db.get_container_host_ip(),
                db.get_exposed_port(6667),
                "root",
                "root",
                None,
                1024,
                "Asia/Shanghai",
                3,
            )
            session_pool = create_session_pool(pool_config, 1, 3000)
            session = session_pool.get_session()
        else:
            session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open(False)

        if not session.is_open():
            print("can't open session")
            exit(1)

        device_id = "root.sg_test_01.d_04"
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
            device_id,
            measurements_new_type,
            data_types_new_type,
            values_new_type,
            timestamps_new_type,
        )
        session.insert_tablet(tablet_new_type)
        np_values_new_type = [
            np.array(
                [date(2024, 1, 5), date(2024, 1, 6), date(2024, 1, 7), date(2024, 1, 8)]
            ),
            np.array([5, 6, 7, 8], TSDataType.INT64.np_dtype()),
            np.array([b"\x12\x34", b"\x12\x34", b"\x12\x34", b"\x12\x34"]),
            np.array(["test05", "test06", "test07", "test08"]),
        ]
        np_timestamps_new_type = np.array([5, 6, 7, 8], TSDataType.INT64.np_dtype())
        np_tablet_new_type = NumpyTablet(
            device_id,
            measurements_new_type,
            data_types_new_type,
            np_values_new_type,
            np_timestamps_new_type,
        )
        session.insert_tablet(np_tablet_new_type)
        session.insert_records(
            [device_id, device_id],
            [9, 10],
            [measurements_new_type, measurements_new_type],
            [data_types_new_type, data_types_new_type],
            [
                [date(2024, 1, 9), 9, b"\x12\x34", "test09"],
                [date(2024, 1, 10), 10, b"\x12\x34", "test010"],
            ],
        )

        with session.execute_query_statement(
            "select s_01,s_02,s_03,s_04 from root.sg_test_01.d_04"
        ) as dataset:
            print(dataset.get_column_names())
            while dataset.has_next():
                print(dataset.next())

        with session.execute_query_statement(
            "select s_01,s_02,s_03,s_04 from root.sg_test_01.d_04"
        ) as dataset:
            df = dataset.todf()
            print(df.to_string())

        with session.execute_query_statement(
            "select s_01,s_02,s_03,s_04 from root.sg_test_01.d_04"
        ) as dataset:
            cnt = 0
            while dataset.has_next():
                row_record = dataset.next()
                timestamp = row_record.get_timestamp()
                assert row_record.get_fields()[0].get_date_value() == date(
                    2024, 1, timestamp
                )
                assert (
                    row_record.get_fields()[1].get_object_value(TSDataType.TIMESTAMP)
                    == timestamp
                )
                assert row_record.get_fields()[2].get_binary_value() == b"\x12\x34"
                assert row_record.get_fields()[3].get_string_value() == "test0" + str(
                    timestamp
                )
                cnt += 1
            assert cnt == 10

        with session.execute_query_statement(
            "select s_01,s_02,s_03,s_04 from root.sg_test_01.d_04"
        ) as dataset:
            df = dataset.todf()
            rows, columns = df.shape
            assert rows == 10
            assert columns == 5

        session.insert_records(
            [device_id, device_id],
            [11, 12],
            [measurements_new_type, ["s_02", "s_03", "s_04"]],
            [
                data_types_new_type,
                [
                    TSDataType.TIMESTAMP,
                    TSDataType.BLOB,
                    TSDataType.STRING,
                ],
            ],
            [
                [date(1971, 1, 1), 11, b"\x12\x34", "test11"],
                [12, b"\x12\x34", "test12"],
            ],
        )

        with session.execute_query_statement(
            "select s_01,s_02,s_03,s_04 from root.sg_test_01.d_04 where time > 10"
        ) as dataset:
            cnt = 0
            while dataset.has_next():
                cnt += 1
                print(dataset.next())
            assert cnt == 2

        # close session connection.
        session.close()
