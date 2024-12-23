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

from iotdb.table_session import TableSession, TableSessionConfig
from iotdb.table_session_pool import TableSessionPool, TableSessionPoolConfig
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.NumpyTablet import NumpyTablet
from iotdb.utils.Tablet import Tablet, ColumnType
from .iotdb_container import IoTDBContainer


def test_session():
    session_test()


def test_session_pool():
    session_test(True)


def session_test(use_session_pool=False):
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        if use_session_pool:
            config = TableSessionPoolConfig(
                node_urls=[f"{db.get_container_host_ip()}:{db.get_exposed_port(6667)}"]
            )
            session_pool = TableSessionPool(config)
            session = session_pool.get_session()
        else:
            config = TableSessionConfig(
                node_urls=[f"{db.get_container_host_ip()}:{db.get_exposed_port(6667)}"]
            )
            session = TableSession(config)

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
        tablet = Tablet(
            "table5", column_names, data_types, values, timestamps, column_types
        )
        session.insert(tablet)

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
        session.insert(np_tablet)

        with session.execute_query_statement(
            "select * from table5 order by time"
        ) as dataset:
            cnt = 0
            while dataset.has_next():
                row_record = dataset.next()
                timestamp = row_record.get_fields()[0].get_long_value()
                assert (
                    "id:" + str(timestamp)
                    == row_record.get_fields()[1].get_string_value()
                )
                assert (
                    "attr:" + str(timestamp)
                    == row_record.get_fields()[2].get_string_value()
                )
                assert timestamp * 1.0 == row_record.get_fields()[3].get_double_value()
                cnt += 1
            assert 30 == cnt

        with session.execute_query_statement(
            "select * from table5 order by time"
        ) as dataset:
            df = dataset.todf()
            rows, columns = df.shape
            assert rows == 30
            assert columns == 4

        # close session connection.
        session.close()
