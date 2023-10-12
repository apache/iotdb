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
import pandas as pd
from pandas.testing import assert_frame_equal

from iotdb.IoTDBContainer import IoTDBContainer
from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.Tablet import Tablet


def test_tablet_insertion():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open(False)
        session.execute_non_query_statement("CREATE DATABASE root.sg_test_01")

        measurements_ = ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"]
        data_types_ = [
            TSDataType.BOOLEAN,
            TSDataType.INT32,
            TSDataType.INT64,
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
            TSDataType.TEXT,
        ]
        values_ = [
            [False, 10, 11, 1.1, 10011.1, "test01"],
            [True, 100, 11111, 1.25, 101.0, "test02"],
            [False, 100, 1, 188.1, 688.25, "test03"],
            [True, 0, 0, 0, 6.25, "test04"],
        ]
        timestamps_ = [16, 17, 18, 19]
        tablet_ = Tablet(
            "root.sg_test_01.d_01", measurements_, data_types_, values_, timestamps_
        )
        session.insert_tablet(tablet_)
        columns = []
        for measurement in measurements_:
            columns.append("root.sg_test_01.d_01." + measurement)
        df_input = pd.DataFrame(values_, columns=columns, dtype=object)
        df_input.insert(0, "Time", np.array(timestamps_))

        session_data_set = session.execute_query_statement(
            "select s_01, s_02, s_03, s_04, s_05, s_06 from root.sg_test_01.d_01"
        )
        df_output = session_data_set.todf()
        df_output = df_output[df_input.columns.tolist()].replace(
            {pd.NA: None, np.nan: None}
        )

        session.close()
    assert_frame_equal(df_input, df_output, False)


def test_nullable_tablet_insertion():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open(False)
        session.execute_non_query_statement("CREATE DATABASE root.sg_test_01")

        measurements_ = ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"]
        data_types_ = [
            TSDataType.BOOLEAN,
            TSDataType.INT32,
            TSDataType.INT64,
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
            TSDataType.TEXT,
        ]
        values_ = [
            [None, None, 11, 1.1, 10011.1, "test01"],
            [True, None, 11111, 1.25, 101.0, "test02"],
            [False, 100, 1, None, 688.25, "test03"],
            [True, None, 0, 0, 6.25, None],
        ]
        timestamps_ = [16, 17, 18, 19]
        tablet_ = Tablet(
            "root.sg_test_01.d_01", measurements_, data_types_, values_, timestamps_
        )
        session.insert_tablet(tablet_)
        columns = []
        for measurement in measurements_:
            columns.append("root.sg_test_01.d_01." + measurement)
        df_input = pd.DataFrame(values_, columns=columns, dtype=object)
        df_input.insert(0, "Time", np.array(timestamps_))

        session_data_set = session.execute_query_statement(
            "select s_01, s_02, s_03, s_04, s_05, s_06 from root.sg_test_01.d_01"
        )
        df_output = session_data_set.todf()
        df_output = df_output[df_input.columns.tolist()]
        df_output = df_output[df_input.columns.tolist()].replace(
            {pd.NA: None, np.nan: None}
        )
        session.close()
    assert_frame_equal(df_input, df_output, False)
