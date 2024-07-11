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

import random

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from iotdb.IoTDBContainer import IoTDBContainer
from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.Tablet import Tablet

device_id = "root.wt1"

ts_path_lst = [
    "root.wt1.temperature",
    "root.wt1.windspeed",
    "root.wt1.angle",
    "root.wt1.altitude",
    "root.wt1.status",
    "root.wt1.hardware",
]
measurements = [
    "temperature",
    "windspeed",
    "angle",
    "altitude",
    "status",
    "hardware",
]
data_type_lst = [
    TSDataType.FLOAT,
    TSDataType.DOUBLE,
    TSDataType.INT32,
    TSDataType.INT64,
    TSDataType.BOOLEAN,
    TSDataType.TEXT,
]


def create_ts(session):
    # setting time series.
    encoding_lst = [TSEncoding.PLAIN for _ in range(len(data_type_lst))]
    compressor_lst = [Compressor.SNAPPY for _ in range(len(data_type_lst))]
    session.create_multi_time_series(
        ts_path_lst, data_type_lst, encoding_lst, compressor_lst
    )


def test_simple_query():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open(False)
        session.execute_non_query_statement("CREATE DATABASE root.wt1")

        create_ts(session)

        # insert data
        data_nums = 100
        data = {}
        timestamps = np.arange(data_nums)
        data[ts_path_lst[0]] = np.float32(np.random.rand(data_nums))
        data[ts_path_lst[1]] = np.random.rand(data_nums)
        data[ts_path_lst[2]] = np.random.randint(10, 100, data_nums, dtype="int32")
        data[ts_path_lst[3]] = np.random.randint(10, 100, data_nums, dtype="int64")
        data[ts_path_lst[4]] = np.random.choice([True, False], size=data_nums)
        data[ts_path_lst[5]] = np.random.choice(["text1", "text2"], size=data_nums)

        df_input = pd.DataFrame(data)

        tablet = Tablet(
            device_id, measurements, data_type_lst, df_input.values, timestamps
        )
        session.insert_tablet(tablet)

        df_input.insert(0, "Time", timestamps)

        session_data_set = session.execute_query_statement("SELECT ** FROM root.wt1")
        df_output = session_data_set.todf()
        df_output = df_output[df_input.columns.tolist()]

        session.close()
    assert_frame_equal(df_input, df_output, check_dtype=False)


def test_with_null_query():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open(False)
        session.execute_non_query_statement("CREATE DATABASE root.wt1")

        create_ts(session)

        # insert data
        data_nums = 100
        data = {}
        timestamps = np.arange(data_nums)
        data[ts_path_lst[0]] = np.float32(np.random.rand(data_nums))
        data[ts_path_lst[1]] = np.random.rand(data_nums)
        data[ts_path_lst[2]] = np.random.randint(10, 100, data_nums, dtype="int32")
        data[ts_path_lst[3]] = np.random.randint(10, 100, data_nums, dtype="int64")
        data[ts_path_lst[4]] = np.random.choice([True, False], size=data_nums).astype(
            "bool"
        )
        data[ts_path_lst[5]] = np.random.choice(
            ["text1", "text2"], size=data_nums
        ).astype(object)

        data_empty = {}
        for ts_path in ts_path_lst:
            if data[ts_path].dtype == np.int32 or data[ts_path].dtype == np.int64:
                tmp_array = np.full(data_nums, np.nan, np.float32)
                if data[ts_path].dtype == np.int32:
                    tmp_array = pd.Series(tmp_array).astype("Int32")
                else:
                    tmp_array = pd.Series(tmp_array).astype("Int64")
            elif data[ts_path].dtype == np.float32 or data[ts_path].dtype == np.double:
                tmp_array = np.full(data_nums, np.nan, data[ts_path].dtype)
            elif data[ts_path].dtype == bool:
                tmp_array = np.full(data_nums, np.nan, np.float32)
                tmp_array = pd.Series(tmp_array).astype("boolean")
            else:
                tmp_array = np.full(data_nums, None, dtype=data[ts_path].dtype)
            data_empty[ts_path] = tmp_array
        df_input = pd.DataFrame(data_empty)

        for row_index in range(data_nums):
            is_row_inserted = False
            for column_index in range(len(measurements)):
                if random.choice([True, False]):
                    session.insert_record(
                        device_id,
                        row_index,
                        [measurements[column_index]],
                        [data_type_lst[column_index]],
                        [data[ts_path_lst[column_index]].tolist()[row_index]],
                    )
                    df_input.at[row_index, ts_path_lst[column_index]] = data[
                        ts_path_lst[column_index]
                    ][row_index]
                    is_row_inserted = True
            if not is_row_inserted:
                column_index = 0
                session.insert_record(
                    device_id,
                    row_index,
                    [measurements[column_index]],
                    [data_type_lst[column_index]],
                    [data[ts_path_lst[column_index]].tolist()[row_index]],
                )
                df_input.at[row_index, ts_path_lst[column_index]] = data[
                    ts_path_lst[column_index]
                ][row_index]

        df_input.insert(0, "Time", timestamps)

        session_data_set = session.execute_query_statement("SELECT ** FROM root.wt1")
        df_output = session_data_set.todf()
        df_output = df_output[df_input.columns.tolist()]

        session.close()
    assert_frame_equal(df_input, df_output, check_dtype=False)


def test_multi_fetch():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open(False)
        session.execute_non_query_statement("CREATE DATABASE root.wt1")

        create_ts(session)

        # insert data
        data_nums = 990
        data = {}
        timestamps = np.arange(data_nums)
        data[ts_path_lst[0]] = np.float32(np.random.rand(data_nums))
        data[ts_path_lst[1]] = np.random.rand(data_nums)
        data[ts_path_lst[2]] = np.random.randint(10, 100, data_nums, dtype="int32")
        data[ts_path_lst[3]] = np.random.randint(10, 100, data_nums, dtype="int64")
        data[ts_path_lst[4]] = np.random.choice([True, False], size=data_nums)
        data[ts_path_lst[5]] = np.random.choice(["text1", "text2"], size=data_nums)

        df_input = pd.DataFrame(data)

        tablet = Tablet(
            device_id, measurements, data_type_lst, df_input.values, timestamps
        )
        session.insert_tablet(tablet)

        df_input.insert(0, "Time", timestamps)

        session_data_set = session.execute_query_statement("SELECT ** FROM root.wt1")
        session_data_set.set_fetch_size(100)
        df_output = session_data_set.todf()
        df_output = df_output[df_input.columns.tolist()]

        session.close()
    assert_frame_equal(df_input, df_output, check_dtype=False)
