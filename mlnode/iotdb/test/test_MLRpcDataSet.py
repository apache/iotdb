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
import os
import sys

sys.path.append("/client-py/iotdb")
import random
import time

import numpy as np
import pandas as pd
from iotdb.IoTDBContainer import IoTDBContainer
from iotdb.Session import Session
from iotdb.mlnode.IoTDBMLDataSet import IoTDBMLDataSet
from iotdb.thrift.rpc.IClientRPCService import Client
from iotdb.thrift.rpc.ttypes import TSOpenSessionReq, TSProtocolVersion, TSCreateMultiTimeseriesReq, TSInsertTabletReq, \
    TSExecuteStatementReq, TSCloseSessionReq, TSInsertRecordReq
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.Tablet import Tablet
from pandas.testing import assert_frame_equal
from thrift.protocol import TCompactProtocol, TBinaryProtocol
from thrift.transport import TTransport, TSocket

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


def create_ts(client, session_id, statement_id):
    # setting time series.
    encoding_lst = [TSEncoding.PLAIN for _ in range(len(data_type_lst))]
    compressor_lst = [Compressor.SNAPPY for _ in range(len(data_type_lst))]
    request = TSCreateMultiTimeseriesReq(
        session_id,
        ts_path_lst,
        data_type_lst,
        encoding_lst,
        compressor_lst,
        None,
        None,
        None,
        None
    )
    status = client.createMultiTimeseries(request)
    return verify_success(status)


def verify_success(status):
    if status.code == 200:
        return 0
    raise Exception("Something happened in client ", status.code)


def close(client, session_id, transport):
    req = TSCloseSessionReq(session_id)
    try:
        client.closeSession(req)
    finally:
        if transport is not None:
            transport.close()


def get_client(transport):
    if not transport.isOpen():
        try:
            transport.open()
        except TTransport.TTransportException as e:
            raise Exception("TTransportException!")

    enable_rpc_compression = False
    if enable_rpc_compression:
        client = Client(TCompactProtocol.TCompactProtocol(transport))
    else:
        client = Client(TBinaryProtocol.TBinaryProtocol(transport))

    open_req = TSOpenSessionReq(
        client_protocol=TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3,
        username="root",
        password="root",
        zoneId=time.strftime("%z"),
        configuration={"version": "V_0_13"},
    )

    try:
        open_resp = client.openSession(open_req)

        if TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3 != open_resp.serverProtocolVersion:
            if open_resp.serverProtocolVersion == 0:
                raise TTransport.TException(message="Protocol not supported.")

        session_id = open_resp.sessionId
        statement_id = client.requestStatementId(session_id)
    except TTransport.TException as e:
        raise RuntimeError("execution of non-query statement fails because: ", e)

    return client, session_id, statement_id


def insert_tablet(client, tablet, session_id, is_aligned=False):
    data_type_values = [data_type.value for data_type in tablet.get_data_types()]
    req = TSInsertTabletReq(
        session_id,
        tablet.get_device_id(),
        tablet.get_measurements(),
        tablet.get_binary_values(),
        tablet.get_binary_timestamps(),
        data_type_values,
        tablet.get_row_number(),
        is_aligned,
    )
    status = client.insertTablet(req)
    verify_success(status)


def insert_record(client, session_id, device_id, timestamp, measurements, data_types, values, is_aligned=False):
    data_types = [data_type.value for data_type in data_types]

    if (len(values) != len(data_types)) or (len(values) != len(measurements)):
        raise RuntimeError(
            "length of data types does not equal to length of values!"
        )
    values_in_bytes = Session.value_to_bytes(data_types, values)
    req = TSInsertRecordReq(
        session_id,
        device_id,
        measurements,
        values_in_bytes,
        timestamp,
        is_aligned,
    )

    status = client.insertRecord(req)
    verify_success(status)


def execute_sql(client, sql, session_id, statement_id, fetch_size=1024, time_out=0):
    req = TSExecuteStatementReq(
        session_id, sql, statement_id, fetch_size, time_out
    )
    resp = client.executeQueryStatementV2(req)

    return IoTDBMLDataSet(
        sql,
        resp.columns,
        resp.dataTypeList,
        resp.columnNameIndexMap,
        resp.queryId,
        client,
        statement_id,
        session_id,
        resp.queryResult,
        fetch_size,
        resp.ignoreTimeStamp
    )


def test_simple_query():
    with IoTDBContainer("iotdb:dev") as db:
        transport = TTransport.TFramedTransport(
            TSocket.TSocket(db.get_container_host_ip(), db.get_exposed_port(6667))
        )

        client, session_id, statement_id = get_client(transport)

        # insert data
        data_nums = 100
        data = {}
        timestamps = np.arange(data_nums, dtype="int64")
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

        insert_tablet(client, tablet, session_id, statement_id)

        df_input.insert(0, "Time", timestamps)

        mlDataSet = execute_sql(client, "SELECT ** FROM root", session_id, statement_id)

        df_output = mlDataSet.fetch_timeseries()
        df_output = df_output[df_input.columns.tolist()]

        close(client, session_id, transport)

    assert_frame_equal(df_input, df_output)


def test_with_null_query():
    with IoTDBContainer("iotdb:dev") as db:
        transport = TTransport.TFramedTransport(
            TSocket.TSocket(db.get_container_host_ip(), db.get_exposed_port(6667))
        )
        client, session_id, statement_id = get_client(transport)

        # insert data

        data_nums = 100
        data = {}
        timestamps = np.arange(data_nums, dtype="int64")
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
                    insert_record(
                        client,
                        session_id,
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
                insert_record(
                    client,
                    session_id,
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

        mlDataSet = execute_sql(client, "SELECT ** FROM root", session_id, statement_id)
        df_output = mlDataSet.fetch_timeseries()
        df_output = df_output[df_input.columns.tolist()]
        close(client, session_id, transport)
    assert_frame_equal(df_input, df_output)


def test_multi_fetch():
    with IoTDBContainer("iotdb:dev") as db:
        transport = TTransport.TFramedTransport(
            TSocket.TSocket(db.get_container_host_ip(), db.get_exposed_port(6667))
        )
        client, session_id, statement_id = get_client(transport)

        # insert data
        data_nums = 990
        data = {}
        timestamps = np.arange(data_nums, dtype="int64")
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
        insert_tablet(client, tablet, session_id)

        df_input.insert(0, "Time", timestamps)

        mlDataSet = execute_sql(client, "SELECT ** FROM root", session_id, statement_id)
        mlDataSet.set_fetch_size(100)
        df_output = mlDataSet.fetch_timeseries()
        df_output = df_output[df_input.columns.tolist()]

        close(client, session_id, transport)
    assert_frame_equal(df_input, df_output)
