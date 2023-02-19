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
from iotdb.thrift.rpc.ttypes import TSFetchResultsReq
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils import MLDataSerde
from thrift.transport import TTransport


class IoTDBMLDataSet(object):
    TIMESTAMP_STR = "Time"
    START_INDEX = 2

    def __init__(self,
                 sql,
                 column_name_list,
                 column_type_list,
                 column_name_index,
                 query_id,
                 client,
                 statement_id,
                 session_id,
                 query_result,
                 fetch_size,
                 ignore_timestamp,
                 ):
        self.__current_tsBlock = None
        self.__statement_id = statement_id
        self.__session_id = session_id
        self.__ignore_timestamp = ignore_timestamp
        self.__sql = sql
        self.__query_id = query_id
        self.__client = client
        self.__fetch_size = fetch_size
        self.__column_size = len(column_name_list)
        self.__default_time_out = 1000
        self.__empty_resultSet = False

        self.__column_name_list = []
        self.__column_type_list = []
        self.__column_ordinal_dict = {}

        self.__ignore_timestamp = ignore_timestamp
        if not ignore_timestamp:
            self.__column_name_list.append(IoTDBMLDataSet.TIMESTAMP_STR)
            self.__column_type_list.append(TSDataType.INT64)
            self.__column_ordinal_dict[IoTDBMLDataSet.TIMESTAMP_STR] = 1

        if column_name_index is not None:
            self.__column_type_deduplicated_list = [
                None for _ in range(len(column_name_index))
            ]
            for i in range(len(column_name_list)):
                name = column_name_list[i]
                self.__column_name_list.append(name)
                self.__column_type_list.append(TSDataType[column_type_list[i]])
                if name not in self.__column_ordinal_dict:
                    index = column_name_index[name]
                    self.__column_ordinal_dict[name] = (
                            index + IoTDBMLDataSet.START_INDEX
                    )
                    self.__column_type_deduplicated_list[index] = TSDataType[
                        column_type_list[i]
                    ]
        else:
            index = IoTDBMLDataSet.START_INDEX
            self.__column_type_deduplicated_list = []
            for i in range(len(column_name_list)):
                name = column_name_list[i]
                self.__column_name_list.append(name)
                self.__column_type_list.append(TSDataType[column_type_list[i]])
                if name not in self.__column_ordinal_dict:
                    self.__column_ordinal_dict[name] = index
                    index += 1
                    self.__column_type_deduplicated_list.append(
                        TSDataType[column_type_list[i]]
                    )

        self.__query_result = query_result
        self.__query_result_size = 0
        self.__query_result_Index = 0
        if query_result is not None:
            self.__query_result_size = len(query_result)

    def fetch_results(self):
        request = TSFetchResultsReq(
            self.__session_id,
            self.__sql,
            self.__fetch_size,
            self.__query_id,
            True,
            self.__default_time_out,
        )
        try:
            resp = self.__client.fetchResults(request)
            if not resp.hasResultSet:
                self.__empty_resultSet = True
            else:
                self.__query_result = resp.queryResult
            return resp.hasResultSet
        except TTransport.TException as e:
            raise RuntimeError(
                "Cannot fetch result from server, because of network connection: ", e
            )

    def has_cached_byteBuffer(self):
        return self.__query_result is not None and self.__query_result_Index < self.__query_result_size

    def has_next_result_set(self):
        if self.has_cached_byteBuffer():
            return True
        if self.__empty_resultSet:
            return False
        if self.fetch_results():
            return True
        return False

    # transfer the whole tsBlock to dataFrame
    # It will call the fetchResult() method to get more data, which only needs to be invoked once for a sql.
    def fetch_timeseries(self):
        result = {}
        for column_name in self.__column_name_list:
            result[column_name] = None
        while self.has_cached_byteBuffer():
            buffer = self.__query_result[self.__query_result_Index]
            self.__query_result_Index += 1
            time_column_values, column_values, null_indicators, position_count, buffer = MLDataSerde.deserialize(buffer)
            time_array = np.frombuffer(
                time_column_values, np.dtype(np.longlong).newbyteorder(">")
            )
            if time_array.dtype.byteorder == ">":
                time_array = time_array.byteswap().newbyteorder("<")
            if (
                    self.__ignore_timestamp is None
                    or self.__ignore_timestamp is False
            ):
                if result[self.TIMESTAMP_STR] is None:
                    result[self.TIMESTAMP_STR] = time_array
                else:
                    result[self.TIMESTAMP_STR] = np.concatenate(
                        (result[self.TIMESTAMP_STR], time_array), axis=0
                    )
            total_length = len(time_array)

            for i in range(len(column_values)):
                if self.__ignore_timestamp is True:
                    column_name = self.__column_name_list[i]
                else:
                    column_name = self.__column_name_list[i + 1]

                location = (
                        self.__column_ordinal_dict[column_name] - self.START_INDEX
                )

                if location < 0:
                    continue
                data_type = self.__column_type_deduplicated_list[location]
                value_buffer = column_values[location]
                value_buffer_len = len(value_buffer)

                if data_type == TSDataType.DOUBLE:
                    data_array = np.frombuffer(
                        value_buffer, np.dtype(np.double).newbyteorder(">")
                    )
                elif data_type == TSDataType.FLOAT:
                    data_array = np.frombuffer(
                        value_buffer, np.dtype(np.float32).newbyteorder(">")
                    )
                elif data_type == TSDataType.BOOLEAN:
                    data_array = []
                    for index in range(len(value_buffer)):
                        data_array.append(value_buffer[index])
                    data_array = np.array(data_array).astype("bool")
                elif data_type == TSDataType.INT32:
                    data_array = np.frombuffer(
                        value_buffer, np.dtype(np.int32).newbyteorder(">")
                    )
                elif data_type == TSDataType.INT64:
                    data_array = np.frombuffer(
                        value_buffer, np.dtype(np.int64).newbyteorder(">")
                    )
                elif data_type == TSDataType.TEXT:
                    index = 0
                    data_array = []
                    while index < value_buffer_len:
                        value_bytes = value_buffer[index]
                        value = value_bytes.decode("utf-8")
                        data_array.append(value)
                        index += 1
                    data_array = np.array(data_array, dtype=object)
                else:
                    raise RuntimeError("unsupported data type {}.".format(data_type))

                if data_array.dtype.byteorder == ">":
                    data_array = data_array.byteswap().newbyteorder("<")

                null_indicator = null_indicators[location]
                if len(data_array) < total_length or (data_type == TSDataType.BOOLEAN and null_indicator is not None):
                    if data_type == TSDataType.INT32 or data_type == TSDataType.INT64:
                        tmp_array = np.full(total_length, np.nan, np.float32)
                    elif data_type == TSDataType.FLOAT or data_type == TSDataType.DOUBLE:
                        tmp_array = np.full(total_length, np.nan, data_array.dtype)
                    elif data_type == TSDataType.BOOLEAN:
                        tmp_array = np.full(total_length, np.nan, np.float32)
                    elif data_type == TSDataType.TEXT:
                        tmp_array = np.full(total_length, None, dtype=data_array.dtype)
                    else:
                        raise Exception("Unsupported dataType in deserialization")


                    if null_indicator is not None:
                        indexes = [not v for v in null_indicator]
                        if data_type == TSDataType.BOOLEAN:
                            tmp_array[indexes] = data_array[indexes]
                        else:
                            tmp_array[indexes] = data_array

                    if data_type == TSDataType.INT32:
                        tmp_array = pd.Series(tmp_array).astype("Int32")
                    elif data_type == TSDataType.INT64:
                        tmp_array = pd.Series(tmp_array).astype("Int64")
                    elif data_type == TSDataType.BOOLEAN:
                        tmp_array = pd.Series(tmp_array).astype("boolean")

                    data_array = tmp_array

                if result[column_name] is None:
                    result[column_name] = data_array
                else:
                    if isinstance(result[column_name], pd.Series):
                        if isinstance(data_array, pd.Series) == False:
                            if data_type == TSDataType.INT32:
                                data_array = pd.Series(data_array).astype("Int32")
                            elif data_type == TSDataType.INT64:
                                data_array = pd.Series(data_array).astype("Int64")
                            elif data_type == TSDataType.BOOLEAN:
                                data_array = pd.Series(data_array).astype("boolean")
                            else:
                                raise RuntimeError("Series Error")
                        result[column_name] = result[column_name].append(data_array)
                    else:
                        result[column_name] = np.concatenate(
                            (result[column_name], data_array), axis=0
                        )
        for k, v in result.items():
            if v is None:
                result[k] = []
        df = pd.DataFrame(result)
        df = df.reset_index(drop=True)
        return df

    def set_fetch_size(self, fetch_size):
        self.__fetch_size = fetch_size
