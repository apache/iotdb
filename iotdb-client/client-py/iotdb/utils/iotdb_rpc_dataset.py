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

# for package
import logging

import numpy as np
import pandas as pd
from thrift.transport import TTransport

from iotdb.thrift.rpc.IClientRPCService import TSFetchResultsReq, TSCloseOperationReq
from iotdb.tsfile.utils.date_utils import parse_int_to_date
from iotdb.tsfile.utils.tsblock_serde import deserialize
from iotdb.utils.exception import IoTDBConnectionException
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.rpc_utils import verify_success

logger = logging.getLogger("IoTDB")
TIMESTAMP_STR = "Time"


class IoTDBRpcDataSet(object):

    def __init__(
        self,
        sql,
        column_name_list,
        column_type_list,
        column_name_index,
        ignore_timestamp,
        more_data,
        query_id,
        client,
        statement_id,
        session_id,
        query_result,
        fetch_size,
        time_out,
        column_index_2_tsblock_column_index_list,
    ):
        self.__statement_id = statement_id
        self.__session_id = session_id
        self.ignore_timestamp = ignore_timestamp
        self.__sql = sql
        self.__query_id = query_id
        self.__client = client
        self.__fetch_size = fetch_size
        self.column_size = len(column_name_list)
        self.__time_out = time_out
        self.__more_data = more_data

        self.__column_name_list = []
        self.__column_type_list = []
        self.column_ordinal_dict = {}
        self.column_name_2_tsblock_column_index_dict = {}
        column_start_index = 1

        start_index_for_column_index_2_tsblock_column_index_list = 0
        if not ignore_timestamp:
            self.__column_name_list.append(TIMESTAMP_STR)
            self.__column_type_list.append(TSDataType.INT64)
            self.column_name_2_tsblock_column_index_dict[TIMESTAMP_STR] = -1
            self.column_ordinal_dict[TIMESTAMP_STR] = 1
            if column_index_2_tsblock_column_index_list is not None:
                column_index_2_tsblock_column_index_list.insert(0, -1)
                start_index_for_column_index_2_tsblock_column_index_list = 1
            column_start_index += 1

        if column_index_2_tsblock_column_index_list is None:
            column_index_2_tsblock_column_index_list = []
            if not ignore_timestamp:
                start_index_for_column_index_2_tsblock_column_index_list = 1
                column_index_2_tsblock_column_index_list.append(-1)
            for i in range(len(column_name_list)):
                column_index_2_tsblock_column_index_list.append(i)
        ts_block_column_size = (
            max(column_index_2_tsblock_column_index_list, default=0) + 1
        )
        self.__data_type_for_tsblock_column = [None] * ts_block_column_size
        for i in range(len(column_name_list)):
            name = column_name_list[i]
            column_type = TSDataType[column_type_list[i]]
            self.__column_name_list.append(name)
            self.__column_type_list.append(column_type)
            tsblock_column_index = column_index_2_tsblock_column_index_list[
                start_index_for_column_index_2_tsblock_column_index_list + i
            ]
            if tsblock_column_index != -1:
                self.__data_type_for_tsblock_column[tsblock_column_index] = column_type
            if name not in self.column_name_2_tsblock_column_index_dict:
                self.column_ordinal_dict[name] = i + column_start_index
                self.column_name_2_tsblock_column_index_dict[name] = (
                    tsblock_column_index
                )

        self.__column_index_2_tsblock_column_index_list = (
            column_index_2_tsblock_column_index_list
        )
        self.__query_result = query_result
        self.__query_result_index = 0
        self.__is_closed = False
        self.__empty_resultSet = False
        self.has_cached_data_frame = False
        self.data_frame = None

    def close(self):
        if self.__is_closed:
            return
        if self.__client is not None:
            try:
                status = self.__client.closeOperation(
                    TSCloseOperationReq(
                        self.__session_id, self.__query_id, self.__statement_id
                    )
                )
                logger.debug(
                    "close session {}, message: {}".format(
                        self.__session_id, status.message
                    )
                )
            except TTransport.TException as e:
                raise IoTDBConnectionException(
                    "close session {} failed because: ".format(self.__session_id), e
                )

            self.__is_closed = True
            self.__client = None

    def next(self):
        if not self.has_cached_data_frame:
            self.construct_one_data_frame()
        if self.has_cached_data_frame:
            return True
        if self.__empty_resultSet:
            return False
        if self.__more_data and self.fetch_results():
            self.construct_one_data_frame()
            return True
        return False

    def construct_one_data_frame(self):
        if self.has_cached_data_frame or self.__query_result is None:
            return True
        result = {}
        has_pd_series = []
        for i in range(len(self.__column_index_2_tsblock_column_index_list)):
            result[i] = []
            has_pd_series.append(False)
        total_length = 0
        while self.__query_result_index < len(self.__query_result):
            time_column_values, column_values, null_indicators, current_length = (
                deserialize(memoryview(self.__query_result[self.__query_result_index]))
            )
            self.__query_result[self.__query_result_index] = None
            self.__query_result_index += 1
            time_array = time_column_values
            if time_array.dtype.byteorder == ">":
                time_array = time_array.byteswap().view(
                    time_array.dtype.newbyteorder("<")
                )
            if self.ignore_timestamp is None or self.ignore_timestamp is False:
                result[0].append(time_array)
            total_length += current_length
            for i, location in enumerate(
                self.__column_index_2_tsblock_column_index_list
            ):
                if location < 0:
                    continue
                data_type = self.__data_type_for_tsblock_column[location]
                value_buffer = column_values[location]
                value_buffer_len = len(value_buffer)
                # DOUBLE
                if data_type == 4:
                    data_array = value_buffer
                # FLOAT
                elif data_type == 3:
                    data_array = value_buffer
                # BOOLEAN
                elif data_type == 0:
                    data_array = np.array(value_buffer).astype("bool")
                # INT32, DATE
                elif data_type == 1 or data_type == 9:
                    data_array = value_buffer
                # INT64, TIMESTAMP
                elif data_type == 2 or data_type == 8:
                    data_array = value_buffer
                # TEXT, STRING, BLOB
                elif data_type == 5 or data_type == 11 or data_type == 10:
                    index = 0
                    data_array = []
                    while index < value_buffer_len:
                        data_array.append(value_buffer[index].tobytes())
                        index += 1
                    data_array = np.array(data_array, dtype=object)
                else:
                    raise RuntimeError("unsupported data type {}.".format(data_type))
                if data_array.dtype.byteorder == ">":
                    data_array = data_array.byteswap().view(
                        data_array.dtype.newbyteorder("<")
                    )

                null_indicator = null_indicators[location]

                if len(data_array) < current_length or (
                    data_type == 0 and null_indicator is not None
                ):
                    tmp_array = np.full(current_length, None, dtype=object)
                    if null_indicator is not None:
                        indexes = [not v for v in null_indicator]
                        if data_type == 0:
                            tmp_array[indexes] = data_array[indexes]
                        else:
                            tmp_array[indexes] = data_array

                    # INT32, DATE
                    if data_type == 1 or data_type == 9:
                        tmp_array = pd.Series(tmp_array, dtype="Int32")
                        has_pd_series[i] = True
                    # INT64, TIMESTAMP
                    elif data_type == 2 or data_type == 8:
                        tmp_array = pd.Series(tmp_array, dtype="Int64")
                        has_pd_series[i] = True
                    # BOOLEAN
                    elif data_type == 0:
                        tmp_array = pd.Series(tmp_array, dtype="boolean")
                        has_pd_series[i] = True
                    # FLOAT, DOUBLE
                    elif data_type == 3 or data_type == 4:
                        tmp_array = pd.Series(tmp_array)
                        has_pd_series[i] = True
                    data_array = tmp_array

                result[i].append(data_array)
        for k, v in result.items():
            if v is None or len(v) < 1 or v[0] is None:
                result[k] = []
            elif not has_pd_series[k]:
                res = np.empty(total_length, dtype=v[0].dtype)
                np.concatenate(v, axis=0, out=res)
                result[k] = res
            else:
                v = [x if isinstance(x, pd.Series) else pd.Series(x) for x in v]
                result[k] = pd.concat(v, ignore_index=True)

        self.__query_result = None
        self.data_frame = pd.DataFrame(result, dtype=object)
        if not self.data_frame.empty:
            self.has_cached_data_frame = True

    def has_cached_result(self):
        return self.has_cached_data_frame

    def _has_next_result_set(self):
        if (self.__query_result is not None) and (
            len(self.__query_result) > self.__query_result_index
        ):
            return True
        if self.__empty_resultSet:
            return False
        if self.__more_data and self.fetch_results():
            return True
        return False

    def result_set_to_pandas(self):
        result = {}
        for i in range(len(self.__column_index_2_tsblock_column_index_list)):
            result[i] = []
        while self._has_next_result_set():
            time_column_values, column_values, null_indicators, _ = deserialize(
                self.__query_result[self.__query_result_index]
            )
            self.__query_result[self.__query_result_index] = None
            self.__query_result_index += 1
            time_array = np.frombuffer(
                time_column_values, np.dtype(np.longlong).newbyteorder(">")
            )
            if time_array.dtype.byteorder == ">":
                time_array = time_array.byteswap().view(
                    time_array.dtype.newbyteorder("<")
                )
            if self.ignore_timestamp is None or self.ignore_timestamp is False:
                result[0].append(time_array)

            total_length = len(time_array)

            for i, location in enumerate(
                self.__column_index_2_tsblock_column_index_list
            ):
                if location < 0:
                    continue
                data_type = self.__data_type_for_tsblock_column[location]
                value_buffer = column_values[location]
                value_buffer_len = len(value_buffer)
                # DOUBLE
                if data_type == 4:
                    data_array = np.frombuffer(
                        value_buffer, np.dtype(np.double).newbyteorder(">")
                    )
                # FLOAT
                elif data_type == 3:
                    data_array = np.frombuffer(
                        value_buffer, np.dtype(np.float32).newbyteorder(">")
                    )
                # BOOLEAN
                elif data_type == 0:
                    data_array = np.array(value_buffer).astype("bool")
                # INT32
                elif data_type == 1:
                    data_array = np.frombuffer(
                        value_buffer, np.dtype(np.int32).newbyteorder(">")
                    )
                # INT64, TIMESTAMP
                elif data_type == 2 or data_type == 8:
                    data_array = np.frombuffer(
                        value_buffer, np.dtype(np.int64).newbyteorder(">")
                    )
                # TEXT, STRING
                elif data_type == 5 or data_type == 11:
                    index = 0
                    data_array = []
                    while index < value_buffer_len:
                        value_bytes = value_buffer[index].tobytes()
                        value = value_bytes.decode("utf-8")
                        data_array.append(value)
                        index += 1
                    data_array = pd.Series(data_array).astype(str)
                # BLOB
                elif data_type == 10:
                    index = 0
                    data_array = []
                    while index < value_buffer_len:
                        data_array.append(value_buffer[index].tobytes())
                        index += 1
                    data_array = pd.Series(data_array)
                # DATE
                elif data_type == 9:
                    data_array = np.frombuffer(
                        value_buffer, np.dtype(np.int32).newbyteorder(">")
                    )
                    data_array = pd.Series(data_array).apply(parse_int_to_date)
                else:
                    raise RuntimeError("unsupported data type {}.".format(data_type))
                if data_array.dtype.byteorder == ">" and len(data_array) > 0:
                    data_array = data_array.byteswap().view(
                        data_array.dtype.newbyteorder("<")
                    )
                tmp_array = []
                null_indicator = null_indicators[location]
                if len(data_array) < total_length or (
                    data_type == 0 and null_indicator is not None
                ):
                    # BOOLEAN, INT32, INT64, TIMESTAMP
                    if (
                        data_type == 0
                        or data_type == 1
                        or data_type == 2
                        or data_type == 8
                    ):
                        tmp_array = np.full(total_length, pd.NA, dtype=object)
                    # FLOAT, DOUBLE
                    elif data_type == 3 or data_type == 4:
                        tmp_array = np.full(
                            total_length, np.nan, dtype=data_array.dtype
                        )
                    # TEXT, STRING, BLOB, DATE
                    elif (
                        data_type == 5
                        or data_type == 11
                        or data_type == 10
                        or data_type == 9
                    ):
                        tmp_array = np.full(total_length, None, dtype=object)

                    if null_indicator is not None:
                        indexes = [not v for v in null_indicator]
                        if data_type == 0:
                            tmp_array[indexes] = data_array[indexes]
                        else:
                            tmp_array[indexes] = data_array

                    if data_type == 1:
                        tmp_array = pd.Series(tmp_array).astype("Int32")
                    elif data_type == 2 or data_type == 8:
                        tmp_array = pd.Series(tmp_array).astype("Int64")
                    elif data_type == 0:
                        tmp_array = pd.Series(tmp_array).astype("boolean")

                    data_array = tmp_array

                result[i].append(data_array)

        for k, v in result.items():
            if v is None or len(v) < 1 or v[0] is None:
                result[k] = []
            elif v[0].dtype == "Int32":
                v = [x if isinstance(x, pd.Series) else pd.Series(x) for x in v]
                result[k] = pd.concat(v, ignore_index=True).astype("Int32")
            elif v[0].dtype == "Int64":
                v = [x if isinstance(x, pd.Series) else pd.Series(x) for x in v]
                result[k] = pd.concat(v, ignore_index=True).astype("Int64")
            elif v[0].dtype == bool:
                result[k] = pd.Series(np.concatenate(v, axis=0)).astype("boolean")
            else:
                result[k] = np.concatenate(v, axis=0)

        df = pd.DataFrame(result)
        df.columns = self.__column_name_list
        return df

    def fetch_results(self):
        if self.__is_closed:
            raise IoTDBConnectionException("This DataSet is already closed")
        request = TSFetchResultsReq(
            self.__session_id,
            self.__sql,
            self.__fetch_size,
            self.__query_id,
            True,
            self.__time_out,
            self.__statement_id,
        )
        try:
            resp = self.__client.fetchResultsV2(request)
            verify_success(resp.status)
            self.__more_data = resp.moreData
            if not resp.hasResultSet:
                self.__empty_resultSet = True
            else:
                self.__query_result = resp.queryResult
                self.__query_result_index = 0
            return resp.hasResultSet
        except TTransport.TException as e:
            raise IoTDBConnectionException(
                "Cannot fetch result from server, because of network connection: ", e
            )

    def find_column_name_by_index(self, column_index):
        if column_index <= 0:
            raise Exception("Column index should start from 1")
        if column_index > len(self.__column_name_list):
            raise Exception(
                "column index {} out of range {}".format(column_index, self.column_size)
            )
        return self.__column_name_list[column_index - 1]

    def get_fetch_size(self):
        return self.__fetch_size

    def set_fetch_size(self, fetch_size):
        self.__fetch_size = fetch_size

    def get_column_names(self):
        return self.__column_name_list

    def get_column_types(self):
        return self.__column_type_list
