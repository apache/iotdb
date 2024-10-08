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
import binascii
import logging

import numpy as np
import pandas as pd
from thrift.transport import TTransport
from iotdb.thrift.rpc.IClientRPCService import TSFetchResultsReq, TSCloseOperationReq
from iotdb.tsfile.utils.DateUtils import parse_int_to_date
from iotdb.utils.IoTDBConstants import TSDataType

logger = logging.getLogger("IoTDB")


def _to_bitbuffer(b):
    return bytes("{:0{}b}".format(int(binascii.hexlify(b), 16), 8 * len(b)), "utf-8")


class IoTDBRpcDataSet(object):
    TIMESTAMP_STR = "Time"
    # VALUE_IS_NULL = "The value got by %s (column name) is NULL."
    START_INDEX = 2

    def __init__(
        self,
        sql,
        column_name_list,
        column_type_list,
        column_name_index,
        ignore_timestamp,
        query_id,
        client,
        statement_id,
        session_id,
        query_data_set,
        fetch_size,
    ):
        self.__statement_id = statement_id
        self.__session_id = session_id
        self.ignore_timestamp = ignore_timestamp
        self.__sql = sql
        self.__query_id = query_id
        self.__client = client
        self.__fetch_size = fetch_size
        self.column_size = len(column_name_list)
        self.__default_time_out = 1000

        self.__column_name_list = []
        self.__column_type_list = []
        self.column_ordinal_dict = {}
        if not ignore_timestamp:
            self.__column_name_list.append(IoTDBRpcDataSet.TIMESTAMP_STR)
            self.__column_type_list.append(TSDataType.INT64)
            self.column_ordinal_dict[IoTDBRpcDataSet.TIMESTAMP_STR] = 1

        if column_name_index is not None:
            self.column_type_deduplicated_list = [
                None for _ in range(len(column_name_index))
            ]
            for i in range(self.column_size):
                name = column_name_list[i]
                self.__column_name_list.append(name)
                self.__column_type_list.append(TSDataType[column_type_list[i]])
                if name not in self.column_ordinal_dict:
                    index = column_name_index[name]
                    self.column_ordinal_dict[name] = index + IoTDBRpcDataSet.START_INDEX
                    self.column_type_deduplicated_list[index] = TSDataType[
                        column_type_list[i]
                    ]
        else:
            index = IoTDBRpcDataSet.START_INDEX
            self.column_type_deduplicated_list = []
            for i in range(len(column_name_list)):
                name = column_name_list[i]
                self.__column_name_list.append(name)
                self.__column_type_list.append(TSDataType[column_type_list[i]])
                if name not in self.column_ordinal_dict:
                    self.column_ordinal_dict[name] = index
                    index += 1
                    self.column_type_deduplicated_list.append(
                        TSDataType[column_type_list[i]]
                    )
        self.__query_data_set = query_data_set
        self.__is_closed = False
        self.__empty_resultSet = False
        self.__rows_index = 0
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
                raise RuntimeError(
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
        if self.fetch_results():
            self.construct_one_data_frame()
            return True
        return False

    def construct_one_data_frame(self):
        if (
            self.has_cached_data_frame
            or self.__query_data_set is None
            or len(self.__query_data_set.time) == 0
        ):
            return
        result = {}
        time_array = np.frombuffer(
            self.__query_data_set.time, np.dtype(np.longlong).newbyteorder(">")
        )
        if time_array.dtype.byteorder == ">":
            time_array = time_array.byteswap().view(time_array.dtype.newbyteorder("<"))
        result[0] = time_array
        total_length = len(time_array)
        for i in range(self.column_size):
            if self.ignore_timestamp is True:
                column_name = self.__column_name_list[i]
            else:
                column_name = self.__column_name_list[i + 1]

            location = (
                self.column_ordinal_dict[column_name] - IoTDBRpcDataSet.START_INDEX
            )
            if location < 0:
                continue
            data_type = self.column_type_deduplicated_list[location]
            value_buffer = self.__query_data_set.valueList[location]
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
                data_array = np.frombuffer(value_buffer, np.dtype("?"))
            # INT32, DATE
            elif data_type == 1 or data_type == 9:
                data_array = np.frombuffer(
                    value_buffer, np.dtype(np.int32).newbyteorder(">")
                )
            # INT64, TIMESTAMP
            elif data_type == 2 or data_type == 8:
                data_array = np.frombuffer(
                    value_buffer, np.dtype(np.int64).newbyteorder(">")
                )
            # TEXT, STRING, BLOB
            elif data_type == 5 or data_type == 11 or data_type == 10:
                j = 0
                offset = 0
                data_array = []
                while offset < value_buffer_len:
                    length = int.from_bytes(
                        value_buffer[offset : offset + 4],
                        byteorder="big",
                        signed=False,
                    )
                    offset += 4
                    value = bytes(value_buffer[offset : offset + length])
                    data_array.append(value)
                    j += 1
                    offset += length
                data_array = np.array(data_array, dtype=object)
            else:
                raise RuntimeError("unsupported data type {}.".format(data_type))
            if data_array.dtype.byteorder == ">":
                data_array = data_array.byteswap().view(
                    data_array.dtype.newbyteorder("<")
                )
            if len(data_array) < total_length:
                # INT32, INT64, BOOLEAN, TIMESTAMP, DATE
                if (
                    data_type == 0
                    or data_type == 1
                    or data_type == 2
                    or data_type == 8
                    or data_type == 9
                ):
                    tmp_array = np.full(total_length, np.nan, np.float32)
                else:
                    tmp_array = np.full(total_length, None, dtype=object)

                bitmap_buffer = self.__query_data_set.bitmapList[location]
                buffer = _to_bitbuffer(bitmap_buffer)
                bit_mask = (np.frombuffer(buffer, "u1") - ord("0")).astype(bool)
                if len(bit_mask) != total_length:
                    bit_mask = bit_mask[:total_length]
                tmp_array[bit_mask] = data_array

                # INT32, DATE
                if data_type == 1 or data_type == 9:
                    tmp_array = pd.Series(tmp_array, dtype="Int32")
                # INT64, TIMESTAMP
                elif data_type == 2 or data_type == 8:
                    tmp_array = pd.Series(tmp_array, dtype="Int64")
                # BOOLEAN
                elif data_type == 0:
                    tmp_array = pd.Series(tmp_array, dtype="boolean")
                data_array = tmp_array

            result[i + 1] = data_array
        self.__query_data_set = None
        self.data_frame = pd.DataFrame(result, dtype=object)
        if not self.data_frame.empty:
            self.has_cached_data_frame = True

    def has_cached_result(self):
        return self.has_cached_data_frame

    def _has_next_result_set(self):
        if (self.__query_data_set is not None) and (
            len(self.__query_data_set.time) != 0
        ):
            return True
        if self.__empty_resultSet:
            return False
        if self.fetch_results():
            return True
        return False

    def result_set_to_pandas(self):
        result = {}
        for column_name in self.__column_name_list:
            result[column_name] = []
        while self._has_next_result_set():
            time_array = np.frombuffer(
                self.__query_data_set.time, np.dtype(np.longlong).newbyteorder(">")
            )
            if time_array.dtype.byteorder == ">":
                time_array = time_array.byteswap().view(
                    time_array.dtype.newbyteorder("<")
                )
            if self.ignore_timestamp is None or self.ignore_timestamp is False:
                result[IoTDBRpcDataSet.TIMESTAMP_STR].append(time_array)

            self.__query_data_set.time = []
            total_length = len(time_array)

            for i in range(len(self.__query_data_set.bitmapList)):
                if self.ignore_timestamp is True:
                    column_name = self.__column_name_list[i]
                else:
                    column_name = self.__column_name_list[i + 1]

                location = (
                    self.column_ordinal_dict[column_name] - IoTDBRpcDataSet.START_INDEX
                )
                if location < 0:
                    continue
                data_type = self.column_type_deduplicated_list[location]
                value_buffer = self.__query_data_set.valueList[location]
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
                    data_array = np.frombuffer(value_buffer, np.dtype("?"))
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
                    j = 0
                    offset = 0
                    data_array = []
                    while offset < value_buffer_len:
                        length = int.from_bytes(
                            value_buffer[offset : offset + 4],
                            byteorder="big",
                            signed=False,
                        )
                        offset += 4
                        value_bytes = bytes(value_buffer[offset : offset + length])
                        value = value_bytes.decode("utf-8")
                        data_array.append(value)
                        j += 1
                        offset += length
                    data_array = pd.Series(data_array).astype(str)
                # BLOB
                elif data_type == 10:
                    j = 0
                    offset = 0
                    data_array = []
                    while offset < value_buffer_len:
                        length = int.from_bytes(
                            value_buffer[offset : offset + 4],
                            byteorder="big",
                            signed=False,
                        )
                        offset += 4
                        value = value_buffer[offset : offset + length]
                        data_array.append(value)
                        j += 1
                        offset += length
                    data_array = pd.Series(data_array)
                # DATE
                elif data_type == 9:
                    data_array = np.frombuffer(
                        value_buffer, np.dtype(np.int32).newbyteorder(">")
                    )
                    data_array = pd.Series(data_array).apply(parse_int_to_date)
                else:
                    raise RuntimeError("unsupported data type {}.".format(data_type))
                if data_array.dtype.byteorder == ">":
                    data_array = data_array.byteswap().view(
                        data_array.dtype.newbyteorder("<")
                    )
                self.__query_data_set.valueList[location] = None
                tmp_array = []
                if len(data_array) < total_length:
                    # BOOLEAN, INT32, INT64, TIMESTAMP
                    if (
                        data_type == 0
                        or data_type == 1
                        or data_type == 2
                        or data_type == 8
                    ):
                        tmp_array = np.full(total_length, np.nan, dtype=np.float32)
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
                        tmp_array = np.full(total_length, None, dtype=data_array.dtype)

                    bitmap_buffer = self.__query_data_set.bitmapList[location]
                    buffer = _to_bitbuffer(bitmap_buffer)
                    bit_mask = (np.frombuffer(buffer, "u1") - ord("0")).astype(bool)
                    if len(bit_mask) != total_length:
                        bit_mask = bit_mask[:total_length]
                    tmp_array[bit_mask] = data_array

                    if data_type == 1:
                        tmp_array = pd.Series(tmp_array).astype("Int32")
                    elif data_type == 2 or data_type == 8:
                        tmp_array = pd.Series(tmp_array).astype("Int64")
                    elif data_type == 0:
                        tmp_array = pd.Series(tmp_array).astype("boolean")

                    data_array = tmp_array

                result[column_name].append(data_array)

        for k, v in result.items():
            if v is None or len(v) < 1 or v[0] is None:
                result[k] = []
            elif v[0].dtype == "Int32":
                result[k] = pd.Series(np.concatenate(v, axis=0)).astype("Int32")
            elif v[0].dtype == "Int64":
                result[k] = pd.Series(np.concatenate(v, axis=0)).astype("Int64")
            elif v[0].dtype == bool:
                result[k] = pd.Series(np.concatenate(v, axis=0)).astype("boolean")
            else:
                result[k] = np.concatenate(v, axis=0)

        df = pd.DataFrame(result)
        return df

    def fetch_results(self):
        self.__rows_index = 0
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
                self.__query_data_set = resp.queryDataSet
            return resp.hasResultSet
        except TTransport.TException as e:
            raise RuntimeError(
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
