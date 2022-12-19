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
from iotdb.utils.IoTDBConstants import TSDataType

logger = logging.getLogger("IoTDB")


class IoTDBRpcDataSet(object):
    TIMESTAMP_STR = "Time"
    # VALUE_IS_NULL = "The value got by %s (column name) is NULL."
    START_INDEX = 2
    FLAG = 0x80

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
        self.__ignore_timestamp = ignore_timestamp
        self.__sql = sql
        self.__query_id = query_id
        self.__client = client
        self.__fetch_size = fetch_size
        self.__column_size = len(column_name_list)
        self.__default_time_out = 1000

        self.__column_name_list = []
        self.__column_type_list = []
        self.__column_ordinal_dict = {}
        if not ignore_timestamp:
            self.__column_name_list.append(IoTDBRpcDataSet.TIMESTAMP_STR)
            self.__column_type_list.append(TSDataType.INT64)
            self.__column_ordinal_dict[IoTDBRpcDataSet.TIMESTAMP_STR] = 1

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
                        index + IoTDBRpcDataSet.START_INDEX
                    )
                    self.__column_type_deduplicated_list[index] = TSDataType[
                        column_type_list[i]
                    ]
        else:
            index = IoTDBRpcDataSet.START_INDEX
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

        self.__time_bytes = bytes(0)
        self.__current_bitmap = [
            bytes(0) for _ in range(len(self.__column_type_deduplicated_list))
        ]
        self.__value = [None for _ in range(len(self.__column_type_deduplicated_list))]
        self.__query_data_set = query_data_set
        self.__is_closed = False
        self.__empty_resultSet = False
        self.__has_cached_record = False
        self.__rows_index = 0

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
        if self.has_cached_result():
            self.construct_one_row()
            return True
        if self.__empty_resultSet:
            return False
        if self.fetch_results():
            self.construct_one_row()
            return True
        return False

    def has_cached_result(self):
        return (self.__query_data_set is not None) and (
            len(self.__query_data_set.time) != 0
        )

    def _has_next_result_set(self):
        if self.has_cached_result():
            return True
        if self.__empty_resultSet:
            return False
        if self.fetch_results():
            return True
        return False

    def _to_bitstring(self, b):
        return "{:0{}b}".format(int(binascii.hexlify(b), 16), 8 * len(b))

    def resultset_to_pandas(self):
        result = {}
        for column_name in self.__column_name_list:
            result[column_name] = None
        while self._has_next_result_set():
            time_array = np.frombuffer(
                self.__query_data_set.time, np.dtype(np.longlong).newbyteorder(">")
            )
            if time_array.dtype.byteorder == ">":
                time_array = time_array.byteswap().newbyteorder("<")
            if (
                self.get_ignore_timestamp() is None
                or self.get_ignore_timestamp() is False
            ):
                if result[IoTDBRpcDataSet.TIMESTAMP_STR] is None:
                    result[IoTDBRpcDataSet.TIMESTAMP_STR] = time_array
                else:
                    result[IoTDBRpcDataSet.TIMESTAMP_STR] = np.concatenate(
                        (result[IoTDBRpcDataSet.TIMESTAMP_STR], time_array), axis=0
                    )
            self.__query_data_set.time = []
            total_length = len(time_array)

            for i in range(len(self.__query_data_set.bitmapList)):
                if self.get_ignore_timestamp() is True:
                    column_name = self.get_column_names()[i]
                else:
                    column_name = self.get_column_names()[i + 1]

                location = (
                    self.__column_ordinal_dict[column_name]
                    - IoTDBRpcDataSet.START_INDEX
                )
                if location < 0:
                    continue
                data_type = self.__column_type_deduplicated_list[location]
                value_buffer = self.__query_data_set.valueList[location]
                value_buffer_len = len(value_buffer)

                data_array = None
                if data_type == TSDataType.DOUBLE:
                    data_array = np.frombuffer(
                        value_buffer, np.dtype(np.double).newbyteorder(">")
                    )
                elif data_type == TSDataType.FLOAT:
                    data_array = np.frombuffer(
                        value_buffer, np.dtype(np.float32).newbyteorder(">")
                    )
                elif data_type == TSDataType.BOOLEAN:
                    data_array = np.frombuffer(value_buffer, np.dtype("?"))
                elif data_type == TSDataType.INT32:
                    data_array = np.frombuffer(
                        value_buffer, np.dtype(np.int32).newbyteorder(">")
                    )
                elif data_type == TSDataType.INT64:
                    data_array = np.frombuffer(
                        value_buffer, np.dtype(np.int64).newbyteorder(">")
                    )
                elif data_type == TSDataType.TEXT:
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
                        value_bytes = value_buffer[offset : offset + length]
                        value = value_bytes.decode("utf-8")
                        data_array.append(value)
                        j += 1
                        offset += length
                    data_array = np.array(data_array, dtype=object)
                else:
                    raise RuntimeError("unsupported data type {}.".format(data_type))
                if data_array.dtype.byteorder == ">":
                    data_array = data_array.byteswap().newbyteorder("<")
                self.__query_data_set.valueList[location] = None

                if len(data_array) < total_length:
                    if data_type == TSDataType.INT32 or data_type == TSDataType.INT64:
                        tmp_array = np.full(total_length, np.nan, np.float32)
                    elif data_type == TSDataType.FLOAT or data_type == TSDataType.DOUBLE:
                        tmp_array = np.full(total_length, np.nan, data_array.dtype)
                    elif data_type == TSDataType.BOOLEAN:
                        tmp_array = np.full(total_length, np.nan, np.float32)
                    elif data_type == TSDataType.TEXT:
                        tmp_array = np.full(total_length, None, dtype=data_array.dtype)

                    bitmap_buffer = self.__query_data_set.bitmapList[location]
                    bitmap_str = self._to_bitstring(bitmap_buffer)
                    bit_mask = (np.fromstring(bitmap_str, 'u1') - ord('0')).astype(bool)
                    if len(bit_mask) != total_length:
                        bit_mask = bit_mask[:total_length]
                    tmp_array[bit_mask] = data_array

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
                        result[column_name] = result[column_name].append(data_array)
                    else:
                        result[column_name] = np.concatenate(
                            (result[column_name], data_array), axis=0
                        )
        for k, v in result.items():
            if v is None:
                result[k] = []

        df = pd.DataFrame(result)
        return df

    def construct_one_row(self):
        # simulating buffer, read 8 bytes from data set and discard first 8 bytes which have been read.
        self.__time_bytes = self.__query_data_set.time[:8]
        self.__query_data_set.time = self.__query_data_set.time[8:]
        for i in range(len(self.__query_data_set.bitmapList)):
            bitmap_buffer = self.__query_data_set.bitmapList[i]

            # another 8 new rows, should move the bitmap buffer position to next byte
            if self.__rows_index % 8 == 0:
                self.__current_bitmap[i] = bitmap_buffer[0]
                self.__query_data_set.bitmapList[i] = bitmap_buffer[1:]
            if not self.is_null(i, self.__rows_index):
                value_buffer = self.__query_data_set.valueList[i]
                data_type = self.__column_type_deduplicated_list[i]

                # simulating buffer
                if data_type == TSDataType.BOOLEAN:
                    self.__value[i] = value_buffer[:1]
                    self.__query_data_set.valueList[i] = value_buffer[1:]
                elif data_type == TSDataType.INT32:
                    self.__value[i] = value_buffer[:4]
                    self.__query_data_set.valueList[i] = value_buffer[4:]
                elif data_type == TSDataType.INT64:
                    self.__value[i] = value_buffer[:8]
                    self.__query_data_set.valueList[i] = value_buffer[8:]
                elif data_type == TSDataType.FLOAT:
                    self.__value[i] = value_buffer[:4]
                    self.__query_data_set.valueList[i] = value_buffer[4:]
                elif data_type == TSDataType.DOUBLE:
                    self.__value[i] = value_buffer[:8]
                    self.__query_data_set.valueList[i] = value_buffer[8:]
                elif data_type == TSDataType.TEXT:
                    length = int.from_bytes(
                        value_buffer[:4], byteorder="big", signed=False
                    )
                    self.__value[i] = value_buffer[4 : 4 + length]
                    self.__query_data_set.valueList[i] = value_buffer[4 + length :]
                else:
                    raise RuntimeError("unsupported data type {}.".format(data_type))
        self.__rows_index += 1
        self.__has_cached_record = True

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

    def is_null(self, index, row_num):
        bitmap = self.__current_bitmap[index]
        shift = row_num % 8
        return ((IoTDBRpcDataSet.FLAG >> shift) & (bitmap & 0xFF)) == 0

    def is_null_by_index(self, column_index):
        index = (
            self.__column_ordinal_dict[self.find_column_name_by_index(column_index)]
            - IoTDBRpcDataSet.START_INDEX
        )
        # time column will never be None
        if index < 0:
            return True
        return self.is_null(index, self.__rows_index - 1)

    def is_null_by_name(self, column_name):
        index = self.__column_ordinal_dict[column_name] - IoTDBRpcDataSet.START_INDEX
        # time column will never be None
        if index < 0:
            return True
        return self.is_null(index, self.__rows_index - 1)

    def find_column_name_by_index(self, column_index):
        if column_index <= 0:
            raise Exception("Column index should start from 1")
        if column_index > len(self.__column_name_list):
            raise Exception(
                "column index {} out of range {}".format(
                    column_index, self.__column_size
                )
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

    def get_column_size(self):
        return self.__column_size

    def get_ignore_timestamp(self):
        return self.__ignore_timestamp

    def get_column_ordinal_dict(self):
        return self.__column_ordinal_dict

    def get_column_type_deduplicated_list(self):
        return self.__column_type_deduplicated_list

    def get_values(self):
        return self.__value

    def get_time_bytes(self):
        return self.__time_bytes

    def get_has_cached_record(self):
        return self.__has_cached_record
