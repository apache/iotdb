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
shift_table = tuple([0x80 >> i for i in range(8)])


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
            for i in range(len(column_name_list)):
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

        self.time_bytes = memoryview(b"\x00")
        self.__current_bitmap = [
            bytes(0) for _ in range(len(self.column_type_deduplicated_list))
        ]
        self.value = [None for _ in range(len(self.column_type_deduplicated_list))]
        self.__query_data_set = query_data_set
        self.__query_data_set.time = memoryview(self.__query_data_set.time)
        self.__query_data_set.valueList = [
            memoryview(value) for value in self.__query_data_set.valueList
        ]
        self.__query_data_set.bitmapList = [
            memoryview(bitmap) for bitmap in self.__query_data_set.bitmapList
        ]
        self.__is_closed = False
        self.__empty_resultSet = False
        self.has_cached_record = False
        self.__rows_index = 0
        self.is_null_info = [
            False for _ in range(len(self.column_type_deduplicated_list))
        ]

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
            if isinstance(self.__query_data_set.time, memoryview):
                self.__query_data_set.time.release()
            for value in self.__query_data_set.valueList:
                if isinstance(value, memoryview):
                    value.release()
            for bitmap in self.__query_data_set.bitmapList:
                if isinstance(bitmap, memoryview):
                    bitmap.release()

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

    def resultset_to_pandas(self):
        result = {}
        for column_name in self.__column_name_list:
            result[column_name] = []
        while self._has_next_result_set():
            time_array = np.frombuffer(
                self.__query_data_set.time, np.dtype(np.longlong).newbyteorder(">")
            )
            if time_array.dtype.byteorder == ">":
                time_array = time_array.byteswap().newbyteorder("<")
            if self.ignore_timestamp is None or self.ignore_timestamp is False:
                result[IoTDBRpcDataSet.TIMESTAMP_STR].append(time_array)

            self.__query_data_set.time = []
            total_length = len(time_array)

            for i in range(len(self.__query_data_set.bitmapList)):
                if self.ignore_timestamp is True:
                    column_name = self.get_column_names()[i]
                else:
                    column_name = self.get_column_names()[i + 1]

                location = (
                    self.column_ordinal_dict[column_name] - IoTDBRpcDataSet.START_INDEX
                )
                if location < 0:
                    continue
                data_type = self.column_type_deduplicated_list[location]
                value_buffer = self.__query_data_set.valueList[location]
                value_buffer_len = len(value_buffer)
                if data_type == 4:
                    data_array = np.frombuffer(
                        value_buffer, np.dtype(np.double).newbyteorder(">")
                    )
                elif data_type == 3:
                    data_array = np.frombuffer(
                        value_buffer, np.dtype(np.float32).newbyteorder(">")
                    )
                elif data_type == 0:
                    data_array = np.frombuffer(value_buffer, np.dtype("?"))
                elif data_type == 1:
                    data_array = np.frombuffer(
                        value_buffer, np.dtype(np.int32).newbyteorder(">")
                    )
                elif data_type == 2:
                    data_array = np.frombuffer(
                        value_buffer, np.dtype(np.int64).newbyteorder(">")
                    )
                elif data_type == 5:
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
                    data_array = np.array(data_array, dtype=object)
                else:
                    raise RuntimeError("unsupported data type {}.".format(data_type))
                if data_array.dtype.byteorder == ">":
                    data_array = data_array.byteswap().newbyteorder("<")
                self.__query_data_set.valueList[location] = None

                if len(data_array) < total_length:
                    if data_type == 1 or data_type == 2:
                        tmp_array = np.full(total_length, np.nan, np.float32)
                    elif data_type == 3 or data_type == 4:
                        tmp_array = np.full(total_length, np.nan, data_array.dtype)
                    elif data_type == 0:
                        tmp_array = np.full(total_length, np.nan, np.float32)
                    elif data_type == 5:
                        tmp_array = np.full(total_length, None, dtype=data_array.dtype)

                    bitmap_buffer = self.__query_data_set.bitmapList[location]
                    buffer = _to_bitbuffer(bitmap_buffer)
                    bit_mask = (np.frombuffer(buffer, "u1") - ord("0")).astype(bool)
                    if len(bit_mask) != total_length:
                        bit_mask = bit_mask[:total_length]
                    tmp_array[bit_mask] = data_array

                    if data_type == 1:
                        tmp_array = pd.Series(tmp_array).astype("Int32")
                    elif data_type == 2:
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
            elif v[0].dtype == "boolean":
                result[k] = pd.Series(np.concatenate(v, axis=0)).astype("boolean")
            else:
                result[k] = np.concatenate(v, axis=0)

        df = pd.DataFrame(result)
        return df

    def construct_one_row(self):
        # simulating buffer, read 8 bytes from data set and discard first 8 bytes which have been read.
        self.time_bytes = self.__query_data_set.time[:8]
        self.__query_data_set.time = self.__query_data_set.time[8:]
        for i, (value_buffer, data_type) in enumerate(
            zip(self.__query_data_set.valueList, self.column_type_deduplicated_list)
        ):
            # another 8 new rows, should move the bitmap buffer position to next byte
            if self.__rows_index % 8 == 0:
                bitmap_buffer = self.__query_data_set.bitmapList[i]
                self.__current_bitmap[i] = bitmap_buffer[0]
                self.__query_data_set.bitmapList[i] = bitmap_buffer[1:]
            is_null = shift_table[self.__rows_index % 8] & self.__current_bitmap[i] == 0
            self.is_null_info[i] = is_null
            if not is_null:
                # simulating buffer
                if data_type == 0:
                    self.value[i] = value_buffer[:1]
                    self.__query_data_set.valueList[i] = value_buffer[1:]
                elif data_type == 1 or data_type == 3:
                    self.value[i] = value_buffer[:4]
                    self.__query_data_set.valueList[i] = value_buffer[4:]
                elif data_type == 2 or data_type == 4:
                    self.value[i] = value_buffer[:8]
                    self.__query_data_set.valueList[i] = value_buffer[8:]
                elif data_type == 5:
                    length = int.from_bytes(
                        value_buffer[:4], byteorder="big", signed=False
                    )
                    self.value[i] = value_buffer[4 : 4 + length]
                    self.__query_data_set.valueList[i] = value_buffer[4 + length :]
                else:
                    raise RuntimeError("unsupported data type {}.".format(data_type))
        self.__rows_index += 1
        self.has_cached_record = True

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
                if isinstance(self.__query_data_set.time, memoryview):
                    self.__query_data_set.time.release()
                for value in self.__query_data_set.valueList:
                    if isinstance(value, memoryview):
                        value.release()
                for bitmap in self.__query_data_set.bitmapList:
                    if isinstance(bitmap, memoryview):
                        bitmap.release()
                self.__query_data_set = resp.queryDataSet
                self.__query_data_set.time = memoryview(self.__query_data_set.time)
                self.__query_data_set.valueList = [
                    memoryview(value) for value in self.__query_data_set.valueList
                ]
                self.__query_data_set.bitmapList = [
                    memoryview(bitmap) for bitmap in self.__query_data_set.bitmapList
                ]
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
