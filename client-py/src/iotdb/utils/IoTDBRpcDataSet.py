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
from .IoTDBConstants import *

# for debug
# from IoTDBConstants import *

import sys
from os.path import dirname, abspath
path = dirname(dirname(abspath(__file__)))
sys.path.append(path)

from thrift.transport import TTransport
from iotdb.thrift.rpc.TSIService import TSFetchResultsReq, TSCloseOperationReq


class IoTDBRpcDataSet(object):
    TIMESTAMP_STR = "Time"
    # VALUE_IS_NULL = "The value got by %s (column name) is NULL."
    START_INDEX = 2
    FLAG = 0x80

    def __init__(self, sql, column_name_list, column_type_list, column_name_index, ignore_timestamp, query_id,
        client, session_id, query_data_set, fetch_size):
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
            self.__column_type_deduplicated_list = [None for _ in range(len(column_name_index))]
            for i in range(len(column_name_list)):
                name = column_name_list[i]
                self.__column_name_list.append(name)
                self.__column_type_list.append(TSDataType[column_type_list[i]])
                if name not in self.__column_ordinal_dict:
                    index = column_name_index[name]
                    self.__column_ordinal_dict[name] = index + IoTDBRpcDataSet.START_INDEX
                    self.__column_type_deduplicated_list[index] = TSDataType[column_type_list[i]]
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
                    self.__column_type_deduplicated_list.append(TSDataType[column_type_list[i]])

        self.__time_bytes = bytes(0)
        self.__current_bitmap = [bytes(0) for _ in range(len(self.__column_type_deduplicated_list))]
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
                status = self.__client.closeOperation(TSCloseOperationReq(self.__session_id, self.__query_id))
                print("close session {}, message: {}".format(self.__session_id, status.message))
            except TTransport.TException as e:
                print("close session {} failed because: ".format(self.__session_id), e)
                raise Exception

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
        return (self.__query_data_set is not None) and (len(self.__query_data_set.time) != 0)

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
                    length = int.from_bytes(value_buffer[:4], byteorder="big", signed=False)
                    self.__value[i] = value_buffer[4: 4 + length]
                    self.__query_data_set.valueList[i] = value_buffer[4 + length:]
                else:
                    print("unsupported data type {}.".format(data_type))
                    # could raise exception here
        self.__rows_index += 1
        self.__has_cached_record = True

    def fetch_results(self):
        self.__rows_index = 0
        request = TSFetchResultsReq(self.__session_id, self.__sql, self.__fetch_size, self.__query_id, True, self.__default_time_out)
        try:
            resp = self.__client.fetchResults(request)
            if not resp.hasResultSet:
                self.__empty_resultSet = True
            else:
                self.__query_data_set = resp.queryDataSet
            return resp.hasResultSet
        except TTransport.TException as e:
            print("Cannot fetch result from server, because of network connection: ", e)

    def is_null(self, index, row_num):
        bitmap = self.__current_bitmap[index]
        shift = row_num % 8
        return ((IoTDBRpcDataSet.FLAG >> shift) & (bitmap & 0xff)) == 0

    def is_null_by_index(self, column_index):
        index = self.__column_ordinal_dict[self.find_column_name_by_index(column_index)] - IoTDBRpcDataSet.START_INDEX
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
            raise Exception("column index {} out of range {}".format(column_index, self.__column_size))
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
