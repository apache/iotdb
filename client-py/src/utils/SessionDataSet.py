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

from IoTDBConstants import TSDataType
from IoTDBRpcDataSet import IoTDBRpcDataSet
from Field import Field
from RowRecord import RowRecord
import struct


class SessionDataSet(object):

    def __init__(self, sql, column_name_list, column_type_list, column_name_index, query_id, client, session_id,
                 query_data_set, ignore_timestamp):
        self.iotdb_rpc_data_set = IoTDBRpcDataSet(sql, column_name_list, column_type_list, column_name_index,
                                                  ignore_timestamp, query_id, client, session_id, query_data_set, 1024)

    def get_fetch_size(self):
        return self.iotdb_rpc_data_set.get_fetch_size()

    def set_fetch_size(self, fetch_size):
        self.iotdb_rpc_data_set.set_fetch_size(fetch_size)

    def get_column_names(self):
        return self.iotdb_rpc_data_set.get_column_names()

    def get_column_types(self):
        return self.iotdb_rpc_data_set.get_column_types()

    def has_next(self):
        return self.iotdb_rpc_data_set.next()

    def next(self):
        if not self.iotdb_rpc_data_set.get_has_cached_record():
            if not self.has_next():
                return None
        self.iotdb_rpc_data_set.has_cached_record = False
        return self.construct_row_record_from_value_array()

    def construct_row_record_from_value_array(self):
        out_fields = []
        for i in range(self.iotdb_rpc_data_set.get_column_size()):
            index = i + 1
            data_set_column_index = i + IoTDBRpcDataSet.START_INDEX
            if self.iotdb_rpc_data_set.get_ignore_timestamp():
                index -= 1
                data_set_column_index -= 1
            column_name = self.iotdb_rpc_data_set.get_column_names()[index]
            location = self.iotdb_rpc_data_set.get_column_ordinal_dict()[column_name] - IoTDBRpcDataSet.START_INDEX

            if not self.iotdb_rpc_data_set.is_null_by_index(data_set_column_index):
                value_bytes = self.iotdb_rpc_data_set.get_values()[location]
                data_type = self.iotdb_rpc_data_set.get_column_type_deduplicated_list()[location]
                field = Field(data_type)
                if data_type == TSDataType.BOOLEAN:
                    value = struct.unpack(">?", value_bytes)[0]
                    field.set_bool_value(value)
                elif data_type == TSDataType.INT32:
                    value = struct.unpack(">i", value_bytes)[0]
                    field.set_int_value(value)
                elif data_type == TSDataType.INT64:
                    value = struct.unpack(">q", value_bytes)[0]
                    field.set_long_value(value)
                elif data_type == TSDataType.FLOAT:
                    value = struct.unpack(">f", value_bytes)[0]
                    field.set_float_value(value)
                elif data_type == TSDataType.DOUBLE:
                    value = struct.unpack(">d", value_bytes)[0]
                    field.set_double_value(value)
                elif data_type == TSDataType.TEXT:
                    field.set_binary_value(value_bytes)
                else:
                    print("unsupported data type {}.".format(data_type))
                    # could raise exception here
            else:
                field = Field(None)
            out_fields.append(field)

        return RowRecord(struct.unpack(">q", self.iotdb_rpc_data_set.get_time_bytes())[0], out_fields)

    def close_operation_handle(self):
        self.iotdb_rpc_data_set.close()




