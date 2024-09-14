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
import logging

from iotdb.utils.Field import Field

# for package
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.IoTDBRpcDataSet import IoTDBRpcDataSet
from iotdb.utils.RowRecord import RowRecord

import pandas as pd

logger = logging.getLogger("IoTDB")


class SessionDataSet(object):
    def __init__(
        self,
        sql,
        column_name_list,
        column_type_list,
        column_name_index,
        query_id,
        client,
        statement_id,
        session_id,
        query_data_set,
        ignore_timestamp,
    ):
        self.iotdb_rpc_data_set = IoTDBRpcDataSet(
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
            5000,
        )
        self.column_size = self.iotdb_rpc_data_set.column_size
        self.is_ignore_timestamp = self.iotdb_rpc_data_set.ignore_timestamp
        self.column_names = tuple(self.iotdb_rpc_data_set.get_column_names())
        self.column_ordinal_dict = self.iotdb_rpc_data_set.column_ordinal_dict
        self.column_type_deduplicated_list = tuple(
            self.iotdb_rpc_data_set.column_type_deduplicated_list
        )
        if self.is_ignore_timestamp:
            self.__field_list = [
                Field(data_type)
                for data_type in self.iotdb_rpc_data_set.get_column_types()
            ]
        else:
            self.__field_list = [
                Field(data_type)
                for data_type in self.iotdb_rpc_data_set.get_column_types()[1:]
            ]
        self.row_index = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_operation_handle()

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
        if not self.iotdb_rpc_data_set.has_cached_data_frame:
            if not self.has_next():
                return None
        return self.construct_row_record_from_data_frame()

    def construct_row_record_from_data_frame(self):
        df = self.iotdb_rpc_data_set.data_frame
        row = df.iloc[self.row_index].to_list()
        row_values = row[1:]
        for i, value in enumerate(row_values):
            self.__field_list[i].value = value

        row_record = RowRecord(
            row[0],
            self.__field_list,
        )
        self.row_index += 1
        if self.row_index == len(df):
            self.row_index = 0
            self.iotdb_rpc_data_set.has_cached_data_frame = False
            self.iotdb_rpc_data_set.data_frame = None

        return row_record

    def close_operation_handle(self):
        self.iotdb_rpc_data_set.close()

    def todf(self) -> pd.DataFrame:
        return result_set_to_pandas(self)


def result_set_to_pandas(result_set: SessionDataSet) -> pd.DataFrame:
    """
    Transforms a SessionDataSet from IoTDB to a Pandas Data Frame
    Each Field from IoTDB is a column in Pandas
    :param result_set:
    :return:
    """
    return result_set.iotdb_rpc_data_set.result_set_to_pandas()


def get_typed_point(field: Field, none_value=None):
    choices = {
        # In Case of Boolean, cast to 0 / 1
        TSDataType.BOOLEAN: lambda f: 1 if f.get_bool_value() else 0,
        TSDataType.TEXT: lambda f: f.get_string_value(),
        TSDataType.FLOAT: lambda f: f.get_float_value(),
        TSDataType.INT32: lambda f: f.get_int_value(),
        TSDataType.DOUBLE: lambda f: f.get_double_value(),
        TSDataType.INT64: lambda f: f.get_long_value(),
        TSDataType.TIMESTAMP: lambda f: f.get_long_value(),
        TSDataType.STRING: lambda f: f.get_string_value(),
        TSDataType.DATE: lambda f: f.get_date_value(),
        TSDataType.BLOB: lambda f: f.get_binary_value(),
    }

    result_next_type: TSDataType = field.get_data_type()

    if result_next_type in choices.keys():
        return choices.get(result_next_type)(field)
    elif result_next_type is None:
        return none_value
    else:
        raise Exception(f"Unknown DataType {result_next_type}!")
