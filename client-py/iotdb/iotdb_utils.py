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

import pandas as pd

from iotdb.utils.Field import Field
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.SessionDataSet import SessionDataSet


def resultset_to_pandas(result_set: SessionDataSet) -> pd.DataFrame:
    """
    Transforms a SessionDataSet from IoTDB to a Pandas Data Frame
    Each Field from IoTDB is a column in Pandas
    :param result_set:
    :return:
    """
    # get column names and fields
    column_names = result_set.get_column_names()

    value_dict = {}

    for i in range(len(column_names)):
        value_dict[column_names[i]] = []

    while result_set.has_next():
        record = result_set.next()

        value_dict["Time"].append(record.get_timestamp())

        for col in range(len(record.get_fields())):
            field: Field = record.get_fields()[col]

            value_dict[column_names[col + 1]].append(
                get_typed_point(field)
            )

    return pd.DataFrame(value_dict)


def get_typed_point(field: Field, none_value=None):
    choices = {
        # In Case of Boolean, cast to 0 / 1
        TSDataType.BOOLEAN: lambda field: 1 if field.get_bool_value() else 0,
        TSDataType.TEXT: lambda field: field.get_string_value(),
        TSDataType.FLOAT: lambda field: field.get_float_value(),
        TSDataType.INT32: lambda field: field.get_int_value(),
        TSDataType.DOUBLE: lambda field: field.get_double_value(),
        TSDataType.INT64: lambda field: field.get_long_value(),
    }

    result_next_type: TSDataType = field.get_data_type()

    if result_next_type in choices.keys():
        return choices.get(result_next_type)(field)
    elif result_next_type is None:
        return none_value
    else:
        raise Exception(f"Unknown DataType {result_next_type}!")
