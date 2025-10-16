1# Licensed to the Apache Software Foundation (ASF) under one
1# or more contributor license agreements.  See the NOTICE file
1# distributed with this work for additional information
1# regarding copyright ownership.  The ASF licenses this file
1# to you under the Apache License, Version 2.0 (the
1# "License"); you may not use this file except in compliance
1# with the License.  You may obtain a copy of the License at
1#
1#     http://www.apache.org/licenses/LICENSE-2.0
1#
1# Unless required by applicable law or agreed to in writing,
1# software distributed under the License is distributed on an
1# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1# KIND, either express or implied.  See the License for the
1# specific language governing permissions and limitations
1# under the License.
1#
1import struct
1from enum import Enum
1
1import numpy as np
1import pandas as pd
1
1from iotdb.ainode.core.exception import BadConfigValueError
1
1
1class TSDataType(Enum):
1    BOOLEAN = 0
1    INT32 = 1
1    INT64 = 2
1    FLOAT = 3
1    DOUBLE = 4
1    TEXT = 5
1
1    # this method is implemented to avoid the issue reported by:
1    # https://bugs.python.org/issue30545
1    def __eq__(self, other) -> bool:
1        return self.value == other.value
1
1    def __hash__(self):
1        return self.value
1
1    def np_dtype(self):
1        return {
1            TSDataType.BOOLEAN: np.dtype(">?"),
1            TSDataType.FLOAT: np.dtype(">f4"),
1            TSDataType.DOUBLE: np.dtype(">f8"),
1            TSDataType.INT32: np.dtype(">i4"),
1            TSDataType.INT64: np.dtype(">i8"),
1            TSDataType.TEXT: np.dtype("str"),
1        }[self]
1
1
1TIMESTAMP_STR = "Time"
1START_INDEX = 2
1
1
1# convert dataFrame to tsBlock in binary
1# input shouldn't contain time column
1def convert_to_binary(data_frame: pd.DataFrame):
1    data_shape = data_frame.shape
1    value_column_size = data_shape[1]
1    position_count = data_shape[0]
1    keys = data_frame.keys()
1
1    binary = value_column_size.to_bytes(4, byteorder="big")
1
1    for data_type in data_frame.dtypes:
1        binary += _get_type_in_byte(data_type)
1
1    # position count
1    binary += position_count.to_bytes(4, byteorder="big")
1
1    # column encoding
1    binary += b"\x02"
1    for data_type in data_frame.dtypes:
1        binary += _get_encoder(data_type)
1
1    # write columns, the column in index 0 must be timeColumn
1    binary += bool.to_bytes(False, 1, byteorder="big")
1    for i in range(position_count):
1        value = 0
1        v = struct.pack(">i", value)
1        binary += v
1        binary += v
1
1    for i in range(value_column_size):
1        # the value can't be null
1        binary += bool.to_bytes(False, 1, byteorder="big")
1        col = data_frame[keys[i]]
1        for j in range(position_count):
1            value = col[j]
1            if value.dtype.byteorder != ">":
1                value = value.byteswap()
1            binary += value.tobytes()
1
1    return binary
1
1
1def _get_encoder(data_type: pd.Series):
1    if data_type == "bool":
1        return b"\x00"
1    elif data_type == "int32" or data_type == "float32":
1        return b"\x01"
1    elif data_type == "int64" or data_type == "float64":
1        return b"\x02"
1    elif data_type == "texr":
1        return b"\x03"
1
1
1def _get_type_in_byte(data_type: pd.Series):
1    if data_type == "bool":
1        return b"\x00"
1    elif data_type == "int32":
1        return b"\x01"
1    elif data_type == "int64":
1        return b"\x02"
1    elif data_type == "float32":
1        return b"\x03"
1    elif data_type == "float64":
1        return b"\x04"
1    elif data_type == "text":
1        return b"\x05"
1    else:
1        raise BadConfigValueError(
1            "data_type",
1            data_type,
1            "data_type should be in ['bool', 'int32', 'int64', 'float32', 'float64', 'text']",
1        )
1
1
1# General Methods
1def get_data_type_byte_from_str(value):
1    """
1    Args:
1        value (str): data type in ['bool', 'int32', 'int64', 'float32', 'float64', 'text']
1    Returns:
1        byte: corresponding data type in [b'\x00', b'\x01', b'\x02', b'\x03', b'\x04', b'\x05']
1    """
1    if value not in ["bool", "int32", "int64", "float32", "float64", "text"]:
1        raise BadConfigValueError(
1            "data_type",
1            value,
1            "data_type should be in ['bool', 'int32', 'int64', 'float32', 'float64', 'text']",
1        )
1    if value == "bool":
1        return TSDataType.BOOLEAN.value
1    elif value == "int32":
1        return TSDataType.INT32.value
1    elif value == "int64":
1        return TSDataType.INT64.value
1    elif value == "float32":
1        return TSDataType.FLOAT.value
1    elif value == "float64":
1        return TSDataType.DOUBLE.value
1    elif value == "text":
1        return TSDataType.TEXT.value
1