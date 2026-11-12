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
from enum import Enum

import numpy as np
import pandas as pd
import torch

from iotdb.ainode.core.exception import BadConfigValueException
from iotdb.tsfile.utils.tsblock_serde import deserialize


class TSDataType(Enum):
    BOOLEAN = 0
    INT32 = 1
    INT64 = 2
    FLOAT = 3
    DOUBLE = 4
    TEXT = 5

    # this method is implemented to avoid the issue reported by:
    # https://bugs.python.org/issue30545
    def __eq__(self, other) -> bool:
        return self.value == other.value

    def __hash__(self):
        return self.value

    def np_dtype(self):
        return {
            TSDataType.BOOLEAN: np.dtype(">?"),
            TSDataType.FLOAT: np.dtype(">f4"),
            TSDataType.DOUBLE: np.dtype(">f8"),
            TSDataType.INT32: np.dtype(">i4"),
            TSDataType.INT64: np.dtype(">i8"),
            TSDataType.TEXT: np.dtype("str"),
        }[self]


TIMESTAMP_STR = "Time"
START_INDEX = 2


# Full data deserialized from iotdb tsblock is composed of [timestampList, multiple valueList, None, length].
# We only get valueList currently.
def convert_tsblock_to_tensor(tsblock_data: bytes):
    full_data = deserialize(tsblock_data)
    # ensure the byteorder is correct.
    for i, data in enumerate(full_data[1]):
        if data.dtype.byteorder not in ("=", "|"):
            np_data = data.byteswap()
            full_data[1][i] = np_data.view(np_data.dtype.newbyteorder())
    # the size should be [batch_size, target_count, sequence_length]
    tensor_data = torch.from_numpy(np.stack(full_data[1], axis=0)).unsqueeze(0).float()
    # data should be on CPU before passing to the inference request
    return tensor_data.to("cpu")


# Convert a tensor to TsBlock in binary, input shouldn't contain time column.
# Maybe contain multiple value columns.
def convert_tensor_to_tsblock(data_tensor: torch.Tensor):
    # Ensure the tensor is 2D with size [target_count, sequence_length]
    if data_tensor.dim() == 0:
        data_tensor = data_tensor.unsqueeze(0).unsqueeze(0)
    elif data_tensor.dim() == 1:
        data_tensor = data_tensor.unsqueeze(0)

    # one value column per target, each of sequence_length values
    columns = data_tensor.detach().cpu().numpy()
    value_column_size, position_count = columns.shape[0], columns.shape[1]
    data_type = str(columns.dtype)

    parts = [value_column_size.to_bytes(4, byteorder="big")]
    parts.extend([_get_type_in_byte(data_type)] * value_column_size)

    # position count
    parts.append(position_count.to_bytes(4, byteorder="big"))

    # column encoding
    parts.append(b"\x02")
    parts.extend([_get_encoder(data_type)] * value_column_size)

    # write columns, the column in index 0 must be timeColumn: no nulls, then two big-endian
    # int32 zeros per position
    parts.append(bool.to_bytes(False, 1, byteorder="big"))
    parts.append(bytes(8 * position_count))

    # the value columns: no nulls, then the values in big-endian order. The parts are joined once
    # at the end: appending to a bytes object copies it, which made the conversion quadratic in the
    # number of values.
    big_endian = columns.dtype.newbyteorder(">")
    for column in columns:
        # the value can't be null
        parts.append(bool.to_bytes(False, 1, byteorder="big"))
        parts.append(np.ascontiguousarray(column, dtype=big_endian).tobytes())

    return b"".join(parts)


def _get_encoder(data_type: pd.Series):
    if data_type == "bool":
        return b"\x00"
    elif data_type == "int32" or data_type == "float32":
        return b"\x01"
    elif data_type == "int64" or data_type == "float64":
        return b"\x02"
    elif data_type == "texr":
        return b"\x03"


def _get_type_in_byte(data_type: pd.Series):
    if data_type == "bool":
        return b"\x00"
    elif data_type == "int32":
        return b"\x01"
    elif data_type == "int64":
        return b"\x02"
    elif data_type == "float32":
        return b"\x03"
    elif data_type == "float64":
        return b"\x04"
    elif data_type == "text":
        return b"\x05"
    else:
        raise BadConfigValueException(
            "data_type",
            data_type,
            "data_type should be in ['bool', 'int32', 'int64', 'float32', 'float64', 'text']",
        )


# General Methods
def get_data_type_byte_from_str(value):
    """
    Args:
        value (str): data type in ['bool', 'int32', 'int64', 'float32', 'float64', 'text']
    Returns:
        byte: corresponding data type in [b'\x00', b'\x01', b'\x02', b'\x03', b'\x04', b'\x05']
    """
    if value not in ["bool", "int32", "int64", "float32", "float64", "text"]:
        raise BadConfigValueException(
            "data_type",
            value,
            "data_type should be in ['bool', 'int32', 'int64', 'float32', 'float64', 'text']",
        )
    if value == "bool":
        return TSDataType.BOOLEAN.value
    elif value == "int32":
        return TSDataType.INT32.value
    elif value == "int64":
        return TSDataType.INT64.value
    elif value == "float32":
        return TSDataType.FLOAT.value
    elif value == "float64":
        return TSDataType.DOUBLE.value
    elif value == "text":
        return TSDataType.TEXT.value
