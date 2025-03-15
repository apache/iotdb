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
import struct
from enum import Enum

import numpy as np
import pandas as pd

from ainode.core.exception import BadConfigValueError


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


# convert dataFrame to tsBlock in binary
# input shouldn't contain time column
def convert_to_binary(data_frame: pd.DataFrame):
    data_shape = data_frame.shape
    value_column_size = data_shape[1]
    position_count = data_shape[0]
    keys = data_frame.keys()

    binary = value_column_size.to_bytes(4, byteorder="big")

    for data_type in data_frame.dtypes:
        binary += _get_type_in_byte(data_type)

    # position count
    binary += position_count.to_bytes(4, byteorder="big")

    # column encoding
    binary += b'\x02'
    for data_type in data_frame.dtypes:
        binary += _get_encoder(data_type)

    # write columns, the column in index 0 must be timeColumn
    binary += bool.to_bytes(False, 1, byteorder="big")
    for i in range(position_count):
        value = 0
        v = struct.pack(">i", value)
        binary += v
        binary += v

    for i in range(value_column_size):
        # the value can't be null
        binary += bool.to_bytes(False, 1, byteorder="big")
        col = data_frame[keys[i]]
        for j in range(position_count):
            value = col[j]
            if value.dtype.byteorder != '>':
                value = value.byteswap()
            binary += value.tobytes()

    return binary


# convert tsBlock in binary to dataFrame
def convert_to_df(name_list, type_list, name_index, binary_list):
    column_name_list = [TIMESTAMP_STR]
    column_type_list = [TSDataType.INT64]
    column_ordinal_dict = {TIMESTAMP_STR: 1}

    if name_index is not None:
        column_type_deduplicated_list = [
            None for _ in range(len(name_index))
        ]
        for i in range(len(name_list)):
            name = name_list[i]
            column_name_list.append(name)
            column_type_list.append(TSDataType[type_list[i]])
            if name not in column_ordinal_dict:
                index = name_index[name]
                column_ordinal_dict[name] = index + START_INDEX
                column_type_deduplicated_list[index] = TSDataType[type_list[i]]
    else:
        index = START_INDEX
        column_type_deduplicated_list = []
        for i in range(len(name_list)):
            name = name_list[i]
            column_name_list.append(name)
            column_type_list.append(TSDataType[type_list[i]])
            if name not in column_ordinal_dict:
                column_ordinal_dict[name] = index
                index += 1
                column_type_deduplicated_list.append(
                    TSDataType[type_list[i]]
                )

    binary_size = len(binary_list)
    binary_index = 0
    result = {}
    for column_name in column_name_list:
        result[column_name] = None

    while binary_index < binary_size:
        buffer = binary_list[binary_index]
        binary_index += 1
        time_column_values, column_values, null_indicators, _ = deserialize(buffer)
        time_array = np.frombuffer(
            time_column_values, np.dtype(np.longlong).newbyteorder(">")
        )
        if time_array.dtype.byteorder == ">":
            time_array = time_array.byteswap().view(time_array.dtype.newbyteorder("<"))

        if result[TIMESTAMP_STR] is None:
            result[TIMESTAMP_STR] = time_array
        else:
            result[TIMESTAMP_STR] = np.concatenate(
                (result[TIMESTAMP_STR], time_array), axis=0
            )
        total_length = len(time_array)

        for i in range(len(column_values)):
            column_name = column_name_list[i + 1]

            location = column_ordinal_dict[column_name] - START_INDEX
            if location < 0:
                continue

            data_type = column_type_deduplicated_list[location]
            value_buffer = column_values[location]
            value_buffer_len = len(value_buffer)

            if data_type == TSDataType.DOUBLE:
                data_array = np.frombuffer(
                    value_buffer, np.dtype(np.double).newbyteorder(">")
                )
            elif data_type == TSDataType.FLOAT:
                data_array = np.frombuffer(
                    value_buffer, np.dtype(np.float32).newbyteorder(">")
                )
            elif data_type == TSDataType.BOOLEAN:
                data_array = []
                for index in range(len(value_buffer)):
                    data_array.append(value_buffer[index])
                data_array = np.array(data_array).astype("bool")
            elif data_type == TSDataType.INT32:
                data_array = np.frombuffer(
                    value_buffer, np.dtype(np.int32).newbyteorder(">")
                )
            elif data_type == TSDataType.INT64:
                data_array = np.frombuffer(
                    value_buffer, np.dtype(np.int64).newbyteorder(">")
                )
            elif data_type == TSDataType.TEXT:
                index = 0
                data_array = []
                while index < value_buffer_len:
                    value_bytes = value_buffer[index]
                    value = value_bytes.decode("utf-8")
                    data_array.append(value)
                    index += 1
                data_array = np.array(data_array, dtype=object)
            else:
                raise RuntimeError("unsupported data type {}.".format(data_type))

            if data_array.dtype.byteorder == ">":
                data_array = data_array.byteswap().view(data_array.dtype.newbyteorder("<"))

            null_indicator = null_indicators[location]
            if len(data_array) < total_length or (data_type == TSDataType.BOOLEAN and null_indicator is not None):
                if data_type == TSDataType.INT32 or data_type == TSDataType.INT64:
                    tmp_array = np.full(total_length, np.nan, np.float32)
                elif data_type == TSDataType.FLOAT or data_type == TSDataType.DOUBLE:
                    tmp_array = np.full(total_length, np.nan, data_array.dtype)
                elif data_type == TSDataType.BOOLEAN:
                    tmp_array = np.full(total_length, np.nan, np.float32)
                elif data_type == TSDataType.TEXT:
                    tmp_array = np.full(total_length, np.nan, dtype=data_array.dtype)
                else:
                    raise Exception("Unsupported dataType in deserialization")

                if null_indicator is not None:
                    indexes = [not v for v in null_indicator]
                    if data_type == TSDataType.BOOLEAN:
                        tmp_array[indexes] = data_array[indexes]
                    else:
                        tmp_array[indexes] = data_array

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
                    if not isinstance(data_array, pd.Series):
                        if data_type == TSDataType.INT32:
                            data_array = pd.Series(data_array).astype("Int32")
                        elif data_type == TSDataType.INT64:
                            data_array = pd.Series(data_array).astype("Int64")
                        elif data_type == TSDataType.BOOLEAN:
                            data_array = pd.Series(data_array).astype("boolean")
                        else:
                            raise RuntimeError("Series Error")
                    result[column_name] = result[column_name].append(data_array)
                else:
                    result[column_name] = np.concatenate(
                        (result[column_name], data_array), axis=0
                    )
    for k, v in result.items():
        if v is None:
            result[k] = []
    df = pd.DataFrame(result)
    df = df.reset_index(drop=True)
    return df


def _get_encoder(data_type: pd.Series):
    if data_type == "bool":
        return b'\x00'
    elif data_type == "int32" or data_type == "float32":
        return b'\x01'
    elif data_type == "int64" or data_type == "float64":
        return b'\x02'
    elif data_type == "texr":
        return b'\x03'


def _get_type_in_byte(data_type: pd.Series):
    if data_type == 'bool':
        return b'\x00'
    elif data_type == 'int32':
        return b'\x01'
    elif data_type == 'int64':
        return b'\x02'
    elif data_type == 'float32':
        return b'\x03'
    elif data_type == 'float64':
        return b'\x04'
    elif data_type == 'text':
        return b'\x05'
    else:
        raise BadConfigValueError('data_type', data_type,
                                  "data_type should be in ['bool', 'int32', 'int64', 'float32', 'float64', 'text']")


# Serialized tsBlock:
#    +-------------+---------------+---------+------------+-----------+----------+
#    | val col cnt | val col types | pos cnt | encodings  | time col  | val col  |
#    +-------------+---------------+---------+------------+-----------+----------+
#    | int32       | list[byte]    | int32   | list[byte] |  bytes    | byte     |
#    +-------------+---------------+---------+------------+-----------+----------+

def deserialize(buffer):
    value_column_count, buffer = read_int_from_buffer(buffer)
    data_types, buffer = read_column_types(buffer, value_column_count)

    position_count, buffer = read_int_from_buffer(buffer)
    column_encodings, buffer = read_column_encoding(buffer, value_column_count + 1)

    time_column_values, buffer = read_time_column(buffer, position_count)
    column_values = [None] * value_column_count
    null_indicators = [None] * value_column_count
    for i in range(value_column_count):
        column_value, null_indicator, buffer = read_column(column_encodings[i + 1], buffer, data_types[i],
                                                           position_count)
        column_values[i] = column_value
        null_indicators[i] = null_indicator

    return time_column_values, column_values, null_indicators, position_count


# General Methods

def read_int_from_buffer(buffer):
    res, buffer = read_from_buffer(buffer, 4)
    return int.from_bytes(res, "big"), buffer


def read_byte_from_buffer(buffer):
    return read_from_buffer(buffer, 1)


def read_from_buffer(buffer, size):
    res = buffer[:size]
    buffer = buffer[size:]
    return res, buffer


# Read ColumnType

def read_column_types(buffer, value_column_count):
    data_types = []
    for _ in range(value_column_count):
        res, buffer = read_byte_from_buffer(buffer)
        data_types.append(get_data_type(res))
    return data_types, buffer


def get_data_type(value):
    if value == b'\x00':
        return TSDataType.BOOLEAN
    elif value == b'\x01':
        return TSDataType.INT32
    elif value == b'\x02':
        return TSDataType.INT64
    elif value == b'\x03':
        return TSDataType.FLOAT
    elif value == b'\x04':
        return TSDataType.DOUBLE
    elif value == b'\x05':
        return TSDataType.TEXT


def get_data_type_byte_from_str(value):
    '''
    Args:
        value (str): data type in ['bool', 'int32', 'int64', 'float32', 'float64', 'text']
    Returns:
        byte: corresponding data type in [b'\x00', b'\x01', b'\x02', b'\x03', b'\x04', b'\x05']
    '''
    if value not in ['bool', 'int32', 'int64', 'float32', 'float64', 'text']:
        raise BadConfigValueError('data_type', value,
                                  "data_type should be in ['bool', 'int32', 'int64', 'float32', 'float64', 'text']")
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


# Read ColumnEncodings

def read_column_encoding(buffer, size):
    encodings = []
    for _ in range(size):
        res, buffer = read_byte_from_buffer(buffer)
        encodings.append(res)
    return encodings, buffer


# Read Column

def deserialize_null_indicators(buffer, size):
    may_have_null, buffer = read_byte_from_buffer(buffer)
    if may_have_null != b'\x00':
        return deserialize_from_boolean_array(buffer, size)
    return None, buffer


# Serialized data layout:
#    +---------------+-----------------+-------------+
#    | may have null | null indicators |   values    |
#    +---------------+-----------------+-------------+
#    | byte          | list[byte]      | list[int64] |
#    +---------------+-----------------+-------------+

def read_time_column(buffer, size):
    null_indicators, buffer = deserialize_null_indicators(buffer, size)
    if null_indicators is None:
        values, buffer = read_from_buffer(
            buffer, size * 8
        )
    else:
        raise Exception("TimeColumn should not contains null value")
    return values, buffer


def read_int64_column(buffer, data_type, position_count):
    null_indicators, buffer = deserialize_null_indicators(buffer, position_count)
    if null_indicators is None:
        size = position_count
    else:
        size = null_indicators.count(False)

    if TSDataType.INT64 == data_type or TSDataType.DOUBLE == data_type:
        values, buffer = read_from_buffer(buffer, size * 8)
        return values, null_indicators, buffer
    else:
        raise Exception("Invalid data type: " + data_type)


# Serialized data layout:
#    +---------------+-----------------+-------------+
#    | may have null | null indicators |   values    |
#    +---------------+-----------------+-------------+
#    | byte          | list[byte]      | list[int32] |
#    +---------------+-----------------+-------------+

def read_int32_column(buffer, data_type, position_count):
    null_indicators, buffer = deserialize_null_indicators(buffer, position_count)
    if null_indicators is None:
        size = position_count
    else:
        size = null_indicators.count(False)

    if TSDataType.INT32 == data_type or TSDataType.FLOAT == data_type:
        values, buffer = read_from_buffer(buffer, size * 4)
        return values, null_indicators, buffer
    else:
        raise Exception("Invalid data type: " + data_type)


# Serialized data layout:
#    +---------------+-----------------+-------------+
#    | may have null | null indicators |   values    |
#    +---------------+-----------------+-------------+
#    | byte          | list[byte]      | list[byte] |
#    +---------------+-----------------+-------------+

def read_byte_column(buffer, data_type, position_count):
    if data_type != TSDataType.BOOLEAN:
        raise Exception("Invalid data type: " + data_type)
    null_indicators, buffer = deserialize_null_indicators(buffer, position_count)
    res, buffer = deserialize_from_boolean_array(buffer, position_count)
    return res, null_indicators, buffer


def deserialize_from_boolean_array(buffer, size):
    packed_boolean_array, buffer = read_from_buffer(buffer, (size + 7) // 8)
    current_byte = 0
    output = [None] * size
    position = 0
    # read null bits 8 at a time
    while position < (size & ~0b111):
        value = packed_boolean_array[current_byte]
        output[position] = ((value & 0b1000_0000) != 0)
        output[position + 1] = ((value & 0b0100_0000) != 0)
        output[position + 2] = ((value & 0b0010_0000) != 0)
        output[position + 3] = ((value & 0b0001_0000) != 0)
        output[position + 4] = ((value & 0b0000_1000) != 0)
        output[position + 5] = ((value & 0b0000_0100) != 0)
        output[position + 6] = ((value & 0b0000_0010) != 0)
        output[position + 7] = ((value & 0b0000_0001) != 0)

        position += 8
        current_byte += 1
    # read last null bits
    if (size & 0b111) > 0:
        value = packed_boolean_array[-1]
        mask = 0b1000_0000
        position = size & ~0b111
        while position < size:
            output[position] = ((value & mask) != 0)
            mask >>= 1
            position += 1
    return output, buffer


# Serialized data layout:
#    +---------------+-----------------+-------------+
#    | may have null | null indicators |   values    |
#    +---------------+-----------------+-------------+
#    | byte          | list[byte]      | list[entry] |
#    +---------------+-----------------+-------------+
#
# Each entry is represented as:
#    +---------------+-------+
#    | value length  | value |
#    +---------------+-------+
#    | int32         | bytes |
#    +---------------+-------+

def read_binary_column(buffer, data_type, position_count):
    if data_type != TSDataType.TEXT:
        raise Exception("Invalid data type: " + data_type)
    null_indicators, buffer = deserialize_null_indicators(buffer, position_count)

    if null_indicators is None:
        size = position_count
    else:
        size = null_indicators.count(False)
    values = [None] * size
    for i in range(size):
        length, buffer = read_int_from_buffer(buffer)
        res, buffer = read_from_buffer(buffer, length)
        values[i] = res
    return values, null_indicators, buffer


def read_column(encoding, buffer, data_type, position_count):
    if encoding == b'\x00':
        return read_byte_column(buffer, data_type, position_count)
    elif encoding == b'\x01':
        return read_int32_column(buffer, data_type, position_count)
    elif encoding == b'\x02':
        return read_int64_column(buffer, data_type, position_count)
    elif encoding == b'\x03':
        return read_binary_column(buffer, data_type, position_count)
    elif encoding == b'\x04':
        return read_run_length_column(buffer, data_type, position_count)
    else:
        raise Exception("Unsupported encoding: " + encoding)


# Serialized data layout:
#    +-----------+-------------------------+
#    | encoding  | serialized inner column |
#    +-----------+-------------------------+
#    | byte      | list[byte]              |
#    +-----------+-------------------------+

def read_run_length_column(buffer, data_type, position_count):
    encoding, buffer = read_byte_from_buffer(buffer)
    column, null_indicators, buffer = read_column(encoding, buffer, data_type, 1)

    return repeat(column, data_type, position_count), null_indicators * position_count, buffer


def repeat(buffer, data_type, position_count):
    if data_type == TSDataType.BOOLEAN or data_type == TSDataType.TEXT:
        return buffer * position_count
    else:
        res = bytes(0)
        for _ in range(position_count):
            res.join(buffer)
        return res
