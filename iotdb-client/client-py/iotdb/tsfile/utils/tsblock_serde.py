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

import numpy as np


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

    time_column_values, _, buffer = read_column(
        column_encodings[0], buffer, 2, position_count
    )
    column_values = []
    null_indicators = []
    for i in range(value_column_count):
        column_value, null_indicator, buffer = read_column(
            column_encodings[i + 1], buffer, data_types[i], position_count
        )
        column_values.append(column_value)
        null_indicators.append(null_indicator)

    return time_column_values, column_values, null_indicators, position_count


# General Methods


def read_int_from_buffer(buffer):
    res = np.frombuffer(buffer, dtype=">i4", count=1)
    buffer = buffer[4:]
    return res[0], buffer


def read_byte_from_buffer(buffer):
    return read_from_buffer(buffer, 1)


def read_from_buffer(buffer, size):
    res = buffer[:size]
    new_buffer = buffer[size:]
    return res, new_buffer


# Read ColumnType


def read_column_types(buffer, value_column_count):
    data_types = np.frombuffer(buffer, dtype=np.uint8, count=value_column_count)
    new_buffer = buffer[value_column_count:]
    if not np.all(np.isin(data_types, (0, 1, 2, 3, 4, 5, 8, 9, 10, 11))):
        raise Exception("Invalid data type encountered: " + str(data_types))
    return data_types, new_buffer


# Read ColumnEncodings


def read_column_encoding(buffer, size):
    encodings = np.frombuffer(buffer, dtype=np.uint8, count=size)
    new_buffer = buffer[size:]
    return encodings, new_buffer


# Read Column


def deserialize_null_indicators(buffer, size):
    may_have_null = buffer[0]
    buffer = buffer[1:]
    if may_have_null != 0:
        return deserialize_from_boolean_array(buffer, size)
    return None, buffer


# Serialized data layout:
#    +---------------+-----------------+-------------+
#    | may have null | null indicators |   values    |
#    +---------------+-----------------+-------------+
#    | byte          | list[byte]      | list[int64] |
#    +---------------+-----------------+-------------+


def read_int64_column(buffer, data_type, position_count):
    null_indicators, buffer = deserialize_null_indicators(buffer, position_count)
    if null_indicators is None:
        size = position_count
    else:
        size = np.count_nonzero(~null_indicators)

    if data_type == 2:
        dtype = ">i8"
    elif data_type == 4:
        dtype = ">f8"
    else:
        raise Exception("Invalid data type: " + str(data_type))
    values = np.frombuffer(buffer, dtype, count=size)
    buffer = buffer[size * 8 :]
    return values, null_indicators, buffer


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
        size = np.count_nonzero(~null_indicators)

    if (data_type == 1) or (data_type == 9):
        dtype = ">i4"
    elif data_type == 3:
        dtype = ">f4"
    else:
        raise Exception("Invalid data type: " + str(data_type))
    values = np.frombuffer(buffer, dtype, count=size)
    buffer = buffer[size * 4 :]
    return values, null_indicators, buffer


# Serialized data layout:
#    +---------------+-----------------+-------------+
#    | may have null | null indicators |   values    |
#    +---------------+-----------------+-------------+
#    | byte          | list[byte]      | list[byte] |
#    +---------------+-----------------+-------------+


def read_byte_column(buffer, data_type, position_count):
    if data_type != 0:
        raise Exception("Invalid data type: " + data_type)
    null_indicators, buffer = deserialize_null_indicators(buffer, position_count)
    res, buffer = deserialize_from_boolean_array(buffer, position_count)
    return res, null_indicators, buffer


def deserialize_from_boolean_array(buffer, size):
    num_bytes = (size + 7) // 8
    packed_boolean_array, buffer = read_from_buffer(buffer, num_bytes)
    arr = np.frombuffer(packed_boolean_array, dtype=np.uint8)
    output = np.unpackbits(arr)[:size].astype(bool)
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
    if data_type != 5:
        raise Exception("Invalid data type: " + data_type)
    null_indicators, buffer = deserialize_null_indicators(buffer, position_count)

    if null_indicators is None:
        size = position_count
    else:
        size = np.count_nonzero(~null_indicators)
    values = np.empty(size, dtype=object)
    for i in range(size):
        length, buffer = read_int_from_buffer(buffer)
        res, buffer = read_from_buffer(buffer, length)
        values[i] = res.tobytes()
    return values, null_indicators, buffer


# Serialized data layout:
#    +-----------+-------------------------+
#    | encoding  | serialized inner column |
#    +-----------+-------------------------+
#    | byte      | list[byte]              |
#    +-----------+-------------------------+


def read_run_length_column(buffer, data_type, position_count):
    encoding, buffer = read_byte_from_buffer(buffer)
    column, null_indicators, buffer = read_column(encoding[0], buffer, data_type, 1)
    return (
        repeat(column, data_type, position_count),
        (
            None
            if null_indicators is None
            else np.full(position_count, null_indicators[0])
        ),
        buffer,
    )


def repeat(column, data_type, position_count):
    if data_type in (0, 5):
        if column.size == 1:
            return np.full(
                position_count, column[0], dtype=(bool if data_type == 0 else object)
            )
        else:
            return np.array(column * position_count, dtype=object)
    else:
        res = bytearray()
        for _ in range(position_count):
            res.extend(column if isinstance(column, bytes) else bytes(column))
        return bytes(res)


def read_dictionary_column(buffer, data_type, position_count):
    raise Exception("dictionary column not implemented")


ENCODING_FUNC_MAP = {
    0: read_byte_column,
    1: read_int32_column,
    2: read_int64_column,
    3: read_binary_column,
    4: read_run_length_column,
    5: read_dictionary_column,
}


def read_column(encoding, buffer, data_type, position_count):
    try:
        func = ENCODING_FUNC_MAP[encoding]
    except KeyError:
        raise Exception("Unsupported encoding: " + str(encoding))
    return func(buffer, data_type, position_count)
