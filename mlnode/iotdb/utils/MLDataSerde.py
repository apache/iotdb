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

from iotdb.utils.IoTDBConstants import TSDataType


# Serialized tsblock:
#    +-------------+---------------+---------+------------+-----------+----------+
#    | val col cnt | val col types | pos cnt | encodings  | time col  | val col  |
#    +-------------+---------------+---------+------------+-----------+----------+
#    | int32       | list[byte]    | int32   | list[byte] |  bytes    | byte     |
#    +-------------+---------------+---------+------------+-----------+----------+

def deserialize(buffer):
    value_column_count, buffer = readIntFromBuffer(buffer)
    data_types, buffer = readColumnTypes(buffer, value_column_count)

    position_count, buffer = readIntFromBuffer(buffer)
    column_encodings, buffer = readColumnEncoding(buffer, value_column_count + 1)

    time_column_values, buffer = readTimeColumn(buffer, position_count)
    column_values = [None] * value_column_count
    null_indicators = [None] * value_column_count
    for i in range(value_column_count):
        column_value, nullIndicator, buffer = readColumn(column_encodings[i + 1], buffer, data_types[i], position_count)
        column_values[i] = column_value
        null_indicators[i] = nullIndicator

    return time_column_values, column_values, null_indicators, position_count, buffer


# General Methods

def readIntFromBuffer(buffer):
    res, buffer = readFromBuffer(buffer, 4)
    return int.from_bytes(res, "big"), buffer


def readByteFromBuffer(buffer):
    return readFromBuffer(buffer, 1)


def readFromBuffer(buffer, size):
    res = buffer[:size]
    buffer = buffer[size:]
    return res, buffer


# Read ColumnType

def readColumnTypes(buffer, value_column_count):
    data_types = []
    for i in range(value_column_count):
        res, buffer = readByteFromBuffer(buffer)
        data_types.append(getDataType(res))
    return data_types, buffer


def getDataType(value):
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
    elif value == b'\x06':
        return TSDataType.VECTOR


# Read ColumnEncodings

def readColumnEncoding(buffer, size):
    encodings = []
    for i in range(size):
        res, buffer = readByteFromBuffer(buffer)
        encodings.append(res)
    return encodings, buffer


# Read Column

def deserializeNullIndicators(buffer, size):
    mayHaveNull, buffer = readByteFromBuffer(buffer)
    if mayHaveNull != b'\x00':
        return deserializeFromBooleanArray(buffer, size)
    return None, buffer


# Serialized data layout:
#    +---------------+-----------------+-------------+
#    | may have null | null indicators |   values    |
#    +---------------+-----------------+-------------+
#    | byte          | list[byte]      | list[int64] |
#    +---------------+-----------------+-------------+

def readTimeColumn(buffer, size):
    nullIndicators, buffer = deserializeNullIndicators(buffer, size)
    if nullIndicators is None:
        values, buffer = readFromBuffer(
            buffer, size * 8
        )
    else:
        raise Exception("TimeColumn should not contains null value")
    return values, buffer


def readINT64Column(buffer, data_type, position_count):
    nullIndicators, buffer = deserializeNullIndicators(buffer, position_count)
    if nullIndicators is None:
        size = position_count
    else:
        size = nullIndicators.count(False)

    if TSDataType.INT64 == data_type or TSDataType.DOUBLE == data_type:
        values, buffer = readFromBuffer(buffer, size * 8)
        return values, nullIndicators, buffer
    else:
        raise Exception("Invalid data type: " + data_type)


# Serialized data layout:
#    +---------------+-----------------+-------------+
#    | may have null | null indicators |   values    |
#    +---------------+-----------------+-------------+
#    | byte          | list[byte]      | list[int32] |
#    +---------------+-----------------+-------------+

def readInt32Column(buffer, data_type, position_count):
    nullIndicators, buffer = deserializeNullIndicators(buffer, position_count)
    if nullIndicators is None:
        size = position_count
    else:
        size = nullIndicators.count(False)

    if TSDataType.INT32 == data_type or TSDataType.FLOAT == data_type:
        values, buffer = readFromBuffer(buffer, size * 4)
        return values, nullIndicators, buffer
    else:
        raise Exception("Invalid data type: " + data_type)


# Serialized data layout:
#    +---------------+-----------------+-------------+
#    | may have null | null indicators |   values    |
#    +---------------+-----------------+-------------+
#    | byte          | list[byte]      | list[byte] |
#    +---------------+-----------------+-------------+

def readByteColumn(buffer, data_type, position_count):
    if data_type != TSDataType.BOOLEAN:
        raise Exception("Invalid data type: " + data_type)
    nullIndicators, buffer = deserializeNullIndicators(buffer, position_count)
    res, buffer = deserializeFromBooleanArray(buffer, position_count)
    return res, nullIndicators, buffer


def deserializeFromBooleanArray(buffer, size):
    packedBooleanArray, buffer = readFromBuffer(buffer, (size + 7) // 8)
    currentByte = 0
    output = [None] * size
    position = 0
    # read null bits 8 at a time
    while position < (size & ~0b111):
        value = packedBooleanArray[currentByte]
        output[position] = ((value & 0b1000_0000) != 0)
        output[position + 1] = ((value & 0b0100_0000) != 0)
        output[position + 2] = ((value & 0b0010_0000) != 0)
        output[position + 3] = ((value & 0b0001_0000) != 0)
        output[position + 4] = ((value & 0b0000_1000) != 0)
        output[position + 5] = ((value & 0b0000_0100) != 0)
        output[position + 6] = ((value & 0b0000_0010) != 0)
        output[position + 7] = ((value & 0b0000_0001) != 0)

        position += 8
        currentByte += 1
    # read last null bits
    if (size & 0b111) > 0:
        value = packedBooleanArray[-1]
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

def readBinaryColumn(buffer, data_type, position_count):
    if data_type != TSDataType.TEXT:
        raise Exception("Invalid data type: " + data_type)
    nullIndicators, buffer = deserializeNullIndicators(buffer, position_count)

    if nullIndicators is None:
        size = position_count
    else:
        size = nullIndicators.count(False)
    values = [None] * size
    for i in range(size):
        length, buffer = readIntFromBuffer(buffer)
        res, buffer = readFromBuffer(buffer, length)
        values[i] = res
    return values, nullIndicators, buffer


def readColumn(encoding, buffer, data_type, position_count):
    if encoding == b'\x00':
        return readByteColumn(buffer, data_type, position_count)
    elif encoding == b'\x01':
        return readInt32Column(buffer, data_type, position_count)
    elif encoding == b'\x02':
        return readINT64Column(buffer, data_type, position_count)
    elif encoding == b'\x03':
        return readBinaryColumn(buffer, data_type, position_count)
    elif encoding == b'\x04':
        return readRunLengthColumn(buffer, data_type, position_count)
    else:
        raise Exception("Unsupported encoding: " + encoding)


# Serialized data layout:
#    +-----------+-------------------------+
#    | encoding  | serialized inner column |
#    +-----------+-------------------------+
#    | byte      | list[byte]              |
#    +-----------+-------------------------+

def readRunLengthColumn(buffer, data_type, position_count):
    encoding, buffer = readByteFromBuffer(buffer)
    column, nullIndicators, buffer = readColumn(encoding, buffer, data_type, 1)

    return repeat(column, data_type, position_count), nullIndicators * position_count, buffer

def repeat(buffer, data_type, position_count):
    if data_type == TSDataType.BOOLEAN or data_type == TSDataType.TEXT:
        return buffer * position_count
    else:
        res = bytes(0)
        for _ in range(position_count):
            res.join(buffer)
        return res
