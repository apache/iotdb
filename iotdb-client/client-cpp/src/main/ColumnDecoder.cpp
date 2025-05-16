/**
* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "ColumnDecoder.h"

#include "Column.h"

std::vector<bool> deserializeNullIndicators(MyStringBuffer& buffer, int32_t positionCount) {
    uint8_t mayHaveNullByte = buffer.getChar();

    bool mayHaveNull = mayHaveNullByte != 0;
    if (!mayHaveNull) {
        return {};
    }

    return deserializeBooleanArray(buffer, positionCount);
}

std::vector<bool> deserializeBooleanArray(MyStringBuffer& buffer, int32_t size) {
    const int32_t packedSize = (size + 7) / 8;
    std::vector<uint8_t> packedBytes(packedSize);
    for (int i = 0; i < packedSize; i++) {
        packedBytes[i] = buffer.getChar();
    }

    std::vector<bool> output(size);
    int currentByte = 0;
    const int fullGroups = size & ~0b111;

    for (int pos = 0; pos < fullGroups; pos += 8) {
        const uint8_t b = packedBytes[currentByte++];
        output[pos + 0] = (b & 0b10000000) != 0;
        output[pos + 1] = (b & 0b01000000) != 0;
        output[pos + 2] = (b & 0b00100000) != 0;
        output[pos + 3] = (b & 0b00010000) != 0;
        output[pos + 4] = (b & 0b00001000) != 0;
        output[pos + 5] = (b & 0b00000100) != 0;
        output[pos + 6] = (b & 0b00000010) != 0;
        output[pos + 7] = (b & 0b00000001) != 0;
    }

    if ((size & 0b111) > 0) {
        const uint8_t b = packedBytes.back();
        uint8_t mask = 0b10000000;

        for (int pos = fullGroups; pos < size; pos++) {
            output[pos] = (b & mask) != 0;
            mask >>= 1;
        }
    }

    return output;
}

std::unique_ptr<Column> BaseColumnDecoder::readColumn(
    MyStringBuffer& buffer, TSDataType::TSDataType dataType, int32_t positionCount) {
    return nullptr;
}

std::unique_ptr<Column> Int32ArrayColumnDecoder::readColumn(
    MyStringBuffer& buffer, TSDataType::TSDataType dataType, int32_t positionCount) {
    auto nullIndicators = deserializeNullIndicators(buffer, positionCount);

    switch (dataType) {
    case TSDataType::INT32:
    case TSDataType::DATE: {
        std::vector<int32_t> intValues(positionCount);
        for (int32_t i = 0; i < positionCount; i++) {
            if (!nullIndicators.empty() && nullIndicators[i]) continue;
            intValues[i] = buffer.getInt();
        }
        return std::unique_ptr<IntColumn>(new IntColumn(0, positionCount, nullIndicators, intValues));
    }
    case TSDataType::FLOAT: {
        std::vector<float> floatValues(positionCount);
        for (int32_t i = 0; i < positionCount; i++) {
            if (!nullIndicators.empty() && nullIndicators[i]) continue;
            floatValues[i] = buffer.getFloat();
        }
        return std::unique_ptr<FloatColumn>(new FloatColumn(0, positionCount, nullIndicators, floatValues));
    }
    default:
        throw IoTDBException("Invalid data type for Int32ArrayColumnDecoder");
    }
}

std::unique_ptr<Column> Int64ArrayColumnDecoder::readColumn(
    MyStringBuffer& buffer, TSDataType::TSDataType dataType, int32_t positionCount) {
    auto nullIndicators = deserializeNullIndicators(buffer, positionCount);

    switch (dataType) {
    case TSDataType::INT64:
    case TSDataType::TIMESTAMP: {
        std::vector<int64_t> values(positionCount);
        for (int32_t i = 0; i < positionCount; i++) {
            if (!nullIndicators.empty() && nullIndicators[i]) continue;
            values[i] = buffer.getInt64();
        }
        return std::unique_ptr<LongColumn>(new LongColumn(0, positionCount, nullIndicators, values));
    }
    case TSDataType::DOUBLE: {
        std::vector<double> values(positionCount);
        for (int32_t i = 0; i < positionCount; i++) {
            if (!nullIndicators.empty() && nullIndicators[i]) continue;
            values[i] = buffer.getDouble();
        }
        return std::unique_ptr<DoubleColumn>(new DoubleColumn(0, positionCount, nullIndicators, values));
    }
    default:
        throw IoTDBException("Invalid data type for Int64ArrayColumnDecoder");
    }
}

std::unique_ptr<Column> ByteArrayColumnDecoder::readColumn(
    MyStringBuffer& buffer, TSDataType::TSDataType dataType, int32_t positionCount) {
    if (dataType != TSDataType::BOOLEAN) {
        throw IoTDBException("Invalid data type for ByteArrayColumnDecoder");
    }

    auto nullIndicators = deserializeNullIndicators(buffer, positionCount);
    auto values = deserializeBooleanArray(buffer, positionCount);
    return std::unique_ptr<BooleanColumn>(new BooleanColumn(0, positionCount, nullIndicators, values));
}

std::unique_ptr<Column> BinaryArrayColumnDecoder::readColumn(
    MyStringBuffer& buffer, TSDataType::TSDataType dataType, int32_t positionCount) {
    if (dataType != TSDataType::TEXT) {
        throw IoTDBException("Invalid data type for BinaryArrayColumnDecoder");
    }

    auto nullIndicators = deserializeNullIndicators(buffer, positionCount);
    std::vector<std::shared_ptr<Binary>> values(positionCount);

    for (int32_t i = 0; i < positionCount; i++) {
        if (!nullIndicators.empty() && nullIndicators[i]) continue;

        int32_t length = buffer.getInt();

        std::vector<uint8_t> value(length);
        for (int32_t j = 0; j < length; j++) {
            value[j] = buffer.getChar();
        }

        values[i] = std::make_shared<Binary>(value);
    }

    return std::unique_ptr<BinaryColumn>(new BinaryColumn(0, positionCount, nullIndicators, values));
}

std::unique_ptr<Column> RunLengthColumnDecoder::readColumn(
    MyStringBuffer& buffer, TSDataType::TSDataType dataType, int32_t positionCount) {
    uint8_t encodingByte = buffer.getChar();

    auto columnEncoding = static_cast<ColumnEncoding>(encodingByte);
    auto decoder = getColumnDecoder(columnEncoding);

    auto column = decoder->readColumn(buffer, dataType, 1);
    if (!column) {
        throw IoTDBException("Failed to read inner column");
    }
    return std::unique_ptr<RunLengthEncodedColumn>(new RunLengthEncodedColumn(move(column), positionCount));
}
