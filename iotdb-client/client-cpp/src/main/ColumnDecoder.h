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
#ifndef IOTDB_COLUMN_DECODER_H
#define IOTDB_COLUMN_DECODER_H

#include <vector>
#include <memory>

#include "Common.h"

class Column;

class ColumnDecoder {
public:
    virtual ~ColumnDecoder() = default;
    virtual std::unique_ptr<Column> readColumn(
        MyStringBuffer& buffer, TSDataType::TSDataType dataType, int32_t positionCount) = 0;
};

std::vector<bool> deserializeNullIndicators(MyStringBuffer& buffer, int32_t positionCount);
std::vector<bool> deserializeBooleanArray(MyStringBuffer& buffer, int32_t size);

class BaseColumnDecoder : public ColumnDecoder {
public:
    std::unique_ptr<Column> readColumn(
        MyStringBuffer& buffer, TSDataType::TSDataType dataType, int32_t positionCount) override;
};

class Int32ArrayColumnDecoder : public BaseColumnDecoder {
public:
    std::unique_ptr<Column> readColumn(
        MyStringBuffer& buffer, TSDataType::TSDataType dataType, int32_t positionCount) override;
};

class Int64ArrayColumnDecoder : public BaseColumnDecoder {
public:
    std::unique_ptr<Column> readColumn(
        MyStringBuffer& buffer, TSDataType::TSDataType dataType, int32_t positionCount) override;
};

class ByteArrayColumnDecoder : public BaseColumnDecoder {
public:
    std::unique_ptr<Column> readColumn(
        MyStringBuffer& buffer, TSDataType::TSDataType dataType, int32_t positionCount) override;
};

class BinaryArrayColumnDecoder : public BaseColumnDecoder {
public:
    std::unique_ptr<Column> readColumn(
        MyStringBuffer& buffer, TSDataType::TSDataType dataType, int32_t positionCount) override;
};

class RunLengthColumnDecoder : public BaseColumnDecoder {
public:
    std::unique_ptr<Column> readColumn(
        MyStringBuffer& buffer, TSDataType::TSDataType dataType, int32_t positionCount) override;
};

#endif
