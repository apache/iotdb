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
#include <stdexcept>
#include <algorithm>
#include "TsBlock.h"

std::shared_ptr<TsBlock> TsBlock::create(int32_t positionCount,
                                         std::shared_ptr<Column> timeColumn,
                                         std::vector<std::shared_ptr<Column>> valueColumns) {
    if (valueColumns.empty()) {
        throw std::invalid_argument("valueColumns cannot be empty");
    }
    return std::shared_ptr<TsBlock>(new TsBlock(positionCount, std::move(timeColumn), std::move(valueColumns)));
}

std::shared_ptr<TsBlock> TsBlock::deserialize(const std::string& data) {
    MyStringBuffer buffer(data);

    // Read value column count
    int32_t valueColumnCount = buffer.getInt();

    // Read value column data types
    std::vector<TSDataType::TSDataType> valueColumnDataTypes(valueColumnCount);
    for (int32_t i = 0; i < valueColumnCount; i++) {
        valueColumnDataTypes[i] = static_cast<TSDataType::TSDataType>(buffer.getChar());
    }

    // Read position count
    int32_t positionCount = buffer.getInt();

    // Read column encodings
    std::vector<ColumnEncoding> columnEncodings(valueColumnCount + 1);
    for (int32_t i = 0; i < valueColumnCount + 1; i++) {
        columnEncodings[i] = static_cast<ColumnEncoding>(buffer.getChar());
    }

    // Read time column
    auto timeColumnDecoder = getColumnDecoder(columnEncodings[0]);
    auto timeColumn = timeColumnDecoder->readColumn(buffer, TSDataType::INT64, positionCount);

    // Read value columns
    std::vector<std::shared_ptr<Column>> valueColumns(valueColumnCount);
    for (int32_t i = 0; i < valueColumnCount; i++) {
        auto valueColumnDecoder = getColumnDecoder(columnEncodings[i + 1]);
        valueColumns[i] = valueColumnDecoder->readColumn(buffer, valueColumnDataTypes[i], positionCount);
    }

    return create(positionCount, std::move(timeColumn), std::move(valueColumns));
}

TsBlock::TsBlock(int32_t positionCount,
                 std::shared_ptr<Column> timeColumn,
                 std::vector<std::shared_ptr<Column>> valueColumns)
    : positionCount_(positionCount),
      timeColumn_(std::move(timeColumn)),
      valueColumns_(std::move(valueColumns)) {
}

int32_t TsBlock::getPositionCount() const {
    return positionCount_;
}

int64_t TsBlock::getStartTime() const {
    return timeColumn_->getLong(0);
}

int64_t TsBlock::getEndTime() const {
    return timeColumn_->getLong(positionCount_ - 1);
}

bool TsBlock::isEmpty() const {
    return positionCount_ == 0;
}

int64_t TsBlock::getTimeByIndex(int32_t index) const {
    return timeColumn_->getLong(index);
}

int32_t TsBlock::getValueColumnCount() const {
    return static_cast<int32_t>(valueColumns_.size());
}

const std::shared_ptr<Column> TsBlock::getTimeColumn() const {
    return timeColumn_;
}

const std::vector<std::shared_ptr<Column>>& TsBlock::getValueColumns() const {
    return valueColumns_;
}

const std::shared_ptr<Column> TsBlock::getColumn(int32_t columnIndex) const {
    return valueColumns_[columnIndex];
}
