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

#include "Column.h"
#include "ColumnDecoder.h"

TimeColumn::TimeColumn(int32_t arrayOffset, int32_t positionCount, const std::vector<int64_t>& values)
    : arrayOffset_(arrayOffset), positionCount_(positionCount), values_(values) {
    if (arrayOffset < 0) throw IoTDBException("arrayOffset is negative");
    if (positionCount < 0) throw IoTDBException("positionCount is negative");
    if (static_cast<int32_t>(values.size()) - arrayOffset < positionCount) {
        throw IoTDBException("values length is less than positionCount");
    }
}

TSDataType::TSDataType TimeColumn::getDataType() const { return TSDataType::INT64; }
ColumnEncoding TimeColumn::getEncoding() const { return ColumnEncoding::Int64Array; }

int64_t TimeColumn::getLong(int32_t position) const {
    return values_[position + arrayOffset_];
}

bool TimeColumn::mayHaveNull() const { return false; }
bool TimeColumn::isNull(int32_t position) const { return false; }
std::vector<bool> TimeColumn::isNulls() const { return {}; }

int32_t TimeColumn::getPositionCount() const { return positionCount_; }

int64_t TimeColumn::getStartTime() const { return values_[arrayOffset_]; }
int64_t TimeColumn::getEndTime() const { return values_[positionCount_ + arrayOffset_ - 1]; }

const std::vector<int64_t>& TimeColumn::getTimes() const { return values_; }
std::vector<int64_t> TimeColumn::getLongs() const { return getTimes(); }

BinaryColumn::BinaryColumn(int32_t arrayOffset, int32_t positionCount,
                           const std::vector<bool>& valueIsNull, const std::vector<std::shared_ptr<Binary>>& values)
    : arrayOffset_(arrayOffset), positionCount_(positionCount),
      valueIsNull_(valueIsNull), values_(values) {
    if (arrayOffset < 0) throw IoTDBException("arrayOffset is negative");
    if (positionCount < 0) throw IoTDBException("positionCount is negative");
    if (static_cast<int32_t>(values.size()) - arrayOffset < positionCount) {
        throw IoTDBException("values length is less than positionCount");
    }
    if (!valueIsNull.empty() && static_cast<int32_t>(valueIsNull.size()) - arrayOffset < positionCount) {
        throw IoTDBException("isNull length is less than positionCount");
    }
}

TSDataType::TSDataType BinaryColumn::getDataType() const { return TSDataType::TSDataType::TEXT; }
ColumnEncoding BinaryColumn::getEncoding() const { return ColumnEncoding::BinaryArray; }

std::shared_ptr<Binary> BinaryColumn::getBinary(int32_t position) const {
    return values_[position + arrayOffset_];
}

std::vector<std::shared_ptr<Binary>> BinaryColumn::getBinaries() const { return values_; }


bool BinaryColumn::mayHaveNull() const { return !valueIsNull_.empty(); }

bool BinaryColumn::isNull(int32_t position) const {
    return !valueIsNull_.empty() && valueIsNull_[position + arrayOffset_];
}

std::vector<bool> BinaryColumn::isNulls() const {
    if (!valueIsNull_.empty()) return valueIsNull_;

    std::vector<bool> result(positionCount_, false);
    return result;
}

int32_t BinaryColumn::getPositionCount() const { return positionCount_; }

IntColumn::IntColumn(int32_t arrayOffset, int32_t positionCount,
                     const std::vector<bool>& valueIsNull, const std::vector<int32_t>& values)
    : arrayOffset_(arrayOffset), positionCount_(positionCount),
      valueNull_(valueIsNull), values_(values) {
    if (arrayOffset < 0) throw IoTDBException("arrayOffset is negative");
    if (positionCount < 0) throw IoTDBException("positionCount is negative");
    if (static_cast<int32_t>(values.size()) - arrayOffset < positionCount) {
        throw IoTDBException("values length is less than positionCount");
    }
    if (!valueIsNull.empty() && static_cast<int32_t>(valueIsNull.size()) - arrayOffset < positionCount) {
        throw IoTDBException("isNull length is less than positionCount");
    }
}

TSDataType::TSDataType IntColumn::getDataType() const { return TSDataType::INT32; }
ColumnEncoding IntColumn::getEncoding() const { return ColumnEncoding::Int32Array; }

int32_t IntColumn::getInt(int32_t position) const {
    return values_[position + arrayOffset_];
}

std::vector<int32_t> IntColumn::getInts() const { return values_; }

bool IntColumn::mayHaveNull() const { return !valueNull_.empty(); }

bool IntColumn::isNull(int32_t position) const {
    return !valueNull_.empty() && valueNull_[position + arrayOffset_];
}

std::vector<bool> IntColumn::isNulls() const {
    if (!valueNull_.empty()) return valueNull_;

    std::vector<bool> result(positionCount_, false);
    return result;
}

int32_t IntColumn::getPositionCount() const { return positionCount_; }

FloatColumn::FloatColumn(int32_t arrayOffset, int32_t positionCount,
                         const std::vector<bool>& valueIsNull, const std::vector<float>& values)
    : arrayOffset_(arrayOffset), positionCount_(positionCount),
      valueIsNull_(valueIsNull), values_(values) {
    if (arrayOffset < 0) throw IoTDBException("arrayOffset is negative");
    if (positionCount < 0) throw IoTDBException("positionCount is negative");
    if (static_cast<int32_t>(values.size()) - arrayOffset < positionCount) {
        throw IoTDBException("values length is less than positionCount");
    }
    if (!valueIsNull.empty() && static_cast<int32_t>(valueIsNull.size()) - arrayOffset < positionCount) {
        throw IoTDBException("isNull length is less than positionCount");
    }
}

TSDataType::TSDataType FloatColumn::getDataType() const { return TSDataType::TSDataType::FLOAT; }
ColumnEncoding FloatColumn::getEncoding() const { return ColumnEncoding::Int32Array; }

float FloatColumn::getFloat(int32_t position) const {
    return values_[position + arrayOffset_];
}

std::vector<float> FloatColumn::getFloats() const { return values_; }

bool FloatColumn::mayHaveNull() const { return !valueIsNull_.empty(); }

bool FloatColumn::isNull(int32_t position) const {
    return !valueIsNull_.empty() && valueIsNull_[position + arrayOffset_];
}

std::vector<bool> FloatColumn::isNulls() const {
    if (!valueIsNull_.empty()) return valueIsNull_;

    std::vector<bool> result(positionCount_, false);
    return result;
}

int32_t FloatColumn::getPositionCount() const { return positionCount_; }

LongColumn::LongColumn(int32_t arrayOffset, int32_t positionCount,
                       const std::vector<bool>& valueIsNull, const std::vector<int64_t>& values)
    : arrayOffset_(arrayOffset), positionCount_(positionCount),
      valueIsNull_(valueIsNull), values_(values) {
    if (arrayOffset < 0) throw IoTDBException("arrayOffset is negative");
    if (positionCount < 0) throw IoTDBException("positionCount is negative");
    if (static_cast<int32_t>(values.size()) - arrayOffset < positionCount) {
        throw IoTDBException("values length is less than positionCount");
    }
    if (!valueIsNull.empty() && static_cast<int32_t>(valueIsNull.size()) - arrayOffset < positionCount) {
        throw IoTDBException("isNull length is less than positionCount");
    }
}

TSDataType::TSDataType LongColumn::getDataType() const { return TSDataType::TSDataType::INT64; }
ColumnEncoding LongColumn::getEncoding() const { return ColumnEncoding::Int64Array; }

int64_t LongColumn::getLong(int32_t position) const {
    return values_[position + arrayOffset_];
}

std::vector<int64_t> LongColumn::getLongs() const { return values_; }

bool LongColumn::mayHaveNull() const { return !valueIsNull_.empty(); }

bool LongColumn::isNull(int32_t position) const {
    return !valueIsNull_.empty() && valueIsNull_[position + arrayOffset_];
}

std::vector<bool> LongColumn::isNulls() const {
    if (!valueIsNull_.empty()) return valueIsNull_;

    std::vector<bool> result(positionCount_, false);
    return result;
}

int32_t LongColumn::getPositionCount() const { return positionCount_; }

DoubleColumn::DoubleColumn(int32_t arrayOffset, int32_t positionCount,
                           const std::vector<bool>& valueIsNull, const std::vector<double>& values)
    : arrayOffset_(arrayOffset), positionCount_(positionCount),
      valueIsNull_(valueIsNull), values_(values) {
    if (arrayOffset < 0) throw IoTDBException("arrayOffset is negative");
    if (positionCount < 0) throw IoTDBException("positionCount is negative");
    if (static_cast<int32_t>(values.size()) - arrayOffset < positionCount) {
        throw IoTDBException("values length is less than positionCount");
    }
    if (!valueIsNull.empty() && static_cast<int32_t>(valueIsNull.size()) - arrayOffset < positionCount) {
        throw IoTDBException("isNull length is less than positionCount");
    }
}

TSDataType::TSDataType DoubleColumn::getDataType() const { return TSDataType::TSDataType::DOUBLE; }
ColumnEncoding DoubleColumn::getEncoding() const { return ColumnEncoding::Int64Array; }

double DoubleColumn::getDouble(int32_t position) const {
    return values_[position + arrayOffset_];
}

std::vector<double> DoubleColumn::getDoubles() const { return values_; }

bool DoubleColumn::mayHaveNull() const { return !valueIsNull_.empty(); }

bool DoubleColumn::isNull(int32_t position) const {
    return !valueIsNull_.empty() && valueIsNull_[position + arrayOffset_];
}

std::vector<bool> DoubleColumn::isNulls() const {
    if (!valueIsNull_.empty()) return valueIsNull_;

    std::vector<bool> result(positionCount_, false);
    return result;
}

int32_t DoubleColumn::getPositionCount() const { return positionCount_; }

BooleanColumn::BooleanColumn(int32_t arrayOffset, int32_t positionCount,
                             const std::vector<bool>& valueIsNull, const std::vector<bool>& values)
    : arrayOffset_(arrayOffset), positionCount_(positionCount),
      valueIsNull_(valueIsNull), values_(values) {
    if (arrayOffset < 0) throw IoTDBException("arrayOffset is negative");
    if (positionCount < 0) throw IoTDBException("positionCount is negative");
    if (static_cast<int32_t>(values.size()) - arrayOffset < positionCount) {
        throw IoTDBException("values length is less than positionCount");
    }
    if (!valueIsNull.empty() && static_cast<int32_t>(valueIsNull.size()) - arrayOffset < positionCount) {
        throw IoTDBException("isNull length is less than positionCount");
    }
}

TSDataType::TSDataType BooleanColumn::getDataType() const { return TSDataType::TSDataType::BOOLEAN; }
ColumnEncoding BooleanColumn::getEncoding() const { return ColumnEncoding::ByteArray; }

bool BooleanColumn::getBoolean(int32_t position) const {
    return values_[position + arrayOffset_];
}

std::vector<bool> BooleanColumn::getBooleans() const { return values_; }

bool BooleanColumn::mayHaveNull() const { return !valueIsNull_.empty(); }

bool BooleanColumn::isNull(int32_t position) const {
    return !valueIsNull_.empty() && valueIsNull_[position + arrayOffset_];
}

std::vector<bool> BooleanColumn::isNulls() const {
    if (!valueIsNull_.empty()) return valueIsNull_;

    std::vector<bool> result(positionCount_, false);
    return result;
}

int32_t BooleanColumn::getPositionCount() const { return positionCount_; }

RunLengthEncodedColumn::RunLengthEncodedColumn(std::shared_ptr<Column> value, int32_t positionCount)
    : value_(value), positionCount_(positionCount) {
    if (!value) throw IoTDBException("value is null");
    if (value->getPositionCount() != 1) {
        throw IoTDBException("Expected value to contain a single position");
    }
    if (positionCount < 0) throw IoTDBException("positionCount is negative");
}

std::shared_ptr<Column> RunLengthEncodedColumn::getValue() const { return value_; }

TSDataType::TSDataType RunLengthEncodedColumn::getDataType() const { return value_->getDataType(); }
ColumnEncoding RunLengthEncodedColumn::getEncoding() const { return ColumnEncoding::Rle; }

bool RunLengthEncodedColumn::getBoolean(int32_t position) const {
    return value_->getBoolean(0);
}

int32_t RunLengthEncodedColumn::getInt(int32_t position) const {
    return value_->getInt(0);
}

int64_t RunLengthEncodedColumn::getLong(int32_t position) const {
    return value_->getLong(0);
}

float RunLengthEncodedColumn::getFloat(int32_t position) const {
    return value_->getFloat(0);
}

double RunLengthEncodedColumn::getDouble(int32_t position) const {
    return value_->getDouble(0);
}

std::shared_ptr<Binary> RunLengthEncodedColumn::getBinary(int32_t position) const {
    return value_->getBinary(0);
}

std::vector<bool> RunLengthEncodedColumn::getBooleans() const {
    bool v = value_->getBoolean(0);
    return std::vector<bool>(positionCount_, v);
}

std::vector<int32_t> RunLengthEncodedColumn::getInts() const {
    int32_t v = value_->getInt(0);
    return std::vector<int32_t>(positionCount_, v);
}

std::vector<int64_t> RunLengthEncodedColumn::getLongs() const {
    int64_t v = value_->getLong(0);
    return std::vector<int64_t>(positionCount_, v);
}

std::vector<float> RunLengthEncodedColumn::getFloats() const {
    float v = value_->getFloat(0);
    return std::vector<float>(positionCount_, v);
}

std::vector<double> RunLengthEncodedColumn::getDoubles() const {
    double v = value_->getDouble(0);
    return std::vector<double>(positionCount_, v);
}

std::vector<std::shared_ptr<Binary>> RunLengthEncodedColumn::getBinaries() const {
    auto v = value_->getBinary(0);
    return std::vector<std::shared_ptr<Binary>>(positionCount_, v);
}

bool RunLengthEncodedColumn::mayHaveNull() const { return value_->mayHaveNull(); }

bool RunLengthEncodedColumn::isNull(int32_t position) const {
    return value_->isNull(0);
}

std::vector<bool> RunLengthEncodedColumn::isNulls() const {
    bool v = value_->isNull(0);
    return std::vector<bool>(positionCount_, v);
}

int32_t RunLengthEncodedColumn::getPositionCount() const { return positionCount_; }
