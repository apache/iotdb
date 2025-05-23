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
#ifndef IOTDB_COLUMN_H
#define IOTDB_COLUMN_H

#include <vector>
#include <memory>
#include <map>
#include <stdexcept>

#include "Common.h"
#include "ColumnDecoder.h"

enum class ColumnEncoding : uint8_t {
    ByteArray,
    Int32Array,
    Int64Array,
    BinaryArray,
    Rle
};

class Binary {
public:
    explicit Binary(std::vector<uint8_t> data) : data_(std::move(data)) {
    }

    const std::vector<uint8_t>& getData() const { return data_; }

    std::string getStringValue() const {
        return {data_.begin(), data_.end()};
    }

private:
    std::vector<uint8_t> data_;
};


const std::map<ColumnEncoding, std::shared_ptr<ColumnDecoder>> kEncodingToDecoder = {
    {ColumnEncoding::Int32Array, std::make_shared<Int32ArrayColumnDecoder>()},
    {ColumnEncoding::Int64Array, std::make_shared<Int64ArrayColumnDecoder>()},
    {ColumnEncoding::ByteArray, std::make_shared<ByteArrayColumnDecoder>()},
    {ColumnEncoding::BinaryArray, std::make_shared<BinaryArrayColumnDecoder>()},
    {ColumnEncoding::Rle, std::make_shared<RunLengthColumnDecoder>()}
};

const std::map<uint8_t, ColumnEncoding> kByteToEncoding = {
    {0, ColumnEncoding::ByteArray},
    {1, ColumnEncoding::Int32Array},
    {2, ColumnEncoding::Int64Array},
    {3, ColumnEncoding::BinaryArray},
    {4, ColumnEncoding::Rle}
};

inline std::shared_ptr<ColumnDecoder> getColumnDecoder(ColumnEncoding encoding) {
    auto it = kEncodingToDecoder.find(encoding);
    if (it == kEncodingToDecoder.end()) {
        throw IoTDBException("Unsupported column encoding");
    }
    return it->second;
}

inline ColumnEncoding getColumnEncodingByByte(uint8_t b) {
    auto it = kByteToEncoding.find(b);
    if (it == kByteToEncoding.end()) {
        throw IoTDBException("Invalid encoding value: " + std::to_string(b));
    }
    return it->second;
}

class Column {
public:
    virtual ~Column() = default;

    virtual TSDataType::TSDataType getDataType() const = 0;
    virtual ColumnEncoding getEncoding() const = 0;

    virtual bool getBoolean(int32_t position) const {
        throw IoTDBException("Unsupported operation: getBoolean");
    }

    virtual int32_t getInt(int32_t position) const {
        throw IoTDBException("Unsupported operation: getInt");
    }

    virtual int64_t getLong(int32_t position) const {
        throw IoTDBException("Unsupported operation: getLong");
    }

    virtual float getFloat(int32_t position) const {
        throw IoTDBException("Unsupported operation: getFloat");
    }

    virtual double getDouble(int32_t position) const {
        throw IoTDBException("Unsupported operation: getDouble");
    }

    virtual std::shared_ptr<Binary> getBinary(int32_t position) const {
        throw IoTDBException("Unsupported operation: getBinary");
    }

    virtual std::vector<bool> getBooleans() const {
        throw IoTDBException("Unsupported operation: getBooleans");
    }

    virtual std::vector<int32_t> getInts() const {
        throw IoTDBException("Unsupported operation: getInts");
    }

    virtual std::vector<int64_t> getLongs() const {
        throw IoTDBException("Unsupported operation: getLongs");
    }

    virtual std::vector<float> getFloats() const {
        throw IoTDBException("Unsupported operation: getFloats");
    }

    virtual std::vector<double> getDoubles() const {
        throw IoTDBException("Unsupported operation: getDoubles");
    }

    virtual std::vector<std::shared_ptr<Binary>> getBinaries() const {
        throw IoTDBException("Unsupported operation: getBinaries");
    }

    virtual bool mayHaveNull() const = 0;
    virtual bool isNull(int32_t position) const = 0;
    virtual std::vector<bool> isNulls() const = 0;

    virtual int32_t getPositionCount() const = 0;
};

class TimeColumn : public Column {
public:
    TimeColumn(int32_t arrayOffset, int32_t positionCount, const std::vector<int64_t>& values);

    TSDataType::TSDataType getDataType() const override;
    ColumnEncoding getEncoding() const override;

    int64_t getLong(int32_t position) const override;

    bool mayHaveNull() const override;
    bool isNull(int32_t position) const override;
    std::vector<bool> isNulls() const override;

    int32_t getPositionCount() const override;

    int64_t getStartTime() const;
    int64_t getEndTime() const;

    const std::vector<int64_t>& getTimes() const;
    std::vector<int64_t> getLongs() const override;

private:
    int32_t arrayOffset_;
    int32_t positionCount_;
    std::vector<int64_t> values_;
};

class BinaryColumn : public Column {
public:
    BinaryColumn(int32_t arrayOffset, int32_t positionCount,
                 const std::vector<bool>& valueIsNull, const std::vector<std::shared_ptr<Binary>>& values);

    TSDataType::TSDataType getDataType() const override;
    ColumnEncoding getEncoding() const override;

    std::shared_ptr<Binary> getBinary(int32_t position) const override;
    std::vector<std::shared_ptr<Binary>> getBinaries() const override;

    bool mayHaveNull() const override;
    bool isNull(int32_t position) const override;
    std::vector<bool> isNulls() const override;

    int32_t getPositionCount() const override;

private:
    int32_t arrayOffset_;
    int32_t positionCount_;
    std::vector<bool> valueIsNull_;
    std::vector<std::shared_ptr<Binary>> values_;
};

class IntColumn : public Column {
public:
    IntColumn(int32_t arrayOffset, int32_t positionCount,
              const std::vector<bool>& valueIsNull, const std::vector<int32_t>& values);

    TSDataType::TSDataType getDataType() const override;
    ColumnEncoding getEncoding() const override;

    int32_t getInt(int32_t position) const override;
    std::vector<int32_t> getInts() const override;

    bool mayHaveNull() const override;
    bool isNull(int32_t position) const override;
    std::vector<bool> isNulls() const override;

    int32_t getPositionCount() const override;

private:
    int32_t arrayOffset_;
    int32_t positionCount_;
    std::vector<bool> valueNull_;
    std::vector<int32_t> values_;
};

class FloatColumn : public Column {
public:
    FloatColumn(int32_t arrayOffset, int32_t positionCount,
                const std::vector<bool>& valueIsNull, const std::vector<float>& values);

    TSDataType::TSDataType getDataType() const override;
    ColumnEncoding getEncoding() const override;

    float getFloat(int32_t position) const override;
    std::vector<float> getFloats() const override;

    bool mayHaveNull() const override;
    bool isNull(int32_t position) const override;
    std::vector<bool> isNulls() const override;

    int32_t getPositionCount() const override;

private:
    int32_t arrayOffset_;
    int32_t positionCount_;
    std::vector<bool> valueIsNull_;
    std::vector<float> values_;
};

class LongColumn : public Column {
public:
    LongColumn(int32_t arrayOffset, int32_t positionCount,
               const std::vector<bool>& valueIsNull, const std::vector<int64_t>& values);

    TSDataType::TSDataType getDataType() const override;
    ColumnEncoding getEncoding() const override;

    int64_t getLong(int32_t position) const override;
    std::vector<int64_t> getLongs() const override;

    bool mayHaveNull() const override;
    bool isNull(int32_t position) const override;
    std::vector<bool> isNulls() const override;

    int32_t getPositionCount() const override;

private:
    int32_t arrayOffset_;
    int32_t positionCount_;
    std::vector<bool> valueIsNull_;
    std::vector<int64_t> values_;
};

class DoubleColumn : public Column {
public:
    DoubleColumn(int32_t arrayOffset, int32_t positionCount,
                 const std::vector<bool>& valueIsNull, const std::vector<double>& values);

    TSDataType::TSDataType getDataType() const override;
    ColumnEncoding getEncoding() const override;

    double getDouble(int32_t position) const override;
    std::vector<double> getDoubles() const override;

    bool mayHaveNull() const override;
    bool isNull(int32_t position) const override;
    std::vector<bool> isNulls() const override;

    int32_t getPositionCount() const override;

private:
    int32_t arrayOffset_;
    int32_t positionCount_;
    std::vector<bool> valueIsNull_;
    std::vector<double> values_;
};

class BooleanColumn : public Column {
public:
    BooleanColumn(int32_t arrayOffset, int32_t positionCount,
                  const std::vector<bool>& valueIsNull, const std::vector<bool>& values);

    TSDataType::TSDataType getDataType() const override;
    ColumnEncoding getEncoding() const override;

    bool getBoolean(int32_t position) const override;
    std::vector<bool> getBooleans() const override;

    bool mayHaveNull() const override;
    bool isNull(int32_t position) const override;
    std::vector<bool> isNulls() const override;

    int32_t getPositionCount() const override;

private:
    int32_t arrayOffset_;
    int32_t positionCount_;
    std::vector<bool> valueIsNull_;
    std::vector<bool> values_;
};

class RunLengthEncodedColumn : public Column {
public:
    RunLengthEncodedColumn(std::shared_ptr<Column> value, int32_t positionCount);

    std::shared_ptr<Column> getValue() const;

    TSDataType::TSDataType getDataType() const override;
    ColumnEncoding getEncoding() const override;

    bool getBoolean(int32_t position) const override;
    int32_t getInt(int32_t position) const override;
    int64_t getLong(int32_t position) const override;
    float getFloat(int32_t position) const override;
    double getDouble(int32_t position) const override;
    std::shared_ptr<Binary> getBinary(int32_t position) const override;

    std::vector<bool> getBooleans() const override;
    std::vector<int32_t> getInts() const override;
    std::vector<int64_t> getLongs() const override;
    std::vector<float> getFloats() const override;
    std::vector<double> getDoubles() const override;
    std::vector<std::shared_ptr<Binary>> getBinaries() const override;

    bool mayHaveNull() const override;
    bool isNull(int32_t position) const override;
    std::vector<bool> isNulls() const override;

    int32_t getPositionCount() const override;

private:
    std::shared_ptr<Column> value_;
    int32_t positionCount_;
};

#endif
