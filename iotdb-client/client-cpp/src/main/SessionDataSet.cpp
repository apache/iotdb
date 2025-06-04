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

#include "SessionDataSet.h"
#include <stdexcept>
#include <boost/date_time/gregorian/gregorian.hpp>

RowRecord::RowRecord(int64_t timestamp) {
    this->timestamp = timestamp;
}

RowRecord::RowRecord(int64_t timestamp, const std::vector<Field>& fields)
    : timestamp(timestamp), fields(fields) {
}

RowRecord::RowRecord(const std::vector<Field>& fields)
    : timestamp(-1), fields(fields) {
}

RowRecord::RowRecord() {
    this->timestamp = -1;
}

void RowRecord::addField(const Field& f) {
    this->fields.push_back(f);
}

std::string RowRecord::toString() {
    std::string ret;
    if (this->timestamp != -1) {
        ret.append(std::to_string(timestamp));
        ret.append("\t");
    }
    for (size_t i = 0; i < fields.size(); i++) {
        if (i != 0) {
            ret.append("\t");
        }
        TSDataType::TSDataType dataType = fields[i].dataType;
        switch (dataType) {
        case TSDataType::BOOLEAN:
            ret.append(fields[i].boolV ? "true" : "false");
            break;
        case TSDataType::INT32:
            ret.append(std::to_string(fields[i].intV));
            break;
        case TSDataType::DATE:
            ret.append(boost::gregorian::to_iso_extended_string(fields[i].dateV));
            break;
        case TSDataType::TIMESTAMP:
        case TSDataType::INT64:
            ret.append(std::to_string(fields[i].longV));
            break;
        case TSDataType::FLOAT:
            ret.append(std::to_string(fields[i].floatV));
            break;
        case TSDataType::DOUBLE:
            ret.append(std::to_string(fields[i].doubleV));
            break;
        case TSDataType::BLOB:
        case TSDataType::STRING:
        case TSDataType::TEXT:
            ret.append(fields[i].stringV);
            break;
        default:
            break;
        }
    }
    ret.append("\n");
    return ret;
}

bool SessionDataSet::hasNext() {
    if (iotdbRpcDataSet_->hasCachedRecord()) {
        return true;
    }
    return iotdbRpcDataSet_->next();
}

shared_ptr<RowRecord> SessionDataSet::next() {
    if (!iotdbRpcDataSet_->hasCachedRecord() && !hasNext()) {
        return nullptr;
    }
    iotdbRpcDataSet_->setHasCachedRecord(false);

    return constructRowRecordFromValueArray();
}

int SessionDataSet::getFetchSize() {
    return iotdbRpcDataSet_->getFetchSize();
}

void SessionDataSet::setFetchSize(int fetchSize) {
    return iotdbRpcDataSet_->setFetchSize(fetchSize);
}

const std::vector<std::string>& SessionDataSet::getColumnNames() const {
    return iotdbRpcDataSet_->getColumnNameList();
}

const std::vector<std::string>& SessionDataSet::getColumnTypeList() const {
    return iotdbRpcDataSet_->getColumnTypeList();
}

void SessionDataSet::closeOperationHandle(bool forceClose) {
    iotdbRpcDataSet_->close(forceClose);
}

bool SessionDataSet::DataIterator::next() {
    return iotdbRpcDataSet_->next();
}

bool SessionDataSet::DataIterator::isNull(const std::string& columnName) {
    return iotdbRpcDataSet_->isNullByColumnName(columnName);
}

bool SessionDataSet::DataIterator::isNullByIndex(int32_t columnIndex) {
    return iotdbRpcDataSet_->isNullByIndex(columnIndex);
}

bool SessionDataSet::DataIterator::getBooleanByIndex(int32_t columnIndex) {
    return iotdbRpcDataSet_->getBooleanByIndex(columnIndex);
}

bool SessionDataSet::DataIterator::getBoolean(const std::string& columnName) {
    return iotdbRpcDataSet_->getBoolean(columnName);
}

double SessionDataSet::DataIterator::getDoubleByIndex(int32_t columnIndex) {
    return iotdbRpcDataSet_->getDoubleByIndex(columnIndex);
}

double SessionDataSet::DataIterator::getDouble(const std::string& columnName) {
    return iotdbRpcDataSet_->getDouble(columnName);
}

float SessionDataSet::DataIterator::getFloatByIndex(int32_t columnIndex) {
    return iotdbRpcDataSet_->getFloatByIndex(columnIndex);
}

float SessionDataSet::DataIterator::getFloat(const std::string& columnName) {
    return iotdbRpcDataSet_->getFloat(columnName);
}

int32_t SessionDataSet::DataIterator::getIntByIndex(int32_t columnIndex) {
    return iotdbRpcDataSet_->getIntByIndex(columnIndex);
}

int32_t SessionDataSet::DataIterator::getInt(const std::string& columnName) {
    return iotdbRpcDataSet_->getInt(columnName);
}

int64_t SessionDataSet::DataIterator::getLongByIndex(int32_t columnIndex) {
    return iotdbRpcDataSet_->getLongByIndex(columnIndex);
}

int64_t SessionDataSet::DataIterator::getLong(const std::string& columnName) {
    return iotdbRpcDataSet_->getLong(columnName);
}

std::string SessionDataSet::DataIterator::getStringByIndex(int32_t columnIndex) {
    return iotdbRpcDataSet_->getStringByIndex(columnIndex);
}

std::string SessionDataSet::DataIterator::getString(const std::string& columnName) {
    return iotdbRpcDataSet_->getString(columnName);
}

int64_t SessionDataSet::DataIterator::getTimestampByIndex(int32_t columnIndex) {
    return iotdbRpcDataSet_->getTimestampByIndex(columnIndex);
}

int64_t SessionDataSet::DataIterator::getTimestamp(const std::string& columnName) {
    return iotdbRpcDataSet_->getTimestamp(columnName);
}

boost::gregorian::date SessionDataSet::DataIterator::getDateByIndex(int32_t columnIndex) {
    return iotdbRpcDataSet_->getDateByIndex(columnIndex);
}

boost::gregorian::date SessionDataSet::DataIterator::getDate(const std::string& columnName) {
    return iotdbRpcDataSet_->getDate(columnName);
}

int32_t SessionDataSet::DataIterator::findColumn(const std::string& columnName) {
    return iotdbRpcDataSet_->findColumn(columnName);
}

const std::vector<std::string>& SessionDataSet::DataIterator::getColumnNames() const {
    return iotdbRpcDataSet_->getColumnNameList();
}

const std::vector<std::string>& SessionDataSet::DataIterator::getColumnTypeList() const {
    return iotdbRpcDataSet_->getColumnTypeList();
}

shared_ptr<RowRecord> SessionDataSet::constructRowRecordFromValueArray() {
    std::vector<Field> outFields;
    for (int i = iotdbRpcDataSet_->getValueColumnStartIndex(); i < iotdbRpcDataSet_->getColumnSize(); i++) {
        Field field;
        std::string columnName = iotdbRpcDataSet_->getColumnNameList().at(i);
        if (!iotdbRpcDataSet_->isNullByColumnName(columnName)) {
            TSDataType::TSDataType dataType = iotdbRpcDataSet_->getDataType(columnName);
            field.dataType = dataType;
            switch (dataType) {
            case TSDataType::BOOLEAN:
                field.boolV = iotdbRpcDataSet_->getBoolean(columnName);
                break;
            case TSDataType::INT32:
                field.intV = iotdbRpcDataSet_->getInt(columnName);
                break;
            case TSDataType::DATE:
                field.dateV = parseIntToDate(iotdbRpcDataSet_->getInt(columnName));
                break;
            case TSDataType::INT64:
            case TSDataType::TIMESTAMP:
                field.longV = iotdbRpcDataSet_->getLong(columnName);
                break;
            case TSDataType::FLOAT:
                field.floatV = iotdbRpcDataSet_->getFloat(columnName);
                break;
            case TSDataType::DOUBLE:
                field.doubleV = iotdbRpcDataSet_->getDouble(columnName);
                break;
            case TSDataType::TEXT:
            case TSDataType::BLOB:
            case TSDataType::STRING:
                field.stringV = iotdbRpcDataSet_->getBinary(columnName)->getStringValue();
                break;
            default:
                throw new UnSupportedDataTypeException("Data type %s is not supported." + dataType);
            }
        }
        outFields.emplace_back(field);
    }
    return std::make_shared<RowRecord>(iotdbRpcDataSet_->getCurrentRowTime(), outFields);
}
