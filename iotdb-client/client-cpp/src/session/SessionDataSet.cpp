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

#include "IoTDBRpcDataSet.h"
#include "SessionDataSetFactory.h"

using namespace std;

struct SessionDataSet::Impl {
  std::shared_ptr<IoTDBRpcDataSet> iotdbRpcDataSet_;
};

std::unique_ptr<SessionDataSet> createSessionDataSet(
    const std::string &sql, const std::vector<std::string> &columnNameList,
    const std::vector<std::string> &columnTypeList,
    const std::map<std::string, int32_t> &columnNameIndex, int64_t queryId,
    int64_t statementId, std::shared_ptr<IClientRPCServiceClient> client,
    int64_t sessionId, const std::vector<std::string> &queryResult,
    bool ignoreTimestamp, int64_t timeout, bool moreData, int32_t fetchSize,
    const std::string &zoneId) {
  auto dataSet = std::unique_ptr<SessionDataSet>(new SessionDataSet());
  dataSet->impl_ =
      std::unique_ptr<SessionDataSet::Impl>(new SessionDataSet::Impl());
  dataSet->impl_->iotdbRpcDataSet_ = std::make_shared<IoTDBRpcDataSet>(
      sql, columnNameList, columnTypeList, columnNameIndex, ignoreTimestamp,
      moreData, queryId, statementId, client, sessionId, queryResult, fetchSize,
      timeout, zoneId, IoTDBRpcDataSet::DEFAULT_TIME_FORMAT);
  return dataSet;
}

RowRecord::RowRecord(int64_t timestamp) { this->timestamp = timestamp; }

RowRecord::RowRecord(int64_t timestamp, const std::vector<Field> &fields)
    : timestamp(timestamp), fields(fields) {}

RowRecord::RowRecord(const std::vector<Field> &fields)
    : timestamp(-1), fields(fields) {}

RowRecord::RowRecord() { this->timestamp = -1; }

void RowRecord::addField(const Field &f) { this->fields.push_back(f); }

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
    const Field &f = fields[i];
    switch (f.dataType) {
    case TSDataType::BOOLEAN:
      if (f.isNull()) {
        ret.append("null");
      } else {
        ret.append(f.boolV.value() ? "true" : "false");
      }
      break;
    case TSDataType::INT32:
      if (f.isNull()) {
        ret.append("null");
      } else {
        ret.append(std::to_string(f.intV.value()));
      }
      break;
    case TSDataType::DATE:
      if (f.isNull()) {
        ret.append("null");
      } else {
        ret.append(f.dateV.value().toIsoExtendedString());
      }
      break;
    case TSDataType::TIMESTAMP:
    case TSDataType::INT64:
      if (f.isNull()) {
        ret.append("null");
      } else {
        ret.append(std::to_string(f.longV.value()));
      }
      break;
    case TSDataType::FLOAT:
      if (f.isNull()) {
        ret.append("null");
      } else {
        ret.append(std::to_string(f.floatV.value()));
      }
      break;
    case TSDataType::DOUBLE:
      if (f.isNull()) {
        ret.append("null");
      } else {
        ret.append(std::to_string(f.doubleV.value()));
      }
      break;
    case TSDataType::BLOB:
    case TSDataType::STRING:
    case TSDataType::TEXT:
      if (f.isNull()) {
        ret.append("null");
      } else {
        ret.append(f.stringV.value());
      }
      break;
    case TSDataType::OBJECT:
      if (!f.stringV.is_initialized()) {
        ret.append("null");
      } else {
        ret.append(f.stringV.value());
      }
      break;
    default:
      break;
    }
  }
  ret.append("\n");
  return ret;
}

SessionDataSet::~SessionDataSet() = default;

bool SessionDataSet::hasNext() {
  if (impl_->iotdbRpcDataSet_->hasCachedRecord()) {
    return true;
  }
  return impl_->iotdbRpcDataSet_->next();
}

shared_ptr<RowRecord> SessionDataSet::next() {
  if (!impl_->iotdbRpcDataSet_->hasCachedRecord() && !hasNext()) {
    return nullptr;
  }
  impl_->iotdbRpcDataSet_->setHasCachedRecord(false);
  return constructRowRecordFromValueArray();
}

int SessionDataSet::getFetchSize() {
  return impl_->iotdbRpcDataSet_->getFetchSize();
}

void SessionDataSet::setFetchSize(int fetchSize) {
  impl_->iotdbRpcDataSet_->setFetchSize(fetchSize);
}

const std::vector<std::string> &SessionDataSet::getColumnNames() const {
  return impl_->iotdbRpcDataSet_->getColumnNameList();
}

const std::vector<std::string> &SessionDataSet::getColumnTypeList() const {
  return impl_->iotdbRpcDataSet_->getColumnTypeList();
}

void SessionDataSet::closeOperationHandle(bool forceClose) {
  impl_->iotdbRpcDataSet_->close(forceClose);
}

SessionDataSet::DataIterator::DataIterator(std::shared_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

bool SessionDataSet::DataIterator::next() {
  return impl_->iotdbRpcDataSet_->next();
}

bool SessionDataSet::DataIterator::isNull(const std::string &columnName) {
  return impl_->iotdbRpcDataSet_->isNullByColumnName(columnName);
}

bool SessionDataSet::DataIterator::isNullByIndex(int32_t columnIndex) {
  return impl_->iotdbRpcDataSet_->isNullByIndex(columnIndex);
}

Optional<bool>
SessionDataSet::DataIterator::getBooleanByIndex(int32_t columnIndex) {
  return impl_->iotdbRpcDataSet_->getBooleanByIndex(columnIndex);
}

Optional<bool>
SessionDataSet::DataIterator::getBoolean(const std::string &columnName) {
  return impl_->iotdbRpcDataSet_->getBoolean(columnName);
}

Optional<double>
SessionDataSet::DataIterator::getDoubleByIndex(int32_t columnIndex) {
  return impl_->iotdbRpcDataSet_->getDoubleByIndex(columnIndex);
}

Optional<double>
SessionDataSet::DataIterator::getDouble(const std::string &columnName) {
  return impl_->iotdbRpcDataSet_->getDouble(columnName);
}

Optional<float>
SessionDataSet::DataIterator::getFloatByIndex(int32_t columnIndex) {
  return impl_->iotdbRpcDataSet_->getFloatByIndex(columnIndex);
}

Optional<float>
SessionDataSet::DataIterator::getFloat(const std::string &columnName) {
  return impl_->iotdbRpcDataSet_->getFloat(columnName);
}

Optional<int32_t>
SessionDataSet::DataIterator::getIntByIndex(int32_t columnIndex) {
  return impl_->iotdbRpcDataSet_->getIntByIndex(columnIndex);
}

Optional<int32_t>
SessionDataSet::DataIterator::getInt(const std::string &columnName) {
  return impl_->iotdbRpcDataSet_->getInt(columnName);
}

Optional<int64_t>
SessionDataSet::DataIterator::getLongByIndex(int32_t columnIndex) {
  return impl_->iotdbRpcDataSet_->getLongByIndex(columnIndex);
}

Optional<int64_t>
SessionDataSet::DataIterator::getLong(const std::string &columnName) {
  return impl_->iotdbRpcDataSet_->getLong(columnName);
}

Optional<std::string>
SessionDataSet::DataIterator::getStringByIndex(int32_t columnIndex) {
  return impl_->iotdbRpcDataSet_->getStringByIndex(columnIndex);
}

Optional<std::string>
SessionDataSet::DataIterator::getString(const std::string &columnName) {
  return impl_->iotdbRpcDataSet_->getString(columnName);
}

Optional<int64_t>
SessionDataSet::DataIterator::getTimestampByIndex(int32_t columnIndex) {
  return impl_->iotdbRpcDataSet_->getTimestampByIndex(columnIndex);
}

Optional<int64_t>
SessionDataSet::DataIterator::getTimestamp(const std::string &columnName) {
  return impl_->iotdbRpcDataSet_->getTimestamp(columnName);
}

Optional<IoTDBDate>
SessionDataSet::DataIterator::getDateByIndex(int32_t columnIndex) {
  return impl_->iotdbRpcDataSet_->getDateByIndex(columnIndex);
}

Optional<IoTDBDate>
SessionDataSet::DataIterator::getDate(const std::string &columnName) {
  return impl_->iotdbRpcDataSet_->getDate(columnName);
}

int32_t
SessionDataSet::DataIterator::findColumn(const std::string &columnName) {
  return impl_->iotdbRpcDataSet_->findColumn(columnName);
}

const std::vector<std::string> &
SessionDataSet::DataIterator::getColumnNames() const {
  return impl_->iotdbRpcDataSet_->getColumnNameList();
}

const std::vector<std::string> &
SessionDataSet::DataIterator::getColumnTypeList() const {
  return impl_->iotdbRpcDataSet_->getColumnTypeList();
}

SessionDataSet::DataIterator SessionDataSet::getIterator() {
  return DataIterator(std::shared_ptr<Impl>(impl_.get(), [](Impl *) {}));
}

shared_ptr<RowRecord> SessionDataSet::constructRowRecordFromValueArray() {
  std::vector<Field> outFields;
  const auto &dataSet = impl_->iotdbRpcDataSet_;
  const int32_t valueColumnCount = dataSet->getServerColumnCount();
  for (int i = 0; i < valueColumnCount; i++) {
    const int32_t listIndex = dataSet->getValueColumnNameListIndex(i);
    const std::string &columnName = dataSet->getColumnNameList().at(listIndex);
    Field field;
    if (!dataSet->isNullByColumnName(columnName)) {
      TSDataType::TSDataType dataType = dataSet->getDataType(columnName);
      field.dataType = dataType;
      switch (dataType) {
      case TSDataType::BOOLEAN:
        field.boolV = dataSet->getBoolean(columnName);
        break;
      case TSDataType::INT32:
        field.intV = dataSet->getInt(columnName);
        break;
      case TSDataType::DATE:
        field.dateV = dataSet->getDate(columnName);
        break;
      case TSDataType::INT64:
      case TSDataType::TIMESTAMP:
        field.longV = dataSet->getLong(columnName);
        break;
      case TSDataType::FLOAT:
        field.floatV = dataSet->getFloat(columnName);
        break;
      case TSDataType::DOUBLE:
        field.doubleV = dataSet->getDouble(columnName);
        break;
      case TSDataType::TEXT:
      case TSDataType::BLOB:
      case TSDataType::STRING:
      case TSDataType::OBJECT: {
        auto stringValue = dataSet->getString(columnName);
        if (stringValue.is_initialized()) {
          field.stringV = stringValue.value();
        }
        break;
      }
      default:
        throw UnSupportedDataTypeException("Data type is not supported.");
      }
    }
    outFields.emplace_back(field);
  }
  return std::make_shared<RowRecord>(dataSet->getCurrentRowTime(), outFields);
}
