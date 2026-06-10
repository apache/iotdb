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

#include <algorithm>
#include <cstdio>
#include <ctime>
#include <stdexcept>

#include "IoTDBRpcDataSet.h"

#include <thrift/transport/TTransportException.h>

#include "Column.h"
#include "Date.h"
#include "Optional.h"
#include "RpcCommon.h"

using apache::thrift::transport::TTransportException;

const int32_t IoTDBRpcDataSet::startIndex = 2;
const std::string IoTDBRpcDataSet::TimestampColumnName = "Time";
const std::string IoTDBRpcDataSet::DEFAULT_TIME_FORMAT = "default";
const std::string IoTDBRpcDataSet::TIME_PRECISION = "timestamp_precision";
const std::string IoTDBRpcDataSet::MILLISECOND = "ms";
const std::string IoTDBRpcDataSet::MICROSECOND = "us";
const std::string IoTDBRpcDataSet::NANOSECOND = "ns";

IoTDBRpcDataSet::IoTDBRpcDataSet(
    const std::string &sql, const std::vector<std::string> &columnNameList,
    const std::vector<std::string> &columnTypeList,
    const std::map<std::string, int32_t> &columnNameIndex, bool ignoreTimestamp,
    bool moreData, int64_t queryId, int64_t statementId,
    std::shared_ptr<IClientRPCServiceClient> client, int64_t sessionId,
    const std::vector<std::string> &queryResult, int32_t fetchSize,
    const int64_t timeout, const std::string &zoneId,
    const std::string &timeFormat)
    : sql_(sql), isClosed_(false), client_(client), fetchSize_(fetchSize),
      timeout_(timeout), hasCachedRecord_(false), lastReadWasNull_(false),
      columnSize_(static_cast<int32_t>(columnNameList.size())),
      sessionId_(sessionId), queryId_(queryId), statementId_(statementId),
      time_(0), ignoreTimestamp_(ignoreTimestamp), moreData_(moreData),
      queryResult_(queryResult), curTsBlock_(nullptr),
      queryResultSize_(static_cast<int32_t>(queryResult.size())),
      queryResultIndex_(0), tsBlockSize_(0), tsBlockIndex_(-1),
      timeZoneId_(zoneId), timeFormat_(timeFormat) {
  std::vector<int32_t> columnIndex2TsBlockColumnIndexList;
  int columnStartIndex = 1;
  int resultSetColumnSize = columnNameList_.size();
  // newly generated or updated columnIndex2TsBlockColumnIndexList.size() may
  // not be equal to columnNameList.size() so we need
  // startIndexForColumnIndex2TsBlockColumnIndexList to adjust the mapping
  // relation
  int startIndexForColumnIndex2TsBlockColumnIndexList = 0;
  const bool serverIncludesTime =
      !columnNameList.empty() && columnNameList[0] == TimestampColumnName;
  // for Time Column in tree model which should always be the first column and
  // its index for TsBlockColumn is -1
  if (!ignoreTimestamp && !serverIncludesTime) {
    columnNameList_.push_back(TimestampColumnName);
    columnTypeList_.push_back("INT64");
    columnName2TsBlockColumnIndexMap_[TimestampColumnName] = -1;
    columnOrdinalMap_[TimestampColumnName] = 1;
    if (!columnIndex2TsBlockColumnIndexList.empty()) {
      columnIndex2TsBlockColumnIndexList.insert(
          columnIndex2TsBlockColumnIndexList.begin(), -1);
      startIndexForColumnIndex2TsBlockColumnIndexList = 1;
    }
    columnStartIndex++;
    resultSetColumnSize++;
  }

  if (columnIndex2TsBlockColumnIndexList.empty()) {
    columnIndex2TsBlockColumnIndexList.reserve(resultSetColumnSize);
    if (!ignoreTimestamp && !serverIncludesTime) {
      startIndexForColumnIndex2TsBlockColumnIndexList = 1;
      columnIndex2TsBlockColumnIndexList.push_back(-1);
    }
    for (size_t i = 0; i < columnNameList.size(); ++i) {
      if (!columnNameIndex.empty()) {
        auto it = columnNameIndex.find(columnNameList[i]);
        columnIndex2TsBlockColumnIndexList.push_back(
            it != columnNameIndex.end() ? it->second : static_cast<int32_t>(i));
      } else {
        columnIndex2TsBlockColumnIndexList.push_back(static_cast<int32_t>(i));
      }
    }
  }

  columnNameList_.insert(columnNameList_.end(), columnNameList.begin(),
                         columnNameList.end());
  columnTypeList_.insert(columnTypeList_.end(), columnTypeList.begin(),
                         columnTypeList.end());

  // Initialize data types for TsBlock columns
  int32_t tsBlockColumnSize = 0;
  for (auto value : columnIndex2TsBlockColumnIndexList) {
    if (value > tsBlockColumnSize) {
      tsBlockColumnSize = value;
    }
  }
  tsBlockColumnSize += 1;
  dataTypeForTsBlockColumn_.resize(tsBlockColumnSize);

  // Populate data types and maps
  for (size_t i = 0; i < columnNameList.size(); i++) {
    auto columnName = columnNameList[i];
    int32_t tsBlockColumnIndex;
    if (!columnNameIndex.empty()) {
      auto it = columnNameIndex.find(columnName);
      tsBlockColumnIndex =
          it != columnNameIndex.end() ? it->second : static_cast<int32_t>(i);
    } else {
      tsBlockColumnIndex = columnIndex2TsBlockColumnIndexList
          [i + startIndexForColumnIndex2TsBlockColumnIndexList];
    }
    if (tsBlockColumnIndex != -1) {
      if (static_cast<size_t>(tsBlockColumnIndex) >=
          dataTypeForTsBlockColumn_.size()) {
        dataTypeForTsBlockColumn_.resize(tsBlockColumnIndex + 1);
      }
      dataTypeForTsBlockColumn_[tsBlockColumnIndex] =
          getDataTypeByStr(columnTypeList[i]);
    }

    if (columnName2TsBlockColumnIndexMap_.find(columnName) ==
        columnName2TsBlockColumnIndexMap_.end()) {
      columnOrdinalMap_[columnName] = i + columnStartIndex;
      columnName2TsBlockColumnIndexMap_[columnName] = tsBlockColumnIndex;
    }
  }

  timePrecision_ = getTimePrecision(timeFactor_);
  columnIndex2TsBlockColumnIndexList_ = columnIndex2TsBlockColumnIndexList;

  if (serverIncludesTime) {
    columnSize_--;
  }
}

IoTDBRpcDataSet::~IoTDBRpcDataSet() {
  if (!isClosed_) {
    close();
  }
}

bool IoTDBRpcDataSet::next() {
  if (hasCachedBlock()) {
    lastReadWasNull_ = false;
    constructOneRow();
    return true;
  }

  if (hasCachedByteBuffer()) {
    constructOneTsBlock();
    constructOneRow();
    return true;
  }

  if (moreData_) {
    bool hasResultSet = fetchResults();
    if (hasResultSet && hasCachedByteBuffer()) {
      constructOneTsBlock();
      constructOneRow();
      return true;
    }
  }

  close();
  return false;
}

void IoTDBRpcDataSet::close(bool forceClose) {
  if ((!forceClose) && isClosed_) {
    return;
  }
  TSCloseOperationReq closeReq;
  closeReq.__set_sessionId(sessionId_);
  closeReq.__set_statementId(statementId_);
  closeReq.__set_queryId(queryId_);
  TSStatus tsStatus;
  try {
    client_->closeOperation(tsStatus, closeReq);
    RpcUtils::verifySuccess(tsStatus);
  } catch (const TTransportException &e) {
    log_debug(e.what());
    throw IoTDBConnectionException(e.what());
  } catch (const IoTDBException &e) {
    log_debug(e.what());
    throw;
  } catch (std::exception &e) {
    log_debug(e.what());
    throw IoTDBException(e.what());
  }
  isClosed_ = true;
  client_ = nullptr;
}

bool IoTDBRpcDataSet::fetchResults() {
  if (isClosed_) {
    throw IoTDBException("This data set is already closed");
  }

  TSFetchResultsReq req;
  req.__set_sessionId(sessionId_);
  req.__set_statement(sql_);
  req.__set_fetchSize(fetchSize_);
  req.__set_queryId(queryId_);
  req.__set_isAlign(true);
  req.__set_timeout(timeout_);
  TSFetchResultsResp resp;
  client_->fetchResultsV2(resp, req);
  RpcUtils::verifySuccess(resp.status);
  moreData_ = resp.moreData;
  if (!resp.hasResultSet) {
    close();
  } else {
    queryResult_ = resp.queryResult;
    queryResultIndex_ = 0;
    if (!queryResult_.empty()) {
      queryResultSize_ = queryResult_.size();
    } else {
      queryResultSize_ = 0;
    }
    tsBlockIndex_ = -1;
    tsBlockSize_ = 0;
  }
  return resp.hasResultSet;
}

void IoTDBRpcDataSet::constructOneRow() {
  tsBlockIndex_++;
  hasCachedRecord_ = true;
  time_ = curTsBlock_->getTimeColumn()->getLong(tsBlockIndex_);
}

void IoTDBRpcDataSet::constructOneTsBlock() {
  lastReadWasNull_ = false;
  const auto &curTsBlockBytes = queryResult_[queryResultIndex_];
  queryResultIndex_++;
  curTsBlock_ = TsBlock::deserialize(curTsBlockBytes);
  tsBlockIndex_ = -1;
  tsBlockSize_ = curTsBlock_->getPositionCount();
}

bool IoTDBRpcDataSet::isNullByIndex(int32_t columnIndex) {
  int32_t index = getTsBlockColumnIndexForColumnIndex(columnIndex);
  return isNull(index, tsBlockIndex_);
}

bool IoTDBRpcDataSet::isNullByColumnName(const std::string &columnName) {
  int32_t index = getTsBlockColumnIndexForColumnName(columnName);
  return isNull(index, tsBlockIndex_);
}

bool IoTDBRpcDataSet::isNull(int32_t index, int32_t rowNum) {
  return index >= 0 && curTsBlock_->getColumn(index)->isNull(rowNum);
}

Optional<bool> IoTDBRpcDataSet::getBooleanByIndex(int32_t columnIndex) {
  int32_t index = getTsBlockColumnIndexForColumnIndex(columnIndex);
  return getBooleanByTsBlockColumnIndex(index);
}

Optional<bool> IoTDBRpcDataSet::getBoolean(const std::string &columnName) {
  int32_t index = getTsBlockColumnIndexForColumnName(columnName);
  return getBooleanByTsBlockColumnIndex(index);
}

// Note: tsBlockColumnIndex < 0 indicates the time pseudo-column in tree model.
// Only getLong and getString support reading the time column directly.
// All other typed getters throw IoTDBException to prevent undefined behavior
// from accessing valueColumns_ with a negative index.
Optional<bool>
IoTDBRpcDataSet::getBooleanByTsBlockColumnIndex(int32_t tsBlockColumnIndex) {
  checkRecord();
  if (tsBlockColumnIndex < 0) {
    throw IoTDBException("Cannot read boolean from time column");
  }
  if (!isNull(tsBlockColumnIndex, tsBlockIndex_)) {
    lastReadWasNull_ = false;
    return curTsBlock_->getColumn(tsBlockColumnIndex)
        ->getBoolean(tsBlockIndex_);
  } else {
    lastReadWasNull_ = true;
    return Optional<bool>::none();
  }
}

Optional<double> IoTDBRpcDataSet::getDoubleByIndex(int32_t columnIndex) {
  int32_t index = getTsBlockColumnIndexForColumnIndex(columnIndex);
  return getDoubleByTsBlockColumnIndex(index);
}

Optional<double> IoTDBRpcDataSet::getDouble(const std::string &columnName) {
  int32_t index = getTsBlockColumnIndexForColumnName(columnName);
  return getDoubleByTsBlockColumnIndex(index);
}

Optional<double>
IoTDBRpcDataSet::getDoubleByTsBlockColumnIndex(int32_t tsBlockColumnIndex) {
  checkRecord();
  if (tsBlockColumnIndex < 0) {
    throw IoTDBException("Cannot read double from time column");
  }
  if (!isNull(tsBlockColumnIndex, tsBlockIndex_)) {
    lastReadWasNull_ = false;
    return curTsBlock_->getColumn(tsBlockColumnIndex)->getDouble(tsBlockIndex_);
  } else {
    lastReadWasNull_ = true;
    return Optional<double>::none();
  }
}

Optional<float> IoTDBRpcDataSet::getFloatByIndex(int32_t columnIndex) {
  int32_t index = getTsBlockColumnIndexForColumnIndex(columnIndex);
  return getFloatByTsBlockColumnIndex(index);
}

Optional<float> IoTDBRpcDataSet::getFloat(const std::string &columnName) {
  int32_t index = getTsBlockColumnIndexForColumnName(columnName);
  return getFloatByTsBlockColumnIndex(index);
}

Optional<float>
IoTDBRpcDataSet::getFloatByTsBlockColumnIndex(int32_t tsBlockColumnIndex) {
  checkRecord();
  if (tsBlockColumnIndex < 0) {
    throw IoTDBException("Cannot read float from time column");
  }
  if (!isNull(tsBlockColumnIndex, tsBlockIndex_)) {
    lastReadWasNull_ = false;
    return curTsBlock_->getColumn(tsBlockColumnIndex)->getFloat(tsBlockIndex_);
  } else {
    lastReadWasNull_ = true;
    return Optional<float>::none();
  }
}

Optional<int32_t> IoTDBRpcDataSet::getIntByIndex(int32_t columnIndex) {
  int32_t index = getTsBlockColumnIndexForColumnIndex(columnIndex);
  return getIntByTsBlockColumnIndex(index);
}

Optional<int32_t> IoTDBRpcDataSet::getInt(const std::string &columnName) {
  int32_t index = getTsBlockColumnIndexForColumnName(columnName);
  return getIntByTsBlockColumnIndex(index);
}

Optional<int32_t>
IoTDBRpcDataSet::getIntByTsBlockColumnIndex(int32_t tsBlockColumnIndex) {
  checkRecord();
  if (tsBlockColumnIndex < 0) {
    throw IoTDBException("Cannot read int32 from time column");
  }
  if (!isNull(tsBlockColumnIndex, tsBlockIndex_)) {
    lastReadWasNull_ = false;
    const auto column = curTsBlock_->getColumn(tsBlockColumnIndex);
    switch (column->getDataType()) {
    case TSDataType::INT64:
      return static_cast<int32_t>(column->getLong(tsBlockIndex_));
    case TSDataType::DOUBLE:
      return static_cast<int32_t>(column->getDouble(tsBlockIndex_));
    case TSDataType::FLOAT:
      return static_cast<int32_t>(column->getFloat(tsBlockIndex_));
    case TSDataType::INT32:
      return column->getInt(tsBlockIndex_);
    default:
      return column->getInt(tsBlockIndex_);
    }
  } else {
    lastReadWasNull_ = true;
    return Optional<int32_t>::none();
  }
}

Optional<int64_t> IoTDBRpcDataSet::getLongByIndex(int32_t columnIndex) {
  int32_t index = getTsBlockColumnIndexForColumnIndex(columnIndex);
  return getLongByTsBlockColumnIndex(index);
}

Optional<int64_t> IoTDBRpcDataSet::getLong(const std::string &columnName) {
  int32_t index = getTsBlockColumnIndexForColumnName(columnName);
  return getLongByTsBlockColumnIndex(index);
}

Optional<int64_t>
IoTDBRpcDataSet::getLongByTsBlockColumnIndex(int32_t tsBlockColumnIndex) {
  checkRecord();
  if (tsBlockColumnIndex < 0) {
    lastReadWasNull_ = false;
    return curTsBlock_->getTimeByIndex(tsBlockIndex_);
  }
  if (!isNull(tsBlockColumnIndex, tsBlockIndex_)) {
    lastReadWasNull_ = false;
    const auto column = curTsBlock_->getColumn(tsBlockColumnIndex);
    switch (column->getDataType()) {
    case TSDataType::INT32:
      return static_cast<int64_t>(column->getInt(tsBlockIndex_));
    case TSDataType::FLOAT:
      return static_cast<int64_t>(column->getFloat(tsBlockIndex_));
    case TSDataType::DOUBLE:
      return static_cast<int64_t>(column->getDouble(tsBlockIndex_));
    case TSDataType::INT64:
      return column->getLong(tsBlockIndex_);
    default:
      return column->getLong(tsBlockIndex_);
    }
  } else {
    lastReadWasNull_ = true;
    return Optional<int64_t>::none();
  }
}

std::shared_ptr<Binary> IoTDBRpcDataSet::getBinaryByIndex(int32_t columnIndex) {
  int32_t index = getTsBlockColumnIndexForColumnIndex(columnIndex);
  return getBinaryByTsBlockColumnIndex(index);
}

std::shared_ptr<Binary>
IoTDBRpcDataSet::getBinary(const std::string &columnName) {
  int32_t index = getTsBlockColumnIndexForColumnName(columnName);
  return getBinaryByTsBlockColumnIndex(index);
}

std::shared_ptr<Binary>
IoTDBRpcDataSet::getBinaryByTsBlockColumnIndex(int32_t tsBlockColumnIndex) {
  checkRecord();
  if (tsBlockColumnIndex < 0) {
    throw IoTDBException("Cannot read binary from time column");
  }
  if (!isNull(tsBlockColumnIndex, tsBlockIndex_)) {
    lastReadWasNull_ = false;
    return curTsBlock_->getColumn(tsBlockColumnIndex)->getBinary(tsBlockIndex_);
  } else {
    lastReadWasNull_ = true;
    return nullptr;
  }
}

Optional<std::string> IoTDBRpcDataSet::getStringByIndex(int32_t columnIndex) {
  int32_t index = getTsBlockColumnIndexForColumnIndex(columnIndex);
  return getStringByTsBlockColumnIndex(index);
}

Optional<std::string>
IoTDBRpcDataSet::getString(const std::string &columnName) {
  int32_t index = getTsBlockColumnIndexForColumnName(columnName);
  return getStringByTsBlockColumnIndex(index);
}

Optional<std::string>
IoTDBRpcDataSet::getStringByTsBlockColumnIndex(int32_t tsBlockColumnIndex) {
  checkRecord();
  if (tsBlockColumnIndex < 0) {
    int64_t timestamp = curTsBlock_->getTimeByIndex(tsBlockIndex_);
    return std::to_string(timestamp);
  }
  if (isNull(tsBlockColumnIndex, tsBlockIndex_)) {
    lastReadWasNull_ = true;
    return Optional<std::string>::none();
  }
  lastReadWasNull_ = false;
  return getStringByTsBlockColumnIndexAndDataType(
      tsBlockColumnIndex, getDataTypeByTsBlockColumnIndex(tsBlockColumnIndex));
}

std::string IoTDBRpcDataSet::getStringByTsBlockColumnIndexAndDataType(
    int32_t index, TSDataType::TSDataType tsDataType) {
  const auto column = curTsBlock_->getColumn(index);
  const TSDataType::TSDataType physicalType = column->getDataType();
  switch (physicalType) {
  case TSDataType::BOOLEAN:
    return std::to_string(column->getBoolean(tsBlockIndex_));
  case TSDataType::INT32:
    return std::to_string(column->getInt(tsBlockIndex_));
  case TSDataType::INT64:
    return std::to_string(column->getLong(tsBlockIndex_));
  case TSDataType::TIMESTAMP: {
    int64_t value = column->getLong(tsBlockIndex_);
    return formatDatetime(timeFormat_, timePrecision_, value, timeZoneId_);
  }
  case TSDataType::FLOAT:
    return std::to_string(column->getFloat(tsBlockIndex_));
  case TSDataType::DOUBLE:
    return std::to_string(column->getDouble(tsBlockIndex_));
  case TSDataType::TEXT:
  case TSDataType::STRING:
  case TSDataType::OBJECT:
  case TSDataType::BLOB:
    return column->getBinary(tsBlockIndex_)->getStringValue();
  case TSDataType::DATE: {
    int32_t value = column->getInt(tsBlockIndex_);
    return parseIntToDate(value).toIsoExtendedString();
  }
  default: {
    if (tsDataType == TSDataType::DATE) {
      auto date = getDateByTsBlockColumnIndex(index);
      if (date.is_initialized()) {
        return date.value().toIsoExtendedString();
      }
    }
    return "";
  }
  }
}

Optional<int64_t> IoTDBRpcDataSet::getTimestampByIndex(int32_t columnIndex) {
  int32_t index = getTsBlockColumnIndexForColumnIndex(columnIndex);
  return getTimestampByTsBlockColumnIndex(index);
}

Optional<int64_t> IoTDBRpcDataSet::getTimestamp(const std::string &columnName) {
  int32_t index = getTsBlockColumnIndexForColumnName(columnName);
  return getTimestampByTsBlockColumnIndex(index);
}

Optional<int64_t>
IoTDBRpcDataSet::getTimestampByTsBlockColumnIndex(int32_t tsBlockColumnIndex) {
  return getLongByTsBlockColumnIndex(tsBlockColumnIndex);
}

Optional<IoTDBDate> IoTDBRpcDataSet::getDateByIndex(int32_t columnIndex) {
  int32_t index = getTsBlockColumnIndexForColumnIndex(columnIndex);
  return getDateByTsBlockColumnIndex(index);
}

Optional<IoTDBDate> IoTDBRpcDataSet::getDate(const std::string &columnName) {
  int32_t index = getTsBlockColumnIndexForColumnName(columnName);
  return getDateByTsBlockColumnIndex(index);
}

Optional<IoTDBDate>
IoTDBRpcDataSet::getDateByTsBlockColumnIndex(int32_t tsBlockColumnIndex) {
  checkRecord();
  if (tsBlockColumnIndex < 0) {
    throw IoTDBException("Cannot read date from time column");
  }
  if (isNull(tsBlockColumnIndex, tsBlockIndex_)) {
    lastReadWasNull_ = true;
    return Optional<IoTDBDate>::none();
  }
  lastReadWasNull_ = false;
  const auto column = curTsBlock_->getColumn(tsBlockColumnIndex);
  switch (column->getDataType()) {
  case TSDataType::INT32:
    return parseIntToDate(column->getInt(tsBlockIndex_));
  case TSDataType::INT64:
    return parseIntToDate(static_cast<int32_t>(column->getLong(tsBlockIndex_)));
  case TSDataType::DOUBLE:
    return parseIntToDate(
        static_cast<int32_t>(column->getDouble(tsBlockIndex_)));
  case TSDataType::FLOAT:
    return parseIntToDate(
        static_cast<int32_t>(column->getFloat(tsBlockIndex_)));
  case TSDataType::TEXT:
  case TSDataType::STRING:
  case TSDataType::BLOB:
  case TSDataType::OBJECT: {
    const auto binary = column->getBinary(tsBlockIndex_);
    const std::string &text = binary->getStringValue();
    if (text.empty()) {
      return IoTDBDate::notADate();
    }
    // Server may return DATE as packed int in text or ISO-8601 string.
    try {
      return parseIntToDate(static_cast<int32_t>(std::stol(text)));
    } catch (const std::exception &) {
      int year = 0, month = 0, day = 0;
      if (std::sscanf(text.c_str(), "%d-%d-%d", &year, &month, &day) == 3) {
        return IoTDBDate(year, month, day);
      }
      throw IoTDBException("Cannot parse DATE value: " + text);
    }
  }
  default:
    return parseIntToDate(
        getIntByTsBlockColumnIndex(tsBlockColumnIndex).value());
  }
}

TSDataType::TSDataType
IoTDBRpcDataSet::getDataTypeByIndex(int32_t columnIndex) {
  int32_t index = getTsBlockColumnIndexForColumnIndex(columnIndex);
  return getDataTypeByTsBlockColumnIndex(index);
}

TSDataType::TSDataType
IoTDBRpcDataSet::getDataType(const std::string &columnName) {
  int32_t index = getTsBlockColumnIndexForColumnName(columnName);
  return getDataTypeByTsBlockColumnIndex(index);
}

TSDataType::TSDataType
IoTDBRpcDataSet::getColumnPhysicalDataType(const std::string &columnName) {
  int32_t index = getTsBlockColumnIndexForColumnName(columnName);
  if (index < 0) {
    return TSDataType::TIMESTAMP;
  }
  checkRecord();
  return curTsBlock_->getColumn(index)->getDataType();
}

int32_t
IoTDBRpcDataSet::getTsBlockColumnIndexForColumnIndex(int32_t columnIndex) {
  const int32_t adjusted_index = columnIndex - 1;
  if (adjusted_index >=
          static_cast<int32_t>(columnIndex2TsBlockColumnIndexList_.size()) ||
      adjusted_index < 0) {
    throw std::out_of_range(
        "Index " + std::to_string(adjusted_index) + " out of range [0, " +
        std::to_string(columnIndex2TsBlockColumnIndexList_.size()) + ")");
  }
  return columnIndex2TsBlockColumnIndexList_[adjusted_index];
}

TSDataType::TSDataType
IoTDBRpcDataSet::getDataTypeByTsBlockColumnIndex(int32_t tsBlockColumnIndex) {
  if (tsBlockColumnIndex < 0) {
    return TSDataType::TIMESTAMP;
  } else {
    return dataTypeForTsBlockColumn_[tsBlockColumnIndex];
  }
}

int32_t IoTDBRpcDataSet::findColumn(const std::string &columnName) {
  auto it = columnOrdinalMap_.find(columnName);
  if (it != columnOrdinalMap_.end()) {
    return it->second;
  }
  return -1;
}

std::string IoTDBRpcDataSet::findColumnNameByIndex(int32_t columnIndex) {
  if (columnIndex <= 0) {
    throw IoTDBException("column index should start from 1");
  }
  if (columnIndex > static_cast<int32_t>(columnNameList_.size())) {
    throw IoTDBException("Column index " + std::to_string(columnIndex) +
                         " is out of range. Valid range is 0 to " +
                         std::to_string(columnNameList_.size() - 1));
  }
  return columnNameList_[columnIndex - 1];
}

int32_t IoTDBRpcDataSet::getTsBlockColumnIndexForColumnName(
    const std::string &columnName) {
  auto it = columnName2TsBlockColumnIndexMap_.find(columnName);
  if (it == columnName2TsBlockColumnIndexMap_.end()) {
    throw IoTDBException("unknown column name: " + columnName);
  }
  return it->second;
}

void IoTDBRpcDataSet::checkRecord() {
  if (queryResultIndex_ > queryResultSize_ || tsBlockIndex_ >= tsBlockSize_ ||
      queryResult_.empty() || !curTsBlock_) {
    throw IoTDBException("no record remains");
  }
}

int32_t IoTDBRpcDataSet::getValueColumnStartIndex() const {
  return getValueColumnNameListIndex(0);
}

int32_t IoTDBRpcDataSet::getServerColumnCount() const { return columnSize_; }

int32_t
IoTDBRpcDataSet::getValueColumnNameListIndex(int32_t valueColumnOrdinal) const {
  if (valueColumnOrdinal < 0 || valueColumnOrdinal >= columnSize_) {
    throw std::out_of_range(
        "value column ordinal " + std::to_string(valueColumnOrdinal) +
        " out of range [0, " + std::to_string(columnSize_) + ")");
  }
  if (!columnNameList_.empty() && columnNameList_[0] == TimestampColumnName) {
    return valueColumnOrdinal + 1;
  }
  return valueColumnOrdinal;
}

int32_t IoTDBRpcDataSet::getColumnSize() const {
  return static_cast<int32_t>(columnNameList_.size());
}

const std::vector<std::string> &IoTDBRpcDataSet::getColumnTypeList() const {
  return columnTypeList_;
}

const std::vector<std::string> &IoTDBRpcDataSet::getColumnNameList() const {
  return columnNameList_;
}

bool IoTDBRpcDataSet::isClosed() const { return isClosed_; }

int32_t IoTDBRpcDataSet::getFetchSize() const { return fetchSize_; }

void IoTDBRpcDataSet::setFetchSize(int32_t fetchSize) {
  fetchSize_ = fetchSize;
}

bool IoTDBRpcDataSet::hasCachedRecord() const { return hasCachedRecord_; }

void IoTDBRpcDataSet::setHasCachedRecord(bool hasCachedRecord) {
  hasCachedRecord_ = hasCachedRecord;
}

bool IoTDBRpcDataSet::isLastReadWasNull() const { return lastReadWasNull_; }

int64_t IoTDBRpcDataSet::getCurrentRowTime() const { return time_; }

bool IoTDBRpcDataSet::isIgnoreTimestamp() const { return ignoreTimestamp_; }

bool IoTDBRpcDataSet::hasCachedBlock() const {
  return curTsBlock_ && tsBlockIndex_ < tsBlockSize_ - 1;
}

bool IoTDBRpcDataSet::hasCachedByteBuffer() const {
  return !queryResult_.empty() && queryResultIndex_ < queryResultSize_;
}
