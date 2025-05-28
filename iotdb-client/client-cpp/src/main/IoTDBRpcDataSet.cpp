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
#include <ctime>
#include <stdexcept>

#include "IoTDBRpcDataSet.h"
#include "Column.h"

const int32_t IoTDBRpcDataSet::START_INDEX = 2;
const std::string IoTDBRpcDataSet::TIMESTAMP_STR = "Time";
const std::string IoTDBRpcDataSet::DEFAULT_TIME_FORMAT = "default";

IoTDBRpcDataSet::IoTDBRpcDataSet(const std::string& sql,
                                 const std::vector<std::string>& columnNameList,
                                 const std::vector<std::string>& columnTypeList,
                                 const std::map<std::string, int32_t>& columnNameIndex,
                                 bool ignoreTimestamp,
                                 bool moreData,
                                 int64_t queryId,
                                 int64_t statementId,
                                 std::shared_ptr<IClientRPCServiceClient> client,
                                 int64_t sessionId,
                                 const std::vector<std::string>& queryResult,
                                 int32_t fetchSize,
                                 int64_t timeout,
                                 const std::string& zoneId,
                                 const std::string& timeFormat)
    : sql_(sql),
      isClosed_(false),
      client_(client),
      fetchSize_(fetchSize),
      timeout_(timeout),
      hasCachedRecord_(false),
      lastReadWasNull_(false),
      columnSize_(static_cast<int32_t>(columnNameList.size())),
      sessionId_(sessionId),
      queryId_(queryId),
      statementId_(statementId),
      time_(0),
      ignoreTimestamp_(ignoreTimestamp),
      moreData_(moreData),
      queryResult_(queryResult),
      curTsBlock_(nullptr),
      queryResultSize_(static_cast<int32_t>(queryResult.size())),
      queryResultIndex_(0),
      tsBlockSize_(0),
      tsBlockIndex_(-1),
      timeZoneId_(zoneId),
      timeFormat_(timeFormat) {
    if (!ignoreTimestamp) {
        columnNameList_.push_back(TIMESTAMP_STR);
        columnTypeList_.emplace_back("INT64");
        columnOrdinalMap_[TIMESTAMP_STR] = 1;
    }

    // Process column names and types
    if (!columnNameIndex.empty()) {
        // Deduplicate column types
        std::set<int32_t> uniqueValues;
        for (const auto& entry : columnNameIndex) {
            uniqueValues.insert(entry.second);
        }
        int deduplicatedColumnSize = static_cast<int>(uniqueValues.size());
        columnTypeDeduplicatedList_.resize(deduplicatedColumnSize);
        for (size_t i = 0; i < columnNameList.size(); ++i) {
            const std::string& name = columnNameList[i];
            columnNameList_.push_back(name);
            columnTypeList_.push_back(columnTypeList[i]);
            // Update ordinal map and deduplicated types
            if (!columnOrdinalMap_.count(name)) {
                int index = columnNameIndex.at(name);
                if (std::none_of(columnOrdinalMap_.begin(), columnOrdinalMap_.end(),
                                 [index](const std::pair<const std::string, int32_t>& entry) {
                                     return entry.second == (index + START_INDEX);
                                 })) {
                    columnTypeDeduplicatedList_[index] = getDataTypeByStr(columnTypeList[i]);
                }
                columnOrdinalMap_[name] = index + START_INDEX;
            }
        }
    }
    else {
        // Handle case without column name index
        int32_t currentIndex = START_INDEX;
        for (size_t i = 0; i < columnNameList.size(); ++i) {
            std::string name = columnNameList[i];
            columnNameList_.push_back(name);
            columnTypeList_.push_back(columnTypeList[i]);
            if (!columnOrdinalMap_.count(name)) {
                columnOrdinalMap_[name] = currentIndex++;
                columnTypeDeduplicatedList_.push_back(getDataTypeByStr(columnTypeList[i]));
            }
        }
    }

    columnSize_ = static_cast<int32_t>(columnNameList_.size());
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
    }
    catch (const TTransportException& e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException& e) {
        log_debug(e.what());
        throw;
    } catch (exception& e) {
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
    if (!resp.hasResultSet) {
        close();
    }
    else {
        queryResult_ = resp.queryResult;
        queryResultIndex_ = 0;
        if (!queryResult_.empty()) {
            queryResultSize_ = queryResult_.size();
        }
        else {
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
    const auto& curTsBlockBytes = queryResult_[queryResultIndex_];
    queryResultIndex_++;
    curTsBlock_ = TsBlock::deserialize(curTsBlockBytes);
    tsBlockIndex_ = -1;
    tsBlockSize_ = curTsBlock_->getPositionCount();
}

bool IoTDBRpcDataSet::isNullByIndex(int32_t columnIndex) {
    int index = columnOrdinalMap_[findColumnNameByIndex(columnIndex)] - START_INDEX;
    // time column will never be null
    if (index < 0) {
        return false;
    }
    return isNull(index, tsBlockIndex_);
}

bool IoTDBRpcDataSet::isNullByColumnName(const std::string& columnName) {
    int index = columnOrdinalMap_[columnName] - START_INDEX;
    // time column will never be null
    if (index < 0) {
        return false;
    }
    return isNull(index, tsBlockIndex_);
}

bool IoTDBRpcDataSet::isNull(int32_t index, int32_t rowNum) {
    return index >= 0 && curTsBlock_->getColumn(index)->isNull(rowNum);
}

bool IoTDBRpcDataSet::getBooleanByIndex(int32_t columnIndex) {
    return getBoolean(findColumnNameByIndex(columnIndex));
}

bool IoTDBRpcDataSet::getBoolean(const std::string& columnName) {
    int index = columnOrdinalMap_[columnName] - START_INDEX;
    return getBooleanByTsBlockColumnIndex(index);
}

bool IoTDBRpcDataSet::getBooleanByTsBlockColumnIndex(int32_t tsBlockColumnIndex) {
    checkRecord();
    if (!isNull(tsBlockColumnIndex, tsBlockIndex_)) {
        lastReadWasNull_ = false;
        return curTsBlock_->getColumn(tsBlockColumnIndex)->getBoolean(tsBlockIndex_);
    }
    else {
        lastReadWasNull_ = true;
        return false;
    }
}

double IoTDBRpcDataSet::getDoubleByIndex(int32_t columnIndex) {
    return getDouble(findColumnNameByIndex(columnIndex));
}

double IoTDBRpcDataSet::getDouble(const std::string& columnName) {
    int index = columnOrdinalMap_[columnName] - START_INDEX;
    return getDoubleByTsBlockColumnIndex(index);
}

double IoTDBRpcDataSet::getDoubleByTsBlockColumnIndex(int32_t tsBlockColumnIndex) {
    checkRecord();
    if (!isNull(tsBlockColumnIndex, tsBlockIndex_)) {
        lastReadWasNull_ = false;
        return curTsBlock_->getColumn(tsBlockColumnIndex)->getDouble(tsBlockIndex_);
    }
    else {
        lastReadWasNull_ = true;
        return 0.0;
    }
}

float IoTDBRpcDataSet::getFloatByIndex(int32_t columnIndex) {
    return getFloat(findColumnNameByIndex(columnIndex));
}

float IoTDBRpcDataSet::getFloat(const std::string& columnName) {
    int index = columnOrdinalMap_[columnName] - START_INDEX;
    return getFloatByTsBlockColumnIndex(index);
}

float IoTDBRpcDataSet::getFloatByTsBlockColumnIndex(int32_t tsBlockColumnIndex) {
    checkRecord();
    if (!isNull(tsBlockColumnIndex, tsBlockIndex_)) {
        lastReadWasNull_ = false;
        return curTsBlock_->getColumn(tsBlockColumnIndex)->getFloat(tsBlockIndex_);
    }
    else {
        lastReadWasNull_ = true;
        return 0.0f;
    }
}

int32_t IoTDBRpcDataSet::getIntByIndex(int32_t columnIndex) {
    return getInt(findColumnNameByIndex(columnIndex));
}

int32_t IoTDBRpcDataSet::getInt(const std::string& columnName) {
    int index = columnOrdinalMap_[columnName] - START_INDEX;
    return getIntByTsBlockColumnIndex(index);
}

int32_t IoTDBRpcDataSet::getIntByTsBlockColumnIndex(int32_t tsBlockColumnIndex) {
    checkRecord();
    if (!isNull(tsBlockColumnIndex, tsBlockIndex_)) {
        lastReadWasNull_ = false;
        TSDataType::TSDataType dataType = curTsBlock_->getColumn(tsBlockColumnIndex)->getDataType();
        if (dataType == TSDataType::INT64) {
            return static_cast<int32_t>(curTsBlock_->getColumn(tsBlockColumnIndex)->getLong(tsBlockIndex_));
        }
        return curTsBlock_->getColumn(tsBlockColumnIndex)->getInt(tsBlockIndex_);
    }
    else {
        lastReadWasNull_ = true;
        return 0;
    }
}

int64_t IoTDBRpcDataSet::getLongByIndex(int32_t columnIndex) {
    return getLong(findColumnNameByIndex(columnIndex));
}

int64_t IoTDBRpcDataSet::getLong(const std::string& columnName) {
    int index = columnOrdinalMap_[columnName] - START_INDEX;
    return getLongByTsBlockColumnIndex(index);
}

int64_t IoTDBRpcDataSet::getLongByTsBlockColumnIndex(int32_t tsBlockColumnIndex) {
    checkRecord();
    if (tsBlockColumnIndex < 0) {
        lastReadWasNull_ = false;
        return curTsBlock_->getTimeByIndex(tsBlockIndex_);
    }
    if (!isNull(tsBlockColumnIndex, tsBlockIndex_)) {
        lastReadWasNull_ = false;
        TSDataType::TSDataType dataType = curTsBlock_->getColumn(tsBlockColumnIndex)->getDataType();
        if (dataType == TSDataType::INT32) {
            return static_cast<int64_t>(curTsBlock_->getColumn(tsBlockColumnIndex)->getInt(tsBlockIndex_));
        }
        return curTsBlock_->getColumn(tsBlockColumnIndex)->getLong(tsBlockIndex_);
    }
    else {
        lastReadWasNull_ = true;
        return 0;
    }
}

std::shared_ptr<Binary> IoTDBRpcDataSet::getBinaryByIndex(int32_t columnIndex) {
    return getBinary(findColumnNameByIndex(columnIndex));
}

std::shared_ptr<Binary> IoTDBRpcDataSet::getBinary(const std::string& columnName) {
    int index = columnOrdinalMap_[columnName] - START_INDEX;
    return getBinaryByTsBlockColumnIndex(index);
}

std::shared_ptr<Binary> IoTDBRpcDataSet::getBinaryByTsBlockColumnIndex(int32_t tsBlockColumnIndex) {
    checkRecord();
    if (!isNull(tsBlockColumnIndex, tsBlockIndex_)) {
        lastReadWasNull_ = false;
        return curTsBlock_->getColumn(tsBlockColumnIndex)->getBinary(tsBlockIndex_);
    }
    else {
        lastReadWasNull_ = true;
        return nullptr;
    }
}

std::string IoTDBRpcDataSet::getStringByIndex(int32_t columnIndex) {
    return getString(findColumnNameByIndex(columnIndex));
}

std::string IoTDBRpcDataSet::getString(const std::string& columnName) {
    int index = columnOrdinalMap_[columnName] - START_INDEX;
    return getStringByTsBlockColumnIndex(index);
}

std::string IoTDBRpcDataSet::getStringByTsBlockColumnIndex(int32_t tsBlockColumnIndex) {
    checkRecord();
    if (tsBlockColumnIndex == -1) {
        int64_t timestamp = curTsBlock_->getTimeByIndex(tsBlockIndex_);
        return std::to_string(timestamp);
    }
    if (isNull(tsBlockColumnIndex, tsBlockIndex_)) {
        lastReadWasNull_ = true;
        return "";
    }
    lastReadWasNull_ = false;
    return getStringByTsBlockColumnIndexAndDataType(tsBlockColumnIndex,
                                                    getDataTypeByIndex(tsBlockColumnIndex));
}

std::string IoTDBRpcDataSet::getStringByTsBlockColumnIndexAndDataType(int32_t index,
                                                                      TSDataType::TSDataType tsDataType) {
    switch (tsDataType) {
    case TSDataType::BOOLEAN:
        return std::to_string(curTsBlock_->getColumn(index)->getBoolean(tsBlockIndex_));
    case TSDataType::INT32:
        return std::to_string(curTsBlock_->getColumn(index)->getInt(tsBlockIndex_));
    case TSDataType::INT64:
        return std::to_string(curTsBlock_->getColumn(index)->getLong(tsBlockIndex_));
    case TSDataType::TIMESTAMP: {
        int64_t value = curTsBlock_->getColumn(index)->getLong(tsBlockIndex_);
        return formatDatetime(timeFormat_, timePrecision_, value, timeZoneId_);
    }
    case TSDataType::FLOAT:
        return std::to_string(curTsBlock_->getColumn(index)->getFloat(tsBlockIndex_));
    case TSDataType::DOUBLE:
        return std::to_string(curTsBlock_->getColumn(index)->getDouble(tsBlockIndex_));
    case TSDataType::TEXT:
    case TSDataType::STRING:
    case TSDataType::BLOB: {
        auto binary = curTsBlock_->getColumn(index)->getBinary(tsBlockIndex_);
        return binary->getStringValue();
    }
    case TSDataType::DATE: {
        int32_t value = curTsBlock_->getColumn(index)->getInt(tsBlockIndex_);
        auto date = parseIntToDate(value);
        return boost::gregorian::to_iso_extended_string(date);
    }
    default:
        return "";
    }
}

int64_t IoTDBRpcDataSet::getTimestampByIndex(int32_t columnIndex) {
    return getTimestamp(findColumnNameByIndex(columnIndex));
}

int64_t IoTDBRpcDataSet::getTimestamp(const std::string& columnName) {
    return getLong(columnName);
}

boost::gregorian::date IoTDBRpcDataSet::getDateByIndex(int32_t columnIndex) {
    return getDate(findColumnNameByIndex(columnIndex));
}

boost::gregorian::date IoTDBRpcDataSet::getDate(const std::string& columnName) {
    int32_t value = getInt(columnName);
    return parseIntToDate(value);
}

TSDataType::TSDataType IoTDBRpcDataSet::getDataTypeByIndex(int32_t columnIndex) {
    return getDataType(findColumnNameByIndex(columnIndex));
}

TSDataType::TSDataType IoTDBRpcDataSet::getDataType(const std::string& columnName) {
    if (columnName == TIMESTAMP_STR) {
        return TSDataType::INT64;
    }
    int index = columnOrdinalMap_[columnName] - START_INDEX;
    return index < 0 || index >= columnTypeDeduplicatedList_.size()
               ? TSDataType::UNKNOWN
               : columnTypeDeduplicatedList_[index];
}

int32_t IoTDBRpcDataSet::findColumn(const std::string& columnName) {
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
        throw IoTDBException(
            "Column index " + std::to_string(columnIndex) +
            " is out of range. Valid range is 0 to " +
            std::to_string(columnNameList_.size() - 1)
        );
    }
    return columnNameList_[columnIndex - 1];
}

void IoTDBRpcDataSet::checkRecord() {
    if (queryResultIndex_ > queryResultSize_ ||
        tsBlockIndex_ >= tsBlockSize_ ||
        queryResult_.empty() ||
        !curTsBlock_) {
        throw IoTDBException("no record remains");
    }
}

int32_t IoTDBRpcDataSet::getValueColumnStartIndex() const {
    return ignoreTimestamp_ ? 0 : 1;
}

int32_t IoTDBRpcDataSet::getColumnSize() const {
    return static_cast<int32_t>(columnNameList_.size());
}

const std::vector<std::string>& IoTDBRpcDataSet::getColumnTypeList() const {
    return columnTypeList_;
}

const std::vector<std::string>& IoTDBRpcDataSet::getColumnNameList() const {
    return columnNameList_;
}

bool IoTDBRpcDataSet::isClosed() const {
    return isClosed_;
}

int32_t IoTDBRpcDataSet::getFetchSize() const {
    return fetchSize_;
}

void IoTDBRpcDataSet::setFetchSize(int32_t fetchSize) {
    fetchSize_ = fetchSize;
}

bool IoTDBRpcDataSet::hasCachedRecord() const {
    return hasCachedRecord_;
}

void IoTDBRpcDataSet::setHasCachedRecord(bool hasCachedRecord) {
    hasCachedRecord_ = hasCachedRecord;
}

bool IoTDBRpcDataSet::isLastReadWasNull() const {
    return lastReadWasNull_;
}

int64_t IoTDBRpcDataSet::getCurrentRowTime() const {
    return time_;
}

bool IoTDBRpcDataSet::isIgnoreTimestamp() const {
    return ignoreTimestamp_;
}

bool IoTDBRpcDataSet::hasCachedBlock() const {
    return curTsBlock_ && tsBlockIndex_ < tsBlockSize_ - 1;
}

bool IoTDBRpcDataSet::hasCachedByteBuffer() const {
    return !queryResult_.empty() && queryResultIndex_ < queryResultSize_;
}
