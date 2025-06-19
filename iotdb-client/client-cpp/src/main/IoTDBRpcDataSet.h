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

#ifndef IOTDB_RPC_DATA_SET_H
#define IOTDB_RPC_DATA_SET_H

#include <vector>
#include <string>
#include <map>
#include <memory>
#include <cstdint>
#include "IClientRPCService.h"
#include <boost/date_time/gregorian/gregorian.hpp>
#include "TsBlock.h"

class IoTDBRpcDataSet {
public:
    static const int32_t startIndex;
    static const std::string TimestampColumnName;

    static const std::string DEFAULT_TIME_FORMAT;
    static const std::string TIME_PRECISION;
    static const std::string MILLISECOND;
    static const std::string MICROSECOND;
    static const std::string NANOSECOND;

    IoTDBRpcDataSet(const std::string& sql,
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
                    const std::string& timeFormat,
                    int32_t timeFactor,
                    std::vector<int32_t>& columnIndex2TsBlockColumnIndexList);

    ~IoTDBRpcDataSet();

    bool next();
    void close(bool forceClose = false);

    bool hasCachedBlock() const;
    bool hasCachedByteBuffer() const;

    bool isNull(int32_t index, int32_t rowNum);
    bool isNullByIndex(int32_t columnIndex);
    bool isNullByColumnName(const std::string& columnName);
    boost::optional<bool> getBooleanByIndex(int32_t columnIndex);
    boost::optional<bool> getBoolean(const std::string& columnName);
    boost::optional<double> getDoubleByIndex(int32_t columnIndex);
    boost::optional<double> getDouble(const std::string& columnName);
    boost::optional<float> getFloatByIndex(int32_t columnIndex);
    boost::optional<float> getFloat(const std::string& columnName);
    boost::optional<int32_t> getIntByIndex(int32_t columnIndex);
    boost::optional<int32_t> getInt(const std::string& columnName);
    boost::optional<int64_t> getLongByIndex(int32_t columnIndex);
    boost::optional<int64_t> getLong(const std::string& columnName);
    std::shared_ptr<Binary> getBinaryByIndex(int32_t columnIndex);
    std::shared_ptr<Binary> getBinary(const std::string& columnName);
    boost::optional<std::string> getStringByIndex(int32_t columnIndex);
    boost::optional<std::string> getString(const std::string& columnName);
    int64_t getTimestampByIndex(int32_t columnIndex);
    int64_t getTimestamp(const std::string& columnName);
    boost::optional<boost::gregorian::date> getDateByIndex(int32_t columnIndex);
    boost::optional<boost::gregorian::date> getDate(const std::string& columnName);

    TSDataType::TSDataType getDataTypeByIndex(int32_t columnIndex);
    TSDataType::TSDataType getDataType(const std::string& columnName);
    int32_t findColumn(const std::string& columnName);
    std::string findColumnNameByIndex(int32_t columnIndex);
    int32_t getValueColumnStartIndex() const;
    int32_t getColumnSize() const;
    const std::vector<std::string>& getColumnTypeList() const;
    const std::vector<std::string>& getColumnNameList() const;
    bool isClosed() const;
    int32_t getFetchSize() const;
    void setFetchSize(int32_t fetchSize);
    bool hasCachedRecord() const;
    void setHasCachedRecord(bool hasCachedRecord);
    bool isLastReadWasNull() const;
    int64_t getCurrentRowTime() const;
    bool isIgnoreTimestamp() const;

private:
    bool fetchResults();
    void constructOneRow();
    void constructOneTsBlock();
    int32_t getTsBlockColumnIndexForColumnName(const std::string& columnName);
    int32_t getTsBlockColumnIndexForColumnIndex(int32_t columnIndex);
    void checkRecord();
    TSDataType::TSDataType getDataTypeByTsBlockColumnIndex(int32_t tsBlockColumnIndex);
    boost::optional<bool> getBooleanByTsBlockColumnIndex(int32_t tsBlockColumnIndex);
    std::string getStringByTsBlockColumnIndexAndDataType(int32_t index, TSDataType::TSDataType tsDataType);
    boost::optional<double> getDoubleByTsBlockColumnIndex(int32_t tsBlockColumnIndex);
    boost::optional<float> getFloatByTsBlockColumnIndex(int32_t tsBlockColumnIndex);
    boost::optional<int32_t> getIntByTsBlockColumnIndex(int32_t tsBlockColumnIndex);
    boost::optional<int64_t> getLongByTsBlockColumnIndex(int32_t tsBlockColumnIndex);
    std::shared_ptr<Binary> getBinaryByTsBlockColumnIndex(int32_t tsBlockColumnIndex);
    boost::optional<std::string> getStringByTsBlockColumnIndex(int32_t tsBlockColumnIndex);
    boost::optional<boost::gregorian::date> getDateByTsBlockColumnIndex(int32_t tsBlockColumnIndex);
    int64_t getTimestampByTsBlockColumnIndex(int32_t tsBlockColumnIndex);

    std::string sql_;
    bool isClosed_;
    std::shared_ptr<IClientRPCServiceClient> client_;
    std::vector<std::string> columnNameList_;
    std::vector<std::string> columnTypeList_;
    std::map<std::string, int32_t> columnOrdinalMap_;
    std::map<std::string, int32_t> columnName2TsBlockColumnIndexMap_;
    std::vector<int32_t> columnIndex2TsBlockColumnIndexList_;
    std::vector<TSDataType::TSDataType> dataTypeForTsBlockColumn_;
    int32_t fetchSize_;
    int64_t timeout_;
    bool hasCachedRecord_;
    bool lastReadWasNull_;
    int32_t columnSize_;
    int64_t sessionId_;
    int64_t queryId_;
    int64_t statementId_;
    int64_t time_;
    bool ignoreTimestamp_;
    bool moreData_;
    std::vector<std::string> queryResult_;
    std::shared_ptr<TsBlock> curTsBlock_;
    int32_t queryResultSize_;
    int32_t queryResultIndex_;
    int32_t tsBlockSize_;
    int32_t tsBlockIndex_;
    std::string timeZoneId_;
    std::string timeFormat_;
    int32_t timeFactor_;
    std::string timePrecision_;
};

#endif // IOTDB_RPC_DATA_SET_H
