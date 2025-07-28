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

#ifndef IOTDB_SESSION_DATA_SET_H
#define IOTDB_SESSION_DATA_SET_H

#include <memory>
#include <string>
#include <vector>
#include <map>
#include <boost/date_time/gregorian/gregorian.hpp>
#include "IoTDBRpcDataSet.h"
#include "Column.h"

class RowRecord {
public:
    int64_t timestamp;
    std::vector<Field> fields;

    explicit RowRecord(int64_t timestamp);
    RowRecord(int64_t timestamp, const std::vector<Field>& fields);
    explicit RowRecord(const std::vector<Field>& fields);
    RowRecord();

    void addField(const Field& f);
    std::string toString();
};

class SessionDataSet {
public:
    SessionDataSet(const std::string& sql,
                   const std::vector<std::string>& columnNameList,
                   const std::vector<std::string>& columnTypeList,
                   const std::map<std::string, int32_t>& columnNameIndex,
                   int64_t queryId,
                   int64_t statementId,
                   std::shared_ptr<IClientRPCServiceClient> client,
                   int64_t sessionId,
                   const std::vector<std::string>& queryResult,
                   bool ignoreTimestamp,
                   int64_t timeout,
                   bool moreData,
                   int32_t fetchSize,
                   const std::string& zoneId,
                   int32_t timeFactor,
                   std::vector<int32_t>& columnIndex2TsBlockColumnIndexList) {
        iotdbRpcDataSet_ = std::make_shared<IoTDBRpcDataSet>(sql,
                                                             columnNameList,
                                                             columnTypeList,
                                                             columnNameIndex,
                                                             ignoreTimestamp,
                                                             moreData,
                                                             queryId,
                                                             statementId,
                                                             client,
                                                             sessionId,
                                                             queryResult,
                                                             fetchSize,
                                                             timeout,
                                                             zoneId,
                                                             IoTDBRpcDataSet::DEFAULT_TIME_FORMAT,
                                                             timeFactor,
                                                             columnIndex2TsBlockColumnIndexList);
    }

    ~SessionDataSet() = default;

    bool hasNext();
    shared_ptr<RowRecord> next();

    int getFetchSize();
    void setFetchSize(int fetchSize);

    const std::vector<std::string>& getColumnNames() const;
    const std::vector<std::string>& getColumnTypeList() const;
    void closeOperationHandle(bool forceClose = false);

    class DataIterator {
        std::shared_ptr<IoTDBRpcDataSet> iotdbRpcDataSet_;

    public:
        DataIterator(std::shared_ptr<IoTDBRpcDataSet> iotdbRpcDataSet) :
            iotdbRpcDataSet_(iotdbRpcDataSet) {
        }

        bool next();

        bool isNull(const std::string& columnName);
        bool isNullByIndex(int32_t columnIndex);

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

        boost::optional<std::string> getStringByIndex(int32_t columnIndex);
        boost::optional<std::string> getString(const std::string& columnName);

        boost::optional<int64_t> getTimestampByIndex(int32_t columnIndex);
        boost::optional<int64_t> getTimestamp(const std::string& columnName);

        boost::optional<boost::gregorian::date> getDateByIndex(int32_t columnIndex);
        boost::optional<boost::gregorian::date> getDate(const std::string& columnName);

        int32_t findColumn(const std::string& columnName);
        const std::vector<std::string>& getColumnNames() const;
        const std::vector<std::string>& getColumnTypeList() const;
    };

    DataIterator getIterator() {
        return {iotdbRpcDataSet_};
    };

private:
    std::shared_ptr<RowRecord> constructRowRecordFromValueArray();

    std::shared_ptr<IoTDBRpcDataSet> iotdbRpcDataSet_;
};

#endif
