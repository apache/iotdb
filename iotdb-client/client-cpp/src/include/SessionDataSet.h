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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "Column.h"
#include "Date.h"
#include "Optional.h"

class RowRecord {
public:
  int64_t timestamp;
  std::vector<Field> fields;

  explicit RowRecord(int64_t timestamp);
  RowRecord(int64_t timestamp, const std::vector<Field> &fields);
  explicit RowRecord(const std::vector<Field> &fields);
  RowRecord();

  void addField(const Field &f);
  std::string toString();
};

class Session;

class SessionDataSet {
  struct Impl;
  std::unique_ptr<Impl> impl_;
  SessionDataSet() = default;
  friend class Session;
  friend std::unique_ptr<SessionDataSet> createSessionDataSet(
      const std::string &sql, const std::vector<std::string> &columnNameList,
      const std::vector<std::string> &columnTypeList,
      const std::map<std::string, int32_t> &columnNameIndex, int64_t queryId,
      int64_t statementId,
      std::shared_ptr<class IClientRPCServiceClient> client, int64_t sessionId,
      const std::vector<std::string> &queryResult, bool ignoreTimestamp,
      int64_t timeout, bool moreData, int32_t fetchSize,
      const std::string &zoneId);

private:
  std::shared_ptr<RowRecord> constructRowRecordFromValueArray();

public:
  ~SessionDataSet();

  bool hasNext();
  std::shared_ptr<RowRecord> next();

  int getFetchSize();
  void setFetchSize(int fetchSize);

  const std::vector<std::string> &getColumnNames() const;
  const std::vector<std::string> &getColumnTypeList() const;
  void closeOperationHandle(bool forceClose = false);

  class DataIterator {
    std::shared_ptr<Impl> impl_;

  public:
    explicit DataIterator(std::shared_ptr<Impl> impl);

    bool next();

    bool isNull(const std::string &columnName);
    bool isNullByIndex(int32_t columnIndex);

    Optional<bool> getBooleanByIndex(int32_t columnIndex);
    Optional<bool> getBoolean(const std::string &columnName);

    Optional<double> getDoubleByIndex(int32_t columnIndex);
    Optional<double> getDouble(const std::string &columnName);

    Optional<float> getFloatByIndex(int32_t columnIndex);
    Optional<float> getFloat(const std::string &columnName);

    Optional<int32_t> getIntByIndex(int32_t columnIndex);
    Optional<int32_t> getInt(const std::string &columnName);

    Optional<int64_t> getLongByIndex(int32_t columnIndex);
    Optional<int64_t> getLong(const std::string &columnName);

    Optional<std::string> getStringByIndex(int32_t columnIndex);
    Optional<std::string> getString(const std::string &columnName);

    Optional<int64_t> getTimestampByIndex(int32_t columnIndex);
    Optional<int64_t> getTimestamp(const std::string &columnName);

    Optional<IoTDBDate> getDateByIndex(int32_t columnIndex);
    Optional<IoTDBDate> getDate(const std::string &columnName);

    int32_t findColumn(const std::string &columnName);
    const std::vector<std::string> &getColumnNames() const;
    const std::vector<std::string> &getColumnTypeList() const;
  };

  // The returned iterator must not outlive this SessionDataSet.
  DataIterator getIterator();
};

#endif
