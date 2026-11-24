/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#ifndef DESCRIPTORHANDLE_H
#define DESCRIPTORHANDLE_H

#include "ODBCHandle.h"
#include "ConnectionHandle.h"

class ConnectionHandle;
class StatementHandle;

enum class DescriptorRole {
  EXPLICIT,
  APPLICATION_ROW,
  IMPLEMENTATION_ROW,
  APPLICATION_PARAM,
  IMPLEMENTATION_PARAM
};

struct DescriptorRecord {
  void setConciseType(SQLSMALLINT value) {
    conciseType = type = value;
    datetimeIntervalCode = 0;
    if (value == SQL_TYPE_DATE || value == SQL_TYPE_TIME || value == SQL_TYPE_TIMESTAMP) {
      type = SQL_DATETIME;
      datetimeIntervalCode = value - SQL_TYPE_DATE + SQL_CODE_DATE;
    }
  }

  void updateConciseType() {
    conciseType = type;
    if (type == SQL_DATETIME && datetimeIntervalCode >= SQL_CODE_DATE &&
        datetimeIntervalCode <= SQL_CODE_TIMESTAMP)
      conciseType = SQL_TYPE_DATE + datetimeIntervalCode - SQL_CODE_DATE;
  }

  SQLSMALLINT conciseType = SQL_C_DEFAULT;
  SQLSMALLINT type = SQL_C_DEFAULT;
  SQLSMALLINT datetimeIntervalCode = 0;
  SQLLEN octetLength = 0;
  SQLULEN length = 0;
  SQLSMALLINT precision = 0;
  SQLSMALLINT scale = 0;
  SQLSMALLINT nullable = SQL_NULLABLE_UNKNOWN;
  SQLSMALLINT parameterType = SQL_PARAM_INPUT;
  SQLSMALLINT unnamed = SQL_UNNAMED;
  SQLINTEGER numPrecRadix = 0;
  SQLPOINTER dataPtr = nullptr;
  SQLLEN* indicatorPtr = nullptr;
  SQLLEN* octetLengthPtr = nullptr;
  std::string name;
};

class DescriptorHandle : public ODBCHandle {
public:
  DescriptorHandle(ConnectionHandle* connection, DescriptorRole role,
                   StatementHandle* owner = nullptr)
      : ODBCHandle(SQL_HANDLE_DESC), connection(connection), owner(owner), role(role),
        implicit(role != DescriptorRole::EXPLICIT) {
    if (connection)
      connection->descriptors.push_back(this);
  }

  ~DescriptorHandle() {
    if (connection) {
      auto& descriptors = connection->descriptors;
      descriptors.erase(std::remove(descriptors.begin(), descriptors.end(), this),
                        descriptors.end());
    }
  }

  DescriptorRecord& record(SQLSMALLINT number) {
    if (records.size() < static_cast<size_t>(number))
      records.resize(static_cast<size_t>(number));
    return records[static_cast<size_t>(number - 1)];
  }

  const DescriptorRecord* findRecord(SQLSMALLINT number) const {
    return number > 0 && static_cast<size_t>(number) <= records.size()
               ? &records[static_cast<size_t>(number - 1)]
               : nullptr;
  }

  DescriptorRecord* findRecord(SQLSMALLINT number) {
    return number > 0 && static_cast<size_t>(number) <= records.size()
               ? &records[static_cast<size_t>(number - 1)]
               : nullptr;
  }

  ConnectionHandle* connection;
  StatementHandle* owner;
  DescriptorRole role;
  bool implicit;
  SQLULEN arraySize = 1;
  SQLUSMALLINT* arrayStatusPtr = nullptr;
  SQLLEN* bindOffsetPtr = nullptr;
  SQLULEN bindType = SQL_BIND_BY_COLUMN;
  SQLSMALLINT count = 0;
  SQLULEN* rowsProcessedPtr = nullptr;
  std::vector<DescriptorRecord> records;
};

#endif // DESCRIPTORHANDLE_H
