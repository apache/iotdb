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
//
// Created by christoferdutz on 05.11.2024.
//

#ifndef STATEMENTHANDLE_H
#define STATEMENTHANDLE_H

#include "Pch.h"
#include "ODBCHandle.h"
#include "ConnectionHandle.h"
#include "DescriptorHandle.h"
#include "ODBCResultSet.h"
#include "ODBCSessionResultSet.h"
#include "ODBCRestResultSet.h"
#include <memory>

// IoTDB Session API includes

// Forward declarations to avoid circular dependencies
struct BindColInfo;

struct ParameterBinding {
  SQLSMALLINT inputOutputType = SQL_PARAM_INPUT;
  SQLSMALLINT valueType = SQL_C_DEFAULT;
  SQLSMALLINT parameterType = SQL_UNKNOWN_TYPE;
  SQLULEN columnSize = 0;
  SQLSMALLINT decimalDigits = 0;
  SQLPOINTER valuePtr = nullptr;
  SQLLEN bufferLength = 0;
  SQLLEN* indicatorPtr = nullptr;
  SQLLEN* octetLengthPtr = nullptr;
  std::vector<std::string> streamedData;
  std::vector<bool> streamedNull;
  bool bound = false;
};

class StatementHandle : public ODBCHandle {
public:
  StatementHandle(ConnectionHandle* connection)
      : ODBCHandle(SQL_HANDLE_STMT), appRowDesc(nullptr), impRowDesc(nullptr),
        appParamDesc(nullptr),
        impParamDesc(nullptr), // Added: Application and implementation parameter descriptors
        curRow(-1), isStream(false), isQuery(false), streamCurRow(0), connectionHandle(connection),
        resultSetPtr(nullptr),
        // Added: Cursor-related properties
        cursorType(SQL_CURSOR_FORWARD_ONLY), // Default to forward-only cursor
        concurrency(SQL_CONCUR_READ_ONLY),   // Default to read-only concurrency
        scrollable(SQL_NONSCROLLABLE),       // Default to non-scrollable
        sensitivity(SQL_UNSPECIFIED),        // Default to unspecified sensitivity
        useBookmarks(SQL_UB_OFF),            // Default to not using bookmarks
        // Added: Pointer-related properties
        bookmarkPtr(nullptr),                             // Bookmark pointer
        rowsFetchedPtr(nullptr),                          // Already exists, but kept for reference
        rowStatusPtr(nullptr), rowBindOffsetPtr(nullptr), // Row bind offset pointer
        paramsProcessedPtr(nullptr),                      // Processed parameters pointer
        paramStatusPtr(nullptr),                          // Parameter status pointer
        // Added: Size and quantity related properties
        rowArraySize(1), // Default row array size is 1
        paramSetSize(1), // Default parameter set size is 1
        maxRows(0),      // Default no maximum row limit
        maxLength(0),    // Default no maximum length limit
        rowBindType(SQL_BIND_BY_COLUMN), paramBindType(SQL_BIND_BY_COLUMN),
        // Added: Other boolean/flag properties
        noScan(SQL_NOSCAN_OFF),   // Default to enable scanning
        retrieveData(SQL_RD_ON),  // Default to retrieve data
        metadataId(SQL_FALSE),    // Default metadata ID mode off
        enableAutoIPD(SQL_FALSE), // Parameter descriptors are not populated automatically
        // Added: SQL Server specific properties
        paramFocus(0), // Default parameter focus is 0
        prepared(false), rowsReturned(0), lastGetDataRow(-1), lastGetDataCol(0) {
    implicitAppRowDesc = new DescriptorHandle(connection, DescriptorRole::APPLICATION_ROW, this);
    implicitImpRowDesc = new DescriptorHandle(connection, DescriptorRole::IMPLEMENTATION_ROW, this);
    implicitAppParamDesc =
        new DescriptorHandle(connection, DescriptorRole::APPLICATION_PARAM, this);
    implicitImpParamDesc =
        new DescriptorHandle(connection, DescriptorRole::IMPLEMENTATION_PARAM, this);
    appRowDesc = implicitAppRowDesc;
    impRowDesc = implicitImpRowDesc;
    appParamDesc = implicitAppParamDesc;
    impParamDesc = implicitImpParamDesc;
    cursorName = "SQL_CUR" + std::to_string(reinterpret_cast<uintptr_t>(this));
    if (connection)
      connection->statements.push_back(this);
  }

  ~StatementHandle() {
    if (connectionHandle) {
      auto& statements = connectionHandle->statements;
      statements.erase(std::remove(statements.begin(), statements.end(), this), statements.end());
    }
    ClearResultSet();
    delete implicitAppRowDesc;
    delete implicitImpRowDesc;
    delete implicitAppParamDesc;
    delete implicitImpParamDesc;
  }

  ConnectionHandle* getConnection() const {
    return connectionHandle;
  }

  void AllocateSessionResultSet();
  void AllocateRestResultSet();
  void ClearResultSet();

  std::string statementText;
  std::string cursorName;
  bool prepared;
  SQLULEN rowsReturned;
  std::vector<BindColInfo> columnBindings;
  std::vector<ParameterBinding> parameterBindings;
  bool needsParameterData = false;
  SQLULEN nextDataSet = 0;
  SQLULEN activeDataSet = std::numeric_limits<SQLULEN>::max();
  size_t nextDataParameter = 0;
  size_t activeDataParameter = std::numeric_limits<size_t>::max();
  SQLHANDLE appRowDesc;   // Application row descriptor (already exists)
  SQLHANDLE impRowDesc;   // Implementation row descriptor (already exists)
  SQLHANDLE appParamDesc; // Added: Application parameter descriptor
  SQLHANDLE impParamDesc; // Added: Implementation parameter descriptor
  DescriptorHandle* implicitAppRowDesc;
  DescriptorHandle* implicitImpRowDesc;
  DescriptorHandle* implicitAppParamDesc;
  DescriptorHandle* implicitImpParamDesc;

  std::shared_ptr<ODBCResultSet> resultSetPtr;
  int curRow;
  bool isStream; // Flag indicating whether the current statement is in streaming mode.
  bool isQuery;  // Flag indicating whether the current statement is a query statement.
  int streamCurRow; // Available in streaming mode, indicates how many to follow after OFFSET. Initially 0.
  // Add more fields if needed

  // Added: Cursor-related properties
  SQLINTEGER cursorType;   // Cursor type (SQL_CURSOR_FORWARD_ONLY, etc.)
  SQLINTEGER concurrency;  // Concurrency type (SQL_CONCUR_READ_ONLY, etc.)
  SQLINTEGER scrollable;   // Scrollable (SQL_SCROLLABLE/SQL_NONSCROLLABLE)
  SQLINTEGER sensitivity;  // Cursor sensitivity (SQL_SENSITIVE/SQL_INSENSITIVE, etc.)
  SQLINTEGER useBookmarks; // Bookmark usage (SQL_UB_OFF/SQL_UB_VARIABLE, etc.)

  // Added: Pointer-related properties
  SQLULEN* bookmarkPtr; // Bookmark pointer
  SQLULEN* rowsFetchedPtr;
  SQLUSMALLINT* rowStatusPtr;   // Status for the single supported row in a rowset
  SQLULEN* rowBindOffsetPtr;    // Row bind offset pointer
  SQLULEN* paramsProcessedPtr;  // Processed parameters pointer
  SQLUSMALLINT* paramStatusPtr; // Parameter status pointer
  SQLULEN* paramBindOffsetPtr = nullptr;

  // Added: Size and quantity related properties
  SQLULEN rowArraySize; // Row array size
  SQLULEN paramSetSize; // Parameter set size
  SQLULEN maxRows;      // Maximum number of rows to return
  SQLULEN maxLength;    // Maximum data length
  SQLULEN rowBindType;  // Column binding or row binding method
  SQLULEN paramBindType;

  // Added: Other boolean/flag properties
  SQLINTEGER noScan;        // Scan option (SQL_NOSCAN/SQL_UNNAMED)
  SQLINTEGER retrieveData;  // Data retrieval option (SQL_RD_ON/SQL_RD_OFF)
  SQLINTEGER metadataId;    // Metadata ID mode (SQL_TRUE/SQL_FALSE)
  SQLINTEGER enableAutoIPD; // Auto IPD enable (SQL_TRUE/SQL_FALSE)

  // Added: SQL Server specific properties
  SQLINTEGER paramFocus; // Parameter focus

  // SQLGetData tracking
  std::vector<SQLLEN> getDataOffsets;
  int lastGetDataRow;
  SQLUSMALLINT lastGetDataCol;

private:
  ConnectionHandle* connectionHandle;
};

// Function declarations
SQLSMALLINT streamNextBatch(StatementHandle* stmt);

#endif //STATEMENTHANDLE_H
