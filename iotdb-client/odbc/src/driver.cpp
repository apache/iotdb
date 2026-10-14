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
#include "ODBCField.h"
#include "Pch.h"
#include "driver.h"

// Additional includes not in Pch.h
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <codecvt> // Need to support wide characters

// ODBC includes
#include <odbcinst.h>
#include <sql.h>
#include <sqltypes.h>

// Project includes
#include "DiagnosticManager.h"
#include "Log.h"
#include "rest_api_client.h"
#include "session_api_client.h"
#include "StatementHandle.h"
#include "StatementUtils.h"
#include "ConnectionHandle.h"
#include "ConnectionString.h"
#include "EnvironmentHandle.h"
#include "CopyField.h"

static void PopulateImplementationRowDescriptor(StatementHandle* stmt);

/**
 * Copy std::string to ODBC string buffer
 * @param value input string
 * @param stringBuffer output buffer
 * @param bufferLength buffer size (in bytes)
 * @param stringLength returns actual string length (excluding null terminator)
 */
SQLRETURN setString(const std::string& value, SQLPOINTER stringBuffer, SQLSMALLINT bufferLength,
                    SQLSMALLINT* stringLength) {
  const SQLSMALLINT actualLength = static_cast<SQLSMALLINT>(
      std::min(value.size(), static_cast<size_t>(std::numeric_limits<SQLSMALLINT>::max())));

  if (stringLength) {
    *stringLength = actualLength;
  }

  if (stringBuffer && bufferLength > 0) {
    const size_t maxCopy = std::min(value.size(), static_cast<size_t>(bufferLength - 1));

    if (!value.empty()) {
      std::memcpy(stringBuffer, value.data(), maxCopy);
    }
    static_cast<char*>(stringBuffer)[maxCopy] = '\0';
    if (maxCopy < value.size())
      return SQL_SUCCESS_WITH_INFO;
  }
  return SQL_SUCCESS;
}

/**
 * Copy C-style string to ODBC string buffer
 * @param value input string (must be null-terminated)
 * @param stringBuffer output buffer
 * @param bufferLength buffer size (in bytes)
 * @param stringLength returns actual string length (excluding null terminator)
 */
SQLRETURN setString(const char* value, SQLPOINTER stringBuffer, SQLSMALLINT bufferLength,
                    SQLSMALLINT* stringLength) {
  if (!value)
    value = "";

  const size_t valueLen = std::strlen(value);
  if (stringLength) {
    *stringLength = static_cast<SQLSMALLINT>(
        std::min(valueLen, static_cast<size_t>(std::numeric_limits<SQLSMALLINT>::max())));
  }

  if (stringBuffer && bufferLength > 0) {
    size_t maxCopy = std::min(valueLen, static_cast<size_t>(bufferLength - 1));

    std::memcpy(stringBuffer, value, maxCopy);
    static_cast<char*>(stringBuffer)[maxCopy] = '\0';
    if (maxCopy < valueLen)
      return SQL_SUCCESS_WITH_INFO;
  }
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLAllocConnect(SQLHENV environmentHandle, SQLHDBC* connectionHandle) {
  logMessage("Enter SQLAllocConnect");
  // This is a deprecated function, but unixodbc needs to call it. Call SQLAllocHandle function in this function.
  SQLRETURN retcode = SQLAllocHandle(SQL_HANDLE_DBC, environmentHandle, connectionHandle);
  if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
    // Handle error
    std::ostringstream oss;
    oss << "SQLAllocConnect: Failed to allocate connection handle. retcode = " << retcode << "\n";
    logMessage(oss.str());
    return retcode;
  }
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLAllocEnv(SQLHENV* environmentHandle) {
  logMessage("SQLAllocEnv");
  // Deprecated function, delegate to SQLAllocHandle for env allocation.
  SQLRETURN retcode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, environmentHandle);
  if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
    std::ostringstream oss;
    oss << "SQLAllocEnv: Failed to allocate environment handle. retcode = " << retcode << "\n";
    logMessage(oss.str());
    return retcode;
  }
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLAllocHandle(const SQLSMALLINT handleType, const SQLHANDLE parentHandle,
                                 SQLHANDLE* outputHandle) {
  logMessage("SQLAllocHandle: Entering.");

  if (!outputHandle) {
    logMessage("SQLAllocHandle: outputHandle is null");
    return SQL_ERROR;
  }
  *outputHandle = nullptr;

  std::stringstream logStream;
  logStream << "Parameters: "
            << "handleType = " << handleType << ", parentHandle = " << handleToString(parentHandle);
  logMessage(logStream.str());

  try {
    switch (handleType) {
    case SQL_HANDLE_ENV:
      if (parentHandle != SQL_NULL_HANDLE)
        return SQL_INVALID_HANDLE;
      *outputHandle = new EnvironmentHandle();
      break;

    case SQL_HANDLE_DBC: {
      auto* env = static_cast<EnvironmentHandle*>(parentHandle);
      if (!env || env->getHandleType() != SQL_HANDLE_ENV || env->isHandleFreed())
        return SQL_INVALID_HANDLE;
      *outputHandle = new ConnectionHandle(env);
      break;
    }

    case SQL_HANDLE_STMT: {
      auto* cnct = static_cast<ConnectionHandle*>(parentHandle);
      if (!cnct || cnct->getHandleType() != SQL_HANDLE_DBC || cnct->isHandleFreed())
        return SQL_INVALID_HANDLE;
      auto* stmt = new StatementHandle(cnct);
      *outputHandle = stmt;
      break;
    }

    case SQL_HANDLE_DESC: {
      auto* cnct = static_cast<ConnectionHandle*>(parentHandle);
      if (!cnct || cnct->getHandleType() != SQL_HANDLE_DBC || cnct->isHandleFreed())
        return SQL_INVALID_HANDLE;
      *outputHandle = new DescriptorHandle(cnct, DescriptorRole::EXPLICIT);
      break;
    }

    default:
      logMessage("SQLAllocHandle: unsupported handle type");
      return SQL_ERROR;
    }
  } catch (const std::bad_alloc&) {
    if (parentHandle)
      static_cast<ODBCHandle*>(parentHandle)->addDiagnostic("HY001", "Memory allocation failed");
    return SQL_ERROR;
  } catch (const std::exception& error) {
    if (parentHandle)
      static_cast<ODBCHandle*>(parentHandle)->addDiagnostic("HY000", error.what());
    return SQL_ERROR;
  }

  logMessage("Allocated handle: " + handleToString(*outputHandle));
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLAllocStmt(SQLHDBC connectionHandle, SQLHSTMT* statementHandle) {
  logMessage("Entering SQLAllocStmt");
  if (!connectionHandle)
    return SQL_INVALID_HANDLE;
  if (!statementHandle) {
    static_cast<ConnectionHandle*>(connectionHandle)
        ->addDiagnostic("HY009", "Invalid use of null pointer");
    return SQL_ERROR;
  }

  // Deprecated function, delegate to SQLAllocHandle for stmt allocation.
  SQLRETURN retcode = SQLAllocHandle(SQL_HANDLE_STMT, connectionHandle, statementHandle);
  if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
    std::ostringstream oss;
    oss << "SQLAllocStmt: Failed to allocate statement handle. retcode = " << retcode << "\n";
    logMessage(oss.str());
    return retcode;
  }
  logMessage("Exiting SQLAllocStmt\n");
  return SQL_SUCCESS;
}

/*
The numbering of columnNumber starts from 1, with column 0 being the bookmark column. However, IoTDB does not have a bookmark column. The numbering still starts from 1.
*/
SQLRETURN SQL_API SQLBindCol(SQLHSTMT statementHandle, SQLUSMALLINT columnNumber,
                             SQLSMALLINT targetType, SQLPOINTER targetValuePtr, SQLLEN bufferLength,
                             SQLLEN* strLen_or_IndPtr) {
  const auto stmt = static_cast<StatementHandle*>(statementHandle);
  ConnectionHandle* cnct = stmt ? stmt->getConnection() : nullptr;
  logMessage(cnct, "SQLBindCol: Entering", LOG_LEVEL_TRACE);
  if (!stmt)
    return SQL_INVALID_HANDLE;
  stmt->clearDiagnostics();
  if (columnNumber == 0) {
    stmt->addDiagnostic("HYC00", "Bookmarks are not supported");
    return SQL_ERROR;
  }
  if (columnNumber > static_cast<SQLUSMALLINT>(std::numeric_limits<SQLSMALLINT>::max())) {
    stmt->addDiagnostic("07009", "Invalid descriptor index");
    return SQL_ERROR;
  }
  if (bufferLength < 0) {
    stmt->addDiagnostic("HY090", "Invalid string or buffer length");
    return SQL_ERROR;
  }
  if (isLogLevelEnabled(cnct, LOG_LEVEL_TRACE)) {
    std::stringstream logStream;
    logStream << "Parameters: "
              << "statementHandle = " << handleToString(statementHandle)
              << ", columnNumber = " << columnNumber << ", targetType = " << targetType
              << ", targetValue = " << targetValuePtr << ", bufferLength = " << bufferLength
              << ", strLen_or_Ind = " << strLen_or_IndPtr;

    logMessage(stmt->getConnection(), logStream.str(), LOG_LEVEL_TRACE);
  }

  if (stmt->columnBindings.size() <= columnNumber)
    stmt->columnBindings.resize(columnNumber + 1);

  BindColInfo& binding = stmt->columnBindings[columnNumber];
  if (!targetValuePtr && !strLen_or_IndPtr) {
    binding = BindColInfo();
    auto* ard = static_cast<DescriptorHandle*>(stmt->appRowDesc);
    if (DescriptorRecord* record = ard->findRecord(columnNumber)) {
      record->dataPtr = nullptr;
      record->octetLengthPtr = nullptr;
      record->indicatorPtr = nullptr;
    }
    if (stmt->resultSetPtr)
      stmt->resultSetPtr->bindColInfo = stmt->columnBindings;
    return SQL_SUCCESS;
  }

  SQLRETURN result = SQL_SUCCESS;
  if (stmt->resultSetPtr) {
    result = stmt->resultSetPtr->bindColumn(columnNumber, targetType, targetValuePtr, bufferLength,
                                            strLen_or_IndPtr);
    if (!SQL_SUCCEEDED(result))
      return result;
    stmt->columnBindings = stmt->resultSetPtr->bindColInfo;
  } else {
    binding.targetType = targetType;
    binding.targetValuePtr = targetValuePtr;
    binding.bufferLength = bufferLength;
    binding.strLen_or_IndPtr = strLen_or_IndPtr;
    binding.isBound = true;
  }

  auto* ard = static_cast<DescriptorHandle*>(stmt->appRowDesc);
  DescriptorRecord& record = ard->record(static_cast<SQLSMALLINT>(columnNumber));
  ard->count = std::max(ard->count, static_cast<SQLSMALLINT>(columnNumber));
  record.setConciseType(targetType);
  record.dataPtr = targetValuePtr;
  record.octetLength = bufferLength;
  record.octetLengthPtr = strLen_or_IndPtr;
  record.indicatorPtr = strLen_or_IndPtr;

  logMessage(stmt->getConnection(), "SQLBindCol: Exiting", LOG_LEVEL_TRACE);
  return result;
}

SQLRETURN SQL_API SQLBindParam(SQLHSTMT statementHandle, SQLUSMALLINT parameterNumber,
                               SQLSMALLINT valueType, SQLSMALLINT parameterType,
                               SQLULEN lengthPrecision, SQLSMALLINT parameterScale,
                               SQLPOINTER parameterValue, SQLLEN* strLen_or_Ind) {
  return SQLBindParameter(statementHandle, parameterNumber, SQL_PARAM_INPUT, valueType,
                          parameterType, lengthPrecision, parameterScale, parameterValue,
                          static_cast<SQLLEN>(lengthPrecision), strLen_or_Ind);
}

SQLRETURN SQL_API SQLBindParameter(SQLHSTMT statementHandle, SQLUSMALLINT parameterNumber,
                                   SQLSMALLINT inputOutputType, SQLSMALLINT valueType,
                                   SQLSMALLINT parameterType, SQLULEN columnSize,
                                   SQLSMALLINT decimalDigits, SQLPOINTER parameterValuePtr,
                                   SQLLEN bufferLength, SQLLEN* strLen_or_IndPtr) {
  if (!statementHandle)
    return SQL_INVALID_HANDLE;
  auto* stmt = static_cast<StatementHandle*>(statementHandle);
  stmt->clearDiagnostics();
  if (stmt->needsParameterData) {
    stmt->addDiagnostic("HY010", "Function sequence error");
    return SQL_ERROR;
  }
  if (parameterNumber == 0 ||
      parameterNumber > static_cast<SQLUSMALLINT>(std::numeric_limits<SQLSMALLINT>::max())) {
    stmt->addDiagnostic("07009", "Invalid parameter number");
    return SQL_ERROR;
  }
  if (inputOutputType != SQL_PARAM_INPUT) {
    stmt->addDiagnostic("HYC00", "Only input parameters are supported");
    return SQL_ERROR;
  }
  if (bufferLength < 0) {
    stmt->addDiagnostic("HY090", "Invalid string or buffer length");
    return SQL_ERROR;
  }
  if (stmt->parameterBindings.size() < parameterNumber)
    stmt->parameterBindings.resize(parameterNumber);
  ParameterBinding& binding = stmt->parameterBindings[parameterNumber - 1];
  binding.inputOutputType = inputOutputType;
  binding.valueType = valueType;
  binding.parameterType = parameterType;
  binding.columnSize = columnSize;
  binding.decimalDigits = decimalDigits;
  binding.valuePtr = parameterValuePtr;
  binding.bufferLength = bufferLength;
  binding.indicatorPtr = strLen_or_IndPtr;
  binding.streamedData.clear();
  binding.streamedNull.clear();
  binding.bound = true;

  auto* apd = static_cast<DescriptorHandle*>(stmt->appParamDesc);
  DescriptorRecord& appRecord = apd->record(static_cast<SQLSMALLINT>(parameterNumber));
  apd->count = std::max(apd->count, static_cast<SQLSMALLINT>(parameterNumber));
  appRecord.setConciseType(valueType);
  appRecord.dataPtr = parameterValuePtr;
  appRecord.octetLength = bufferLength;
  appRecord.octetLengthPtr = strLen_or_IndPtr;
  appRecord.indicatorPtr = strLen_or_IndPtr;
  auto* ipd = static_cast<DescriptorHandle*>(stmt->impParamDesc);
  DescriptorRecord& impRecord = ipd->record(static_cast<SQLSMALLINT>(parameterNumber));
  ipd->count = std::max(ipd->count, static_cast<SQLSMALLINT>(parameterNumber));
  impRecord.setConciseType(parameterType);
  impRecord.length = columnSize;
  impRecord.precision = static_cast<SQLSMALLINT>(
      std::min<SQLULEN>(columnSize, std::numeric_limits<SQLSMALLINT>::max()));
  impRecord.scale = decimalDigits;
  impRecord.parameterType = inputOutputType;
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLCancel(SQLHSTMT statementHandle) {
  if (!statementHandle) {
    logMessage(nullptr, "SQLCancel: Invalid statement handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  auto stmt = static_cast<StatementHandle*>(statementHandle);
  stmt->clearDiagnostics();
  stmt->needsParameterData = false;
  stmt->nextDataSet = 0;
  stmt->activeDataSet = std::numeric_limits<SQLULEN>::max();
  stmt->nextDataParameter = 0;
  stmt->activeDataParameter = std::numeric_limits<size_t>::max();
  for (auto& binding : stmt->parameterBindings) {
    binding.streamedData.clear();
    binding.streamedNull.clear();
  }
  // Other operations are synchronous. Once control returns to the application
  // there is no active operation to cancel.
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLCancelHandle(SQLSMALLINT handleType, SQLHANDLE inputHandle) {
  if (!inputHandle) {
    logMessage(nullptr, "SQLCancelHandle: Invalid handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  if (handleType == SQL_HANDLE_STMT)
    return SQLCancel(static_cast<SQLHSTMT>(inputHandle));
  if (handleType == SQL_HANDLE_DBC) {
    static_cast<ConnectionHandle*>(inputHandle)->clearDiagnostics();
    // All connection operations are synchronous, so there is no pending work.
    return SQL_SUCCESS;
  }
  logMessage("SQLCancelHandle: Invalid handle type");
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT statementHandle) {
  if (!statementHandle) {
    logMessage(nullptr, "SQLCloseCursor: Invalid statement handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  auto stmt = static_cast<StatementHandle*>(statementHandle);
  ConnectionHandle* cnct = stmt->getConnection();
  stmt->clearDiagnostics();
  if (!stmt->resultSetPtr) {
    stmt->addDiagnostic("24000", "Invalid cursor state");
    return SQL_ERROR;
  }
  stmt->ClearResultSet();
  stmt->curRow = -1;
  stmt->rowsReturned = 0;
  stmt->lastGetDataRow = -1;
  stmt->lastGetDataCol = 0;
  stmt->getDataOffsets.clear();
  logMessage(cnct, "SQLCloseCursor: Cursor closed", LOG_LEVEL_DEBUG);
  return SQL_SUCCESS;
}

// This function obtains the column name for the specified column number in the result set.
SQLRETURN getColName(StatementHandle* stmt, const SQLUSMALLINT columnNumber,
                     const SQLPOINTER stringBuffer, const SQLSMALLINT bufferLength,
                     SQLSMALLINT* stringLength) {
  ConnectionHandle* cnct = stmt->getConnection();
  logMessage(cnct, "Entering getColName", LOG_LEVEL_TRACE);

  // Use ODBCResultSet to get column names (columnNumber is 1-based, getColumnName uses 0-based)
  std::string columnExpression = stmt->resultSetPtr->getColumnName(columnNumber - 1);

  // Check if a valid column name is obtained
  if (columnExpression.empty() && columnNumber > stmt->resultSetPtr->getNumColumns()) {
    std::string errorMsg = "Requested column " + std::to_string(columnNumber) + " but only " +
                           std::to_string(stmt->resultSetPtr->getNumColumns()) +
                           " columns available";
    logMessage(cnct, errorMsg, LOG_LEVEL_ERROR);
    stmt->addDiagnostic("07009", "Invalid column number");
    return SQL_ERROR;
  }

  SQLSMALLINT actualLength = static_cast<SQLSMALLINT>(columnExpression.size());
  // Set StringLength to the length of the column name
  if (stringLength != nullptr) {
    *stringLength = actualLength;
    logMessage(cnct, "Set string length to " + std::to_string(*stringLength), LOG_LEVEL_DEBUG);
  }

  // Copy columnExpression to stringBuffer
  if (stringBuffer != nullptr) {
    SQLSMALLINT lengthToCopy = (bufferLength > 0)
                                   ? std::min(static_cast<SQLSMALLINT>(actualLength),
                                              static_cast<SQLSMALLINT>(bufferLength - 1))
                                   : 0;
    if (lengthToCopy > 0) {
      std::memcpy(stringBuffer, columnExpression.c_str(), lengthToCopy);
    }

    if (bufferLength > 0) {
      static_cast<char*>(stringBuffer)[lengthToCopy] = '\0';
    }

    if (lengthToCopy < actualLength) {
      std::string truncatedString =
          lengthToCopy == 0 ? "[Empty string]" : columnExpression.substr(0, lengthToCopy);
      logMessage(cnct,
                 "Returning Column name truncated: " + truncatedString + " (original: '" +
                     columnExpression + "')",
                 LOG_LEVEL_WARN);
    } else {
      logMessage(cnct, "Returning column name: " + std::string(static_cast<char*>(stringBuffer)),
                 LOG_LEVEL_DEBUG);
    }
  }

  logMessage(cnct, "Exiting getColName", LOG_LEVEL_TRACE);
  return SQL_SUCCESS;
}

// Map 10 IoTDB data types to ODBC data types, output result via numericAttribute.
void colTypeMapping(ConnectionHandle* cnct, std::string iotdbDataType,
                    const SQLUSMALLINT columnNumber, SQLSMALLINT* numericAttribute) {
  static const std::unordered_map<std::string, SQLSMALLINT> typeMap = {
      {"INT16", SQL_SMALLINT},    {"BOOLEAN", SQL_BIT},     {"INT32", SQL_INTEGER},
      {"INT64", SQL_BIGINT},      {"FLOAT", SQL_REAL},      {"DOUBLE", SQL_DOUBLE},
      {"TEXT", SQL_WLONGVARCHAR}, {"STRING", SQL_WVARCHAR}, {"BLOB", SQL_LONGVARBINARY},
      {"TIMESTAMP", SQL_BIGINT},  {"DATE", SQL_TYPE_DATE}};

  auto it = typeMap.find(iotdbDataType);
  if (it != typeMap.end()) {
    *numericAttribute = it->second;
    static const std::unordered_map<SQLSMALLINT, std::string> typeNameMap = {
        {SQL_BIT, "SQL_BIT"},
        {SQL_INTEGER, "SQL_INTEGER"},
        {SQL_BIGINT, "SQL_BIGINT"},
        {SQL_REAL, "SQL_REAL"},
        {SQL_DOUBLE, "SQL_DOUBLE"},
        {SQL_LONGVARCHAR, "SQL_LONGVARCHAR"},
        {SQL_VARCHAR, "SQL_VARCHAR"},
        {SQL_LONGVARBINARY, "SQL_LONGVARBINARY"},
        {SQL_TIMESTAMP, "SQL_TIMESTAMP"},
        {SQL_DATE, "SQL_DATE"}};

    if (isLogLevelEnabled(cnct, LOG_LEVEL_DEBUG)) {
      std::string typeName = typeNameMap.count(*numericAttribute)
                                 ? typeNameMap.at(*numericAttribute)
                                 : "UNKNOWN_TYPE_CODE";

      std::string logMsg = "colTypeMapping: Column " + std::to_string(columnNumber) +
                           ": IoTDB type '" + iotdbDataType + "' mapped to ODBC type '" + typeName +
                           "'";
      logMessage(cnct, logMsg, LOG_LEVEL_DEBUG);
    }
  } else {
    *numericAttribute = SQL_UNKNOWN_TYPE;

    if (isLogLevelEnabled(cnct, LOG_LEVEL_DEBUG)) {
      std::string logMsg = "colTypeMapping: Column " + std::to_string(columnNumber) +
                           ": Unknown IoTDB type '" + iotdbDataType +
                           "' mapped to SQL_UNKNOWN_TYPE";
      logMessage(cnct, logMsg, LOG_LEVEL_DEBUG);
    }
  }
}

// Abstract function to get column type based on column number
SQLRETURN getColType(StatementHandle* stmt, const SQLUSMALLINT columnNumber,
                     SQLSMALLINT* numericAttribute) {
  ConnectionHandle* cnct = stmt->getConnection();
  logMessage(cnct, "getColType: Entering", LOG_LEVEL_TRACE);

  if (!numericAttribute) {
    logMessage(cnct, "getColType: numericAttribute is null", LOG_LEVEL_ERROR);
    stmt->addDiagnostic("HY009", "Invalid use of null pointer");
    return SQL_ERROR;
  }

  if (!stmt->resultSetPtr->getIsMetaData() || stmt->getConnection()->isTableModel ||
      !stmt->resultSetPtr->getColumnType(columnNumber - 1).empty()) {
    logMessage(cnct, "getColType: Using column types from ODBCResultSet for type mapping",
               LOG_LEVEL_DEBUG);

    // Table model: get type directly from ODBCResultSet column types
    const std::string iotdbDataType = stmt->resultSetPtr->getColumnType(columnNumber - 1);
    if (iotdbDataType.empty()) {
      logMessage(cnct,
                 "getColType: Failed to get data type for column " + std::to_string(columnNumber),
                 LOG_LEVEL_ERROR);
      stmt->addDiagnostic("07009", "Invalid column number");
      return SQL_ERROR;
    }
    colTypeMapping(cnct, iotdbDataType, columnNumber, numericAttribute);

  } else {
    // Metadata mode: determine type by examining actual data from ODBCResultSet
    logMessage(cnct, "getColType: Metadata mode - determining type from data content",
               LOG_LEVEL_DEBUG);

    bool foundFlag = false;
    int numRows = stmt->resultSetPtr->getNumRows();
    for (int curRow = 0; curRow < numRows; curRow++) {
      const ODBCField& value = stmt->resultSetPtr->getValue(curRow, columnNumber - 1);
      if (value.isNull()) {
        continue;
      }

      switch (value.getDataType()) {
      case TSDataType::BOOLEAN:
        *numericAttribute = SQL_BIT;
        break;
      case TSDataType::INT32:
        *numericAttribute = SQL_INTEGER;
        break;
      case TSDataType::INT64:
        *numericAttribute = SQL_BIGINT;
        break;
      case TSDataType::FLOAT:
        *numericAttribute = SQL_REAL;
        break;
      case TSDataType::DOUBLE:
        *numericAttribute = SQL_DOUBLE;
        break;
      case TSDataType::TEXT:
        *numericAttribute = SQL_WLONGVARCHAR;
        break;
      case TSDataType::STRING:
        *numericAttribute = SQL_WVARCHAR;
        break;
      case TSDataType::DATE:
        *numericAttribute = SQL_WVARCHAR;
        break;
      case TSDataType::BLOB:
        *numericAttribute = SQL_LONGVARBINARY;
        break;
      case TSDataType::TIMESTAMP:
        *numericAttribute = SQL_BIGINT;
        break;
      default:
        *numericAttribute = SQL_UNKNOWN_TYPE;
        break;
      }
      foundFlag = true;
    }

    if (!foundFlag) {
      // All rows are empty/null
      *numericAttribute = SQL_UNKNOWN_TYPE; // Default to UNKNOWN_TYPE for null columns
      logMessage(cnct,
                 "getColType: All values empty for column " + std::to_string(columnNumber) +
                     ", defaulting to SQL_VARCHAR",
                 LOG_LEVEL_DEBUG);
    }
  }

  logMessage(cnct, "getColType: Exiting", LOG_LEVEL_TRACE);
  return SQL_SUCCESS;
}

SQLRETURN getColumnSize(SQLSMALLINT odbcType, SQLULEN* columnSize) {
  /*
     * Map an ODBC SQL type to its “column size” (display width in characters,
     * not bytes) according to
     * https://learn.microsoft.com/en-us/sql/odbc/reference/appendixes/column-size
     */
  if (columnSize == nullptr) {
    return SQL_ERROR;
  }

  switch (odbcType) {
  case SQL_CHAR:
  case SQL_WCHAR:
  case SQL_WVARCHAR:
  case SQL_WLONGVARCHAR:
  case SQL_VARCHAR:
  case SQL_LONGVARCHAR:
    *columnSize = 2147483647; // IoTDB places no length limit on strings
    break;

  case SQL_BIT:
    *columnSize = 1;
    break;
  case SQL_TINYINT:
    *columnSize = 3;
    break;
  case SQL_SMALLINT:
    *columnSize = 5;
    break;
  case SQL_INTEGER:
    *columnSize = 10;
    break;
  case SQL_BIGINT:
    *columnSize = 19;
    break;
  case SQL_REAL:
    *columnSize = 7;
    break;
  case SQL_FLOAT:
  case SQL_DOUBLE:
    *columnSize = 15;
    break;

  case SQL_LONGVARBINARY:
    *columnSize = 2147483647;
    break;

  case SQL_TYPE_DATE:
  case SQL_DATE:
    *columnSize = 10; // yyyy-mm-dd
    break;
  case SQL_TYPE_TIMESTAMP:
  case SQL_TIMESTAMP:
    *columnSize = 23; // yyyy-mm-dd hh:mm:ss.nnn
    break;

  case SQL_DECIMAL:
  case SQL_NUMERIC:
    // format: [sign][integer digits].[fractional digits]
    // assume max precision 38, scale 10
    *columnSize = 40; // 38 digits + decimal point + optional sign
    break;

  case SQL_TIME:
    *columnSize = 8; // hh:mm:ss
    break;

  case SQL_GUID:
    *columnSize = 36; // canonical form: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

  case SQL_UNKNOWN_TYPE:
    return SQL_ERROR;

  default:
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

SQLRETURN getNullable(StatementHandle* stmt, const SQLUSMALLINT columnNumber,
                      SQLSMALLINT* nullable) {
  // Retrieve the connection handle for logging purposes
  ConnectionHandle* cnct = stmt ? stmt->getConnection() : nullptr;

  // Validate input parameters
  if (nullable == nullptr) {
    logMessage(cnct, "getNullable: Output pointer is null", LOG_LEVEL_ERROR);
    if (stmt)
      stmt->addDiagnostic("HY009", "Invalid use of null pointer");
    return SQL_ERROR;
  }

  if (stmt == nullptr) {
    logMessage(cnct, "getNullable: Statement handle is null", LOG_LEVEL_ERROR);
    return SQL_ERROR;
  }

  if (stmt->resultSetPtr == nullptr) {
    logMessage(cnct, "getNullable: Result set is null or empty", LOG_LEVEL_ERROR);
    stmt->addDiagnostic("HY000", "No query result available");
    return SQL_ERROR;
  }

  // Log debug information
  logMessage(cnct, "getNullable: Checking nullability for column " + std::to_string(columnNumber),
             LOG_LEVEL_DEBUG);

  *nullable = SQL_NULLABLE_UNKNOWN;

  logMessage(cnct, "getNullable: Exiting successfully", LOG_LEVEL_TRACE);
  return SQL_SUCCESS;
}

SQLRETURN getColTypeName(StatementHandle* stmt, const SQLUSMALLINT columnNumber,
                         const SQLPOINTER stringBuffer, const SQLSMALLINT bufferLength,
                         SQLSMALLINT* stringLength) {
  ConnectionHandle* cnct = stmt->getConnection();
  logMessage(cnct, "getColTypeName: Entering", LOG_LEVEL_TRACE);

  // Declared catalog types remain available for empty and all-NULL results.
  const auto declaredType = stmt->resultSetPtr->getColumnType(columnNumber - 1);
  if (!declaredType.empty())
    return setString(declaredType, stringBuffer, bufferLength, stringLength);

  if (!stringBuffer) {
    logMessage(cnct, "getColTypeName: stringBuffer is null", LOG_LEVEL_ERROR);
    stmt->addDiagnostic("HY009", "Invalid use of null pointer");
    return SQL_ERROR;
  }

  if (!stmt->resultSetPtr->getIsMetaData() || stmt->getConnection()->isTableModel) {
    logMessage(cnct, "getColTypeName: Using column types from ODBCResultSet for type name mapping",
               LOG_LEVEL_DEBUG);

    if (stmt->getConnection()->isTableModel) {
      // Table model: get type name directly from ODBCResultSet column types
      const std::string iotdbDataType = stmt->resultSetPtr->getColumnType(columnNumber - 1);
      if (iotdbDataType.empty()) {
        logMessage(cnct,
                   "getColTypeName: Failed to get data type for column " +
                       std::to_string(columnNumber),
                   LOG_LEVEL_ERROR);
        stmt->addDiagnostic("07009", "Invalid column number");
        return SQL_ERROR;
      }
      SQLRETURN result = setString(iotdbDataType, stringBuffer, bufferLength, stringLength);
      if (result != SQL_SUCCESS) {
        logMessage(
            cnct, "getColTypeName: Failed to set string for column " + std::to_string(columnNumber),
            LOG_LEVEL_ERROR);
        return result;
      }
      logMessage(cnct,
                 "getColTypeName: " + iotdbDataType + " returned for column " +
                     std::to_string(columnNumber),
                 LOG_LEVEL_DEBUG);
    } else {
      // Tree model: not metadata, determine type name from column types
      if (columnNumber == 1) {
        SQLRETURN result = setString("TIMESTAMP", stringBuffer, bufferLength, stringLength);
        if (result != SQL_SUCCESS) {
          logMessage(cnct,
                     "getColTypeName: Failed to set string for column " +
                         std::to_string(columnNumber),
                     LOG_LEVEL_ERROR);
          return result;
        }
        logMessage(cnct,
                   "getColTypeName: TIMESTAMP returned for column " + std::to_string(columnNumber),
                   LOG_LEVEL_DEBUG);
      } else {
        const std::string iotdbDataType = stmt->resultSetPtr->getColumnType(columnNumber - 1);
        if (iotdbDataType.empty()) {
          logMessage(cnct,
                     "getColTypeName: Failed to get data type for column " +
                         std::to_string(columnNumber),
                     LOG_LEVEL_ERROR);
          stmt->addDiagnostic("07009", "Invalid column number");
          return SQL_ERROR;
        }
        SQLRETURN result = setString(iotdbDataType, stringBuffer, bufferLength, stringLength);
        if (result != SQL_SUCCESS) {
          logMessage(cnct,
                     "getColTypeName: Failed to set string for column " +
                         std::to_string(columnNumber),
                     LOG_LEVEL_ERROR);
          return result;
        }
        logMessage(cnct,
                   "getColTypeName: " + iotdbDataType + " returned for column " +
                       std::to_string(columnNumber),
                   LOG_LEVEL_DEBUG);
      }
    }
  } else {
    // Metadata mode: determine type name by examining actual data from ODBCResultSet
    logMessage(cnct, "getColTypeName: Metadata mode - determining type name from data content",
               LOG_LEVEL_DEBUG);

    bool foundFlag = false;
    int numRows = stmt->resultSetPtr->getNumRows();
    for (int curRow = 0; curRow < numRows; curRow++) {
      const ODBCField& value = stmt->resultSetPtr->getValue(curRow, columnNumber - 1);
      if (value.isNull()) {
        continue;
      }
      SQLRETURN result;
      switch (value.getDataType()) {
      case TSDataType::BOOLEAN:
        result = setString("BOOLEAN", stringBuffer, bufferLength, stringLength);
        break;
      case TSDataType::FLOAT:
        result = setString("FLOAT", stringBuffer, bufferLength, stringLength);
        break;
      case TSDataType::DOUBLE:
        result = setString("DOUBLE", stringBuffer, bufferLength, stringLength);
        break;
      case TSDataType::STRING:
        result = setString("STRING", stringBuffer, bufferLength, stringLength);
        break;
      case TSDataType::INT32:
        result = setString("INT32", stringBuffer, bufferLength, stringLength);
        break;
      case TSDataType::INT64:
        result = setString("INT64", stringBuffer, bufferLength, stringLength);
        break;
      case TSDataType::TIMESTAMP:
        result = setString("TIMESTAMP", stringBuffer, bufferLength, stringLength);
        break;
      case TSDataType::DATE:
        result = setString("DATE", stringBuffer, bufferLength, stringLength);
        break;
      case TSDataType::BLOB:
        result = setString("BLOB", stringBuffer, bufferLength, stringLength);
        break;
      default:
        result = setString("", stringBuffer, bufferLength, stringLength);
        break;
      }
      foundFlag = true;
    }

    if (!foundFlag) {
      // All rows are empty/null
      SQLRETURN result = setString("", stringBuffer, bufferLength, stringLength);
      if (result != SQL_SUCCESS) {
        logMessage(cnct,
                   "getColTypeName: Failed to set empty string for null column " +
                       std::to_string(columnNumber),
                   LOG_LEVEL_ERROR);
        return result;
      }
      logMessage(cnct,
                 "getColTypeName: Empty string returned for null column " +
                     std::to_string(columnNumber),
                 LOG_LEVEL_DEBUG);
    }
  }

  logMessage(cnct, "getColTypeName: Exiting", LOG_LEVEL_TRACE);
  return SQL_SUCCESS;
}

SQLRETURN getPrecision(SQLSMALLINT odbcType, SQLULEN* precision) {
  if (precision == nullptr) {
    logMessage("getPrecision: precision pointer is nullptr");
    return SQL_SUCCESS;
  }

  switch (odbcType) {
  case SQL_BIT:     // BOOLEAN
    *precision = 1; // Boolean value fixed to 1 bit
    break;

  case SQL_INTEGER:  // INT32
    *precision = 10; // 32-bit integer maximum 10 digits (2,147,483,647)
    break;

  case SQL_BIGINT:   // INT64
    *precision = 19; // 64-bit integer maximum 19 digits (9,223,372,036,854,775,807)
    break;

  case SQL_REAL:     // FLOAT
    *precision = 23; // IEEE 754 single precision has 23 bits of binary precision
    break;

  case SQL_DOUBLE:   // DOUBLE
    *precision = 52; // IEEE 754 double precision has 52 bits of binary precision
    break;

  case SQL_WCHAR:
  case SQL_WVARCHAR:

  case SQL_WLONGVARCHAR:
  case SQL_VARCHAR:
  case SQL_LONGVARCHAR:
  case SQL_LONGVARBINARY:
  case SQL_TYPE_TIMESTAMP:
  case SQL_TIMESTAMP:
  case SQL_TYPE_DATE:
  case SQL_DATE:
    *precision = 0;
    break;

  default:
    *precision = 0; // Unknown type defaults to 0
    break;
  }
  return SQL_SUCCESS;
}

SQLRETURN getPrecRadix(SQLSMALLINT odbcType, SQLULEN* radix) {
  if (radix == nullptr) {
    logMessage("getPrecRadix: precision pointer is nullptr");
    return SQL_SUCCESS;
  }

  switch (odbcType) {
  case SQL_REAL:
  case SQL_DOUBLE:
  case SQL_BIT: // BOOLEAN
    *radix = 2; // Boolean value fixed to 1 bit
    break;
  case SQL_INTEGER:
  case SQL_BIGINT: // INT64
    *radix = 10;   // 64-bit integer maximum 19 digits (9,223,372,036,854,775,807)
    break;
  case SQL_WCHAR:
  case SQL_WVARCHAR:
  case SQL_WLONGVARCHAR:
  case SQL_VARCHAR:
  case SQL_LONGVARCHAR:
  case SQL_LONGVARBINARY:
  case SQL_TYPE_TIMESTAMP:
  case SQL_TIMESTAMP:
  case SQL_TYPE_DATE:
  case SQL_DATE:
    *radix = 0;
    break;

  default:
    *radix = 0; // Unknown type defaults to 0
    break;
  }
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLColAttribute(const SQLHSTMT statementHandle, const SQLUSMALLINT columnNumber,
                                  const SQLUSMALLINT fieldIdentifier,
                                  const SQLPOINTER characterAttribute,
                                  const SQLSMALLINT bufferLength, SQLSMALLINT* stringLength,
                                  SQLLEN* numericAttribute) {
  // Get connection handle for logging
  StatementHandle* stmt = static_cast<StatementHandle*>(statementHandle);
  ConnectionHandle* cnct = stmt ? stmt->getConnection() : nullptr;

  logMessage(cnct, "SQLColAttribute: Entering", LOG_LEVEL_TRACE);

  if (isLogLevelEnabled(cnct, LOG_LEVEL_TRACE)) {
    std::stringstream logStream;
    logStream << "SQLColAttribute parameters: "
              << "StatementHandle = " << handleToString(statementHandle)
              << ", ColumnNumber = " << columnNumber << ", FieldIdentifier = " << fieldIdentifier
              << ", CharacterAttribute = " << characterAttribute
              << ", BufferLength = " << bufferLength;

    logMessage(cnct, logStream.str(), LOG_LEVEL_TRACE);
  }

  if (statementHandle == nullptr) {
    logMessage(cnct, "SQLColAttribute: Invalid statement handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }

  if (stmt->resultSetPtr == nullptr) {
    logMessage(cnct, "SQLColAttribute: Missing result set", LOG_LEVEL_ERROR);
    stmt->addDiagnostic("HY000", "No query result available");
    return SQL_ERROR;
  }

  // Validate column number
  if (columnNumber < 1 || columnNumber > stmt->resultSetPtr->getNumColumns()) {
    logMessage(cnct,
               "SQLColAttribute: Invalid column number: " + std::to_string(columnNumber) +
                   ", max columns: " + std::to_string(stmt->resultSetPtr->getNumColumns()),
               LOG_LEVEL_ERROR);
    stmt->addDiagnostic("07009", "Invalid column number");
    return SQL_ERROR;
  }

  // Initialize output parameters
  if (characterAttribute) {
    memset(characterAttribute, 0, bufferLength);
  }
  if (stringLength) {
    *stringLength = 0;
  }
  if (numericAttribute) {
    *numericAttribute = 0;
  }

  // Handle based on fieldIdentifier
  SQLRETURN sqlreturn;
  SQLSMALLINT columnType;
  SQLLEN octetLength;
  SQLLEN scale;
  switch (fieldIdentifier) {
  case SQL_DESC_LABEL:
    logMessage(cnct, "SQLColAttribute: Processing SQL_DESC_LABEL", LOG_LEVEL_DEBUG);
    if (characterAttribute) {
      // Get column name as label
      sqlreturn = getColName(stmt, columnNumber, characterAttribute, bufferLength, stringLength);
      if (sqlreturn != SQL_SUCCESS) {
        logMessage(cnct, "SQLColAttribute: Failed to get column name for label", LOG_LEVEL_ERROR);
        return sqlreturn;
      }
    }
    break;

  case SQL_DESC_COUNT:
    logMessage(cnct, "SQLColAttribute: Processing SQL_DESC_COUNT", LOG_LEVEL_DEBUG);
    {
      int columnCount = stmt->resultSetPtr->getNumColumns();

      if (numericAttribute) {
        *numericAttribute = static_cast<SQLLEN>(columnCount);
      }
      logMessage(cnct, "SQLColAttribute: Column count = " + std::to_string(columnCount),
                 LOG_LEVEL_DEBUG);
    }
    break;

  case SQL_DESC_NAME:
    logMessage(cnct, "SQLColAttribute: Processing SQL_DESC_NAME", LOG_LEVEL_DEBUG);
    sqlreturn = getColName(stmt, columnNumber, characterAttribute, bufferLength, stringLength);
    if (sqlreturn != SQL_SUCCESS) {
      logMessage(cnct, "SQLColAttribute: Failed to get column name", LOG_LEVEL_ERROR);
      return sqlreturn;
    }
    break;

  case SQL_COLUMN_TYPE:
  case SQL_DESC_TYPE:
    logMessage(cnct, "SQLColAttribute: Processing SQL_COLUMN_TYPE/SQL_DESC_TYPE", LOG_LEVEL_DEBUG);
    if (!numericAttribute) {
      logMessage(cnct, "SQLColAttribute: numericAttribute is null for type request",
                 LOG_LEVEL_ERROR);
      stmt->addDiagnostic("HY009", "Invalid use of null pointer");
      return SQL_ERROR;
    }
    sqlreturn = getColType(stmt, columnNumber, reinterpret_cast<SQLSMALLINT*>(numericAttribute));
    if (sqlreturn != SQL_SUCCESS) {
      logMessage(cnct, "SQLColAttribute: Failed to get column type", LOG_LEVEL_ERROR);
      return sqlreturn;
    }
    break;

  case SQL_COLUMN_DISPLAY_SIZE:
    logMessage(cnct,
               "SQLColAttribute: Processing SQL_COLUMN_DISPLAY_SIZE for column " +
                   std::to_string(columnNumber),
               LOG_LEVEL_DEBUG);
    {
      // First get the column type
      columnType = SQL_UNKNOWN_TYPE;
      sqlreturn = getColType(stmt, columnNumber, &columnType);
      if (sqlreturn != SQL_SUCCESS) {
        logMessage(cnct, "SQLColAttribute: Failed to get column type for display size",
                   LOG_LEVEL_ERROR);
        return sqlreturn;
      }

      SQLLEN displaySize = 0;
      switch (columnType) {
      case SQL_CHAR:
      case SQL_WCHAR:
      case SQL_WVARCHAR:
      case SQL_WLONGVARCHAR:
      case SQL_VARCHAR:
      case SQL_LONGVARCHAR:
        // For string types, use the actual length if available, otherwise default
        displaySize = 255; // Default for string types
        break;

      case SQL_DECIMAL:
      case SQL_NUMERIC:
        displaySize = 20; // Reasonable default for decimal types
        break;

      case SQL_INTEGER:
        displaySize = 11; // Max display size for a signed 32-bit integer (-2147483648)
        break;

      case SQL_SMALLINT:
        displaySize = 6; // Max display size for a signed 16-bit integer (-32768)
        break;

      case SQL_BIGINT:
        displaySize = 20; // Max display size for a signed 64-bit integer
        break;

      case SQL_FLOAT:
      case SQL_DOUBLE:
      case SQL_REAL:
        displaySize = 24; // Typical display size for floating-point numbers
        break;

      case SQL_BIT:
        displaySize = 1; // 0 or 1
        break;

      case SQL_TYPE_DATE:
        displaySize = 10; // YYYY-MM-DD
        break;

      case SQL_TYPE_TIME:
        displaySize = 8; // HH:MM:SS
        break;

      case SQL_TYPE_TIMESTAMP:
        displaySize = 23; // YYYY-MM-DD HH:MM:SS.sss
        break;

      default:
        displaySize = 0; // Unknown type
        logMessage(cnct,
                   "SQLColAttribute: Unknown column type for display size: " +
                       std::to_string(columnType),
                   LOG_LEVEL_WARN);
        break;
      }

      if (numericAttribute) {
        *numericAttribute = displaySize;
        logMessage(cnct, "SQLColAttribute: Display size set to " + std::to_string(displaySize),
                   LOG_LEVEL_DEBUG);
      }

      if (characterAttribute && bufferLength > 0) {
        const std::string displaySizeStr = std::to_string(displaySize);
        const size_t copyLen =
            std::min(static_cast<size_t>(bufferLength - 1), displaySizeStr.length());
        std::memcpy(characterAttribute, displaySizeStr.c_str(), copyLen);
        static_cast<char*>(characterAttribute)[copyLen] = '\0';

        if (stringLength) {
          *stringLength = static_cast<SQLSMALLINT>(displaySizeStr.length());
        }
      }
    }
    break;

  case SQL_COLUMN_UNSIGNED: {
    // FieldIdentifier == 8
    logMessage(cnct,
               "SQLColAttribute: Processing SQL_COLUMN_UNSIGNED for column " +
                   std::to_string(columnNumber),
               LOG_LEVEL_DEBUG);

    // Default: Column is signed unless explicitly determined otherwise
    SQLLEN isUnsigned = SQL_FALSE;

    columnType = SQL_UNKNOWN_TYPE;
    sqlreturn = getColType(stmt, columnNumber, &columnType);
    if (sqlreturn != SQL_SUCCESS) {
      logMessage(cnct, "SQLColAttribute: Failed to get column type for unsigned check",
                 LOG_LEVEL_ERROR);
      return sqlreturn;
    }

    switch (columnType) {
    case SQL_BIT: // BIT type is effectively unsigned
      isUnsigned = SQL_TRUE;
      break;

    default:
      isUnsigned = SQL_FALSE; // Most types are signed by default
      break;
    }

    if (numericAttribute) {
      *numericAttribute = isUnsigned;
      logMessage(cnct, "SQLColAttribute: Unsigned attribute set to " + std::to_string(isUnsigned),
                 LOG_LEVEL_DEBUG);
    }

    if (characterAttribute && bufferLength > 0) {
      // Return a textual representation
      const char* unsignedStr = (isUnsigned == SQL_TRUE) ? "TRUE" : "FALSE";
      const size_t copyLen =
          std::min(static_cast<size_t>(bufferLength - 1), std::strlen(unsignedStr));
      std::memcpy(characterAttribute, unsignedStr, copyLen);
      static_cast<char*>(characterAttribute)[copyLen] = '\0';

      if (stringLength) {
        *stringLength = static_cast<SQLSMALLINT>(std::strlen(unsignedStr));
      }
    }
    break;
  }

  case SQL_DESC_OCTET_LENGTH: // Byte length
    logMessage(cnct,
               "SQLColAttribute: Processing SQL_DESC_OCTET_LENGTH for column " +
                   std::to_string(columnNumber),
               LOG_LEVEL_DEBUG);

    // Get the column type to determine octet length
    columnType = SQL_UNKNOWN_TYPE;
    sqlreturn = getColType(stmt, columnNumber, &columnType);
    if (sqlreturn != SQL_SUCCESS) {
      logMessage(cnct, "SQLColAttribute: Failed to get column type for octet length",
                 LOG_LEVEL_ERROR);
      return sqlreturn;
    }

    octetLength = 0;
    switch (columnType) {
    case SQL_BIT:
      octetLength = 1;
      break;
    case SQL_TINYINT:
      octetLength = 1;
      break;
    case SQL_SMALLINT:
      octetLength = 2;
      break;
    case SQL_INTEGER:
      octetLength = 4;
      break;
    case SQL_BIGINT:
      octetLength = 8;
      break;
    case SQL_REAL:
      octetLength = 4;
      break;
    case SQL_FLOAT:
    case SQL_DOUBLE:
      octetLength = 8;
      break;
    case SQL_TYPE_DATE:
      octetLength = 6; // Size of DATE_STRUCT
      break;
    case SQL_TYPE_TIME:
      octetLength = 6; // Size of TIME_STRUCT
      break;
    case SQL_TYPE_TIMESTAMP:
      octetLength = 16; // Size of TIMESTAMP_STRUCT
      break;
    default:
      // For string types, use a reasonable default or calculate based on data
      octetLength = 255;
      break;
    }

    if (numericAttribute) {
      *numericAttribute = octetLength;
    }
    break;

  case SQL_COLUMN_PRECISION:
  case SQL_DESC_PRECISION: // Precision
  {
    logMessage(cnct,
               "SQLColAttribute: Processing SQL_DESC_PRECISION for column " +
                   std::to_string(columnNumber),
               LOG_LEVEL_DEBUG);
    columnType = SQL_UNKNOWN_TYPE;
    sqlreturn = getColType(stmt, columnNumber, &columnType);
    if (sqlreturn != SQL_SUCCESS) {
      logMessage(cnct, "SQLColAttribute: Failed to get column type for precision", LOG_LEVEL_ERROR);
      return sqlreturn;
    }
    SQLULEN value;
    sqlreturn = getPrecision(columnType, &value);
    if (sqlreturn != SQL_SUCCESS) {
      logMessage(cnct, "SQLColAttribute: Failed to get precision", LOG_LEVEL_ERROR);
      return sqlreturn;
    }
    if (numericAttribute) {
      *numericAttribute = value;
    }
    break;
  }

  case SQL_COLUMN_SCALE:
  case SQL_DESC_SCALE: // Scale
    logMessage(cnct,
               "SQLColAttribute: Processing SQL_DESC_SCALE for column " +
                   std::to_string(columnNumber),
               LOG_LEVEL_DEBUG);

    // Get the column type to determine scale
    columnType = SQL_UNKNOWN_TYPE;
    sqlreturn = getColType(stmt, columnNumber, &columnType);
    if (sqlreturn != SQL_SUCCESS) {
      logMessage(cnct, "SQLColAttribute: Failed to get column type for scale", LOG_LEVEL_ERROR);
      return sqlreturn;
    }

    scale = 0;
    switch (columnType) {
    case SQL_DECIMAL:
    case SQL_NUMERIC:
      // For decimal types, you might need to get the actual scale from metadata
      scale = 0; // Default scale for decimal types
      break;
    default:
      // For other types, scale is not applicable
      scale = 0;
      break;
    }

    if (numericAttribute) {
      *numericAttribute = scale;
    }
    break;

  case SQL_DESC_UPDATABLE:
    if (numericAttribute)
      *numericAttribute = SQL_ATTR_READONLY;
    break;
  case SQL_DESC_AUTO_UNIQUE_VALUE:
    if (numericAttribute)
      *numericAttribute = SQL_FALSE;
    break;
  case SQL_DESC_CASE_SENSITIVE:
    if (numericAttribute)
      *numericAttribute = SQL_TRUE;
    break;
  case SQL_DESC_SEARCHABLE:
    if (numericAttribute)
      *numericAttribute = SQL_PRED_SEARCHABLE;
    break;
  case SQL_DESC_TYPE_NAME:
    logMessage(cnct,
               "SQLColAttribute: Processing SQL_DESC_TYPE_NAME for column " +
                   std::to_string(columnNumber),
               LOG_LEVEL_DEBUG);
    sqlreturn = getColTypeName(stmt, columnNumber, characterAttribute, bufferLength, stringLength);
    if (sqlreturn != SQL_SUCCESS) {
      logMessage(cnct, "SQLColAttribute: Failed to get column type name", LOG_LEVEL_ERROR);
      return sqlreturn;
    }
    break;

  case SQL_DESC_NULLABLE: {
    logMessage(cnct,
               "SQLColAttribute: Processing SQL_DESC_NULLABLE for column " +
                   std::to_string(columnNumber),
               LOG_LEVEL_DEBUG);
    SQLSMALLINT value = 0;
    sqlreturn = getNullable(stmt, columnNumber, &value);
    if (numericAttribute) {
      *numericAttribute = value;
    }
    break;
  }

  case SQL_DESC_LENGTH: {
    logMessage(cnct,
               "SQLColAttribute: Processing SQL_DESC_LENGTH for column " +
                   std::to_string(columnNumber),
               LOG_LEVEL_DEBUG);
    SQLSMALLINT type = SQL_UNKNOWN_TYPE;
    sqlreturn = getColType(stmt, columnNumber, &type);
    if (sqlreturn != SQL_SUCCESS) {
      logMessage(cnct, "SQLColAttribute: Failed to get column type for length", LOG_LEVEL_ERROR);
      return sqlreturn;
    }
    SQLULEN value;
    sqlreturn = getColumnSize(type, &value);
    if (sqlreturn != SQL_SUCCESS) {
      logMessage(cnct, "SQLColAttribute: Failed to get length", LOG_LEVEL_ERROR);
      return sqlreturn;
    }
    if (numericAttribute) {
      *numericAttribute = value;
    }
    break;
  }

  case SQL_DESC_NUM_PREC_RADIX: {
    logMessage(cnct,
               "SQLColAttribute: Processing SQL_DESC_NUM_PREC_RADIX for column " +
                   std::to_string(columnNumber),
               LOG_LEVEL_DEBUG);
    SQLSMALLINT type = SQL_UNKNOWN_TYPE;
    sqlreturn = getColType(stmt, columnNumber, &type);
    if (sqlreturn != SQL_SUCCESS) {
      logMessage(cnct, "SQLColAttribute: Failed to get column type for precision radix",
                 LOG_LEVEL_ERROR);
      return sqlreturn;
    }
    SQLULEN value;
    sqlreturn = getPrecRadix(type, &value);
    if (sqlreturn != SQL_SUCCESS) {
      logMessage(cnct, "SQLColAttribute: Failed to get column type for precision radix",
                 LOG_LEVEL_ERROR);
      return sqlreturn;
    }
    if (numericAttribute) {
      *numericAttribute = value;
    }
    break;
  }

  default:
    logMessage(cnct,
               "SQLColAttribute: Unsupported field identifier: " + std::to_string(fieldIdentifier),
               LOG_LEVEL_WARN);
    stmt->addDiagnostic("HY000", "Unsupported field identifier");
    return SQL_ERROR;
  }

  logMessage(cnct, "SQLColAttribute: Exiting successfully", LOG_LEVEL_TRACE);
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLConnect(SQLHDBC connectionHandle, SQLCHAR* serverName, SQLSMALLINT nameLength1,
                             SQLCHAR* userName, SQLSMALLINT nameLength2, SQLCHAR* password,
                             SQLSMALLINT nameLength3) {
  if (!connectionHandle) {
    logMessage(nullptr, "SQLConnect: Invalid connection handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  auto cnct = static_cast<ConnectionHandle*>(connectionHandle);
  cnct->clearDiagnostics();
  logMessage(cnct, "SQLConnect: Entering", LOG_LEVEL_TRACE);

  if ((nameLength1 < 0 && nameLength1 != SQL_NTS) || (nameLength2 < 0 && nameLength2 != SQL_NTS) ||
      (nameLength3 < 0 && nameLength3 != SQL_NTS)) {
    cnct->addDiagnostic("HY090", "Invalid string or buffer length");
    return SQL_ERROR;
  }

  try {
    std::string dsnName;
    if (serverName) {
      if (nameLength1 == SQL_NTS)
        dsnName = reinterpret_cast<const char*>(serverName);
      else if (nameLength1 > 0)
        dsnName = std::string(reinterpret_cast<const char*>(serverName), nameLength1);
    }
    logMessage(cnct, "SQLConnect: DSN='" + dsnName + "'", LOG_LEVEL_INFO);

    if (dsnName.empty()) {
      logMessage(cnct, "SQLConnect: DSN name is empty", LOG_LEVEL_ERROR);
      cnct->addDiagnostic("IM002", "Data source name not found and no default driver specified");
      return SQL_ERROR;
    }

    cnct->LoadDsnFromOdbcIni(dsnName);

    if (userName) {
      std::string uid;
      if (nameLength2 == SQL_NTS)
        uid = reinterpret_cast<const char*>(userName);
      else if (nameLength2 > 0)
        uid = std::string(reinterpret_cast<const char*>(userName), nameLength2);
      if (!uid.empty()) {
        cnct->userName = uid;
        logMessage(cnct, "SQLConnect: UID overridden from parameter: " + uid, LOG_LEVEL_DEBUG);
      }
    }

    if (password) {
      std::string pwd;
      if (nameLength3 == SQL_NTS)
        pwd = reinterpret_cast<const char*>(password);
      else if (nameLength3 > 0)
        pwd = std::string(reinterpret_cast<const char*>(password), nameLength3);
      if (!pwd.empty()) {
        cnct->password = pwd;
        logMessage(cnct, "SQLConnect: PWD overridden from parameter", LOG_LEVEL_DEBUG);
      }
    }

    if (cnct->isTableModel && cnct->database.empty()) {
      logMessage(cnct, "SQLConnect: Database not set, using 'information_schema' as default",
                 LOG_LEVEL_INFO);
      cnct->database = "information_schema";
    }

    if (isLogLevelEnabled(cnct, LOG_LEVEL_INFO)) {
      std::ostringstream oss;
      oss << "SQLConnect: Connecting with Server=" << cnct->serverHostName
          << ", Port=" << cnct->serverPort << ", UID=" << cnct->userName
          << ", Database=" << cnct->database
          << ", TableModel=" << (cnct->isTableModel ? "true" : "false");
      logMessage(cnct, oss.str(), LOG_LEVEL_INFO);
    }

    SQLRETURN returnCode;
    if (cnct->useRestful) {
      logMessage(cnct, "SQLConnect: Connecting using RESTful API", LOG_LEVEL_DEBUG);
      returnCode = IoTDB_DriverConnect_Rest(cnct);
    } else {
      logMessage(cnct, "SQLConnect: Connecting using Session API", LOG_LEVEL_DEBUG);
      returnCode = IoTDB_DriverConnect_Session(cnct);
    }

    logMessage(cnct, "SQLConnect: Exiting with return code: " + std::to_string(returnCode),
               LOG_LEVEL_TRACE);
    return returnCode;
  } catch (const std::exception& error) {
    cnct->addDiagnostic("08001", error.what());
    return SQL_ERROR;
  }
}

SQLRETURN SQL_API SQLCopyDesc(SQLHDESC sourceDescHandle, SQLHDESC targetDescHandle) {
  if (!sourceDescHandle || !targetDescHandle)
    return SQL_INVALID_HANDLE;
  auto* source = static_cast<DescriptorHandle*>(sourceDescHandle);
  auto* target = static_cast<DescriptorHandle*>(targetDescHandle);
  if (source->getHandleType() != SQL_HANDLE_DESC || target->getHandleType() != SQL_HANDLE_DESC)
    return SQL_INVALID_HANDLE;
  target->clearDiagnostics();
  if (target->role == DescriptorRole::IMPLEMENTATION_ROW) {
    target->addDiagnostic("HY016", "Cannot modify an implementation row descriptor");
    return SQL_ERROR;
  }
  if (source->connection != target->connection) {
    target->addDiagnostic("HY024", "Descriptor handles belong to different connections");
    return SQL_ERROR;
  }
  if (source->role == DescriptorRole::IMPLEMENTATION_ROW && source->owner)
    PopulateImplementationRowDescriptor(source->owner);
  target->arraySize = source->arraySize;
  target->arrayStatusPtr = source->arrayStatusPtr;
  target->bindOffsetPtr = source->bindOffsetPtr;
  target->bindType = source->bindType;
  target->count = source->count;
  target->rowsProcessedPtr = source->rowsProcessedPtr;
  target->records = source->records;
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLDataSources(SQLHENV environmentHandle, SQLUSMALLINT direction,
                                 SQLCHAR* serverName, SQLSMALLINT bufferLength1,
                                 SQLSMALLINT* nameLength1Ptr, SQLCHAR* description,
                                 SQLSMALLINT bufferLength2, SQLSMALLINT* nameLength2Ptr) {
  if (!environmentHandle)
    return SQL_INVALID_HANDLE;
  auto* env = static_cast<EnvironmentHandle*>(environmentHandle);
  env->clearDiagnostics();
  if (direction != SQL_FETCH_FIRST && direction != SQL_FETCH_NEXT &&
      direction != SQL_FETCH_FIRST_USER && direction != SQL_FETCH_FIRST_SYSTEM) {
    env->addDiagnostic("HY103", "Invalid retrieval code");
    return SQL_ERROR;
  }
  if (bufferLength1 < 0 || bufferLength2 < 0) {
    env->addDiagnostic("HY090", "Invalid string or buffer length");
    return SQL_ERROR;
  }
  if (nameLength1Ptr)
    *nameLength1Ptr = 0;
  if (nameLength2Ptr)
    *nameLength2Ptr = 0;
  if (serverName && bufferLength1 > 0)
    serverName[0] = '\0';
  if (description && bufferLength2 > 0)
    description[0] = '\0';
  // Driver Manager-owned DSN enumeration is not duplicated by the driver.
  return SQL_NO_DATA;
}

SQLRETURN SQL_API SQLDescribeCol(SQLHSTMT statementHandle, SQLUSMALLINT columnNumber,
                                 SQLCHAR* columnName, SQLSMALLINT bufferLength,
                                 SQLSMALLINT* nameLength, SQLSMALLINT* dataType,
                                 SQLULEN* columnSize, SQLSMALLINT* decimalDigits,
                                 SQLSMALLINT* nullable) {
  // Most content copied from SQLColAttribute
  const auto stmt = static_cast<StatementHandle*>(statementHandle);
  ConnectionHandle* cnct = stmt ? stmt->getConnection() : nullptr;
  logMessage(cnct, "SQLDescribeCol: Entering", LOG_LEVEL_TRACE);

  // Prepare log message for the parameters
  if (isLogLevelEnabled(cnct, LOG_LEVEL_TRACE)) {
    std::stringstream logStream;
    logStream << "SQLDescribeCol parameters: "
              << "StatementHandle = " << handleToString(statementHandle)
              << ", ColumnNumber = " << columnNumber << ", ColumnName = "
              << (columnName ? reinterpret_cast<const char*>(columnName) : "nullptr")
              << ", BufferLength = " << bufferLength
              << ", NameLength = " << (nameLength ? std::to_string(*nameLength) : "nullptr")
              << ", DataType = " << (dataType ? std::to_string(*dataType) : "nullptr")
              << ", ColumnSize = " << (columnSize ? std::to_string(*columnSize) : "nullptr")
              << ", DecimalDigits = "
              << (decimalDigits ? std::to_string(*decimalDigits) : "nullptr")
              << ", Nullable = " << (nullable ? std::to_string(*nullable) : "nullptr");
    logMessage(cnct, logStream.str(), LOG_LEVEL_TRACE);
  }

  if (statementHandle == nullptr) {
    logMessage(cnct, "SQLDescribeCol: Invalid statement handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }

  if (stmt->resultSetPtr == nullptr) {
    logMessage(cnct, "SQLDescribeCol: Missing result set", LOG_LEVEL_ERROR);
    stmt->addDiagnostic("HY000", "No query result available");
    return SQL_ERROR;
  }

  // Validate column number
  if (columnNumber < 1 || columnNumber > stmt->resultSetPtr->getNumColumns()) {
    logMessage(cnct,
               "SQLDescribeCol: Invalid column number: " + std::to_string(columnNumber) +
                   ", max columns: " + std::to_string(stmt->resultSetPtr->getNumColumns()),
               LOG_LEVEL_ERROR);
    stmt->addDiagnostic("07009", "Invalid column number");
    return SQL_ERROR;
  }

  // Column name
  SQLRETURN sqlreturn = getColName(stmt, columnNumber, columnName, bufferLength, nameLength);
  if (sqlreturn != SQL_SUCCESS) {
    logMessage(cnct, "SQLDescribeCol: Failed to get column name", LOG_LEVEL_ERROR);
    return sqlreturn;
  }

  // Data type
  if (dataType) {
    sqlreturn = getColType(stmt, columnNumber, dataType);
    logMessage(cnct, "SQLDescribeCol: Data type set to " + std::to_string(*dataType),
               LOG_LEVEL_TRACE);
    if (sqlreturn != SQL_SUCCESS) {
      logMessage(cnct, "SQLDescribeCol: Failed to get column type", LOG_LEVEL_ERROR);
      return sqlreturn;
    }
  }

  // Column size
  if (columnSize) {
    sqlreturn = getColumnSize(*dataType, columnSize);
    if (sqlreturn != SQL_SUCCESS) {
      logMessage(cnct, "SQLDescribeCol: Failed to get column size", LOG_LEVEL_ERROR);
      return sqlreturn;
    }

    logMessage(cnct, "SQLDescribeCol: Column size set to " + std::to_string(*columnSize),
               LOG_LEVEL_DEBUG);
  }

  if (decimalDigits) {
    // This field is only applicable for fixed-point values.
    // For timestamps with sub-second precision and SQL_DECIMAL/SQL_NUMERIC types,
    // it should have a non-zero value; for other cases, it should be 0 or NULL.
    *decimalDigits = 0;
  }

  // Nullable
  if (nullable) {
    sqlreturn = getNullable(stmt, columnNumber, nullable);
    if (sqlreturn != SQL_SUCCESS) {
      logMessage(cnct, "SQLDescribeCol: Failed to get nullable attribute", LOG_LEVEL_ERROR);
      return sqlreturn;
    }

    const char* nullableStr = (*nullable == SQL_NULLABLE)   ? "NULLABLE"
                              : (*nullable == SQL_NO_NULLS) ? "NOT NULL"
                                                            : "UNKNOWN";
    logMessage(cnct, "SQLDescribeCol: Nullable attribute set to " + std::string(nullableStr),
               LOG_LEVEL_DEBUG);
  }

  logMessage(cnct, "SQLDescribeCol: Exiting successfully", LOG_LEVEL_TRACE);
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLDisconnect(SQLHDBC connectionHandle) {
  if (!connectionHandle) {
    logMessage(nullptr, "SQLDisconnect: Invalid connection handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  auto cnct = static_cast<ConnectionHandle*>(connectionHandle);
  try {
    cnct->CloseSession();
    return SQL_SUCCESS;
  } catch (const std::exception& e) {
    cnct->addDiagnostic("08003", e.what());
    return SQL_ERROR;
  }
}

SQLRETURN SQL_API SQLEndTran(SQLSMALLINT handleType, SQLHANDLE handle, SQLSMALLINT completionType) {
  if (!handle) {
    logMessage(nullptr, "SQLEndTran: Invalid handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }

  auto* baseHandle = static_cast<ODBCHandle*>(handle);

  EnvironmentHandle* env = nullptr;
  ConnectionHandle* cnct = nullptr;
  if (handleType == SQL_HANDLE_ENV) {
    if (baseHandle && baseHandle->getHandleType() != SQL_HANDLE_ENV) {
      baseHandle->addDiagnostic("HY092", "Invalid handle type for SQLEndTran: expected ENV");
      return SQL_INVALID_HANDLE;
    }
    env = static_cast<EnvironmentHandle*>(handle);
  } else if (handleType == SQL_HANDLE_DBC) {
    if (baseHandle && baseHandle->getHandleType() != SQL_HANDLE_DBC) {
      baseHandle->addDiagnostic("HY092", "Invalid handle type for SQLEndTran: expected DBC");
      return SQL_INVALID_HANDLE;
    }
    cnct = static_cast<ConnectionHandle*>(handle);
    env = cnct ? cnct->getEnvironment() : nullptr;
  }

  if (cnct && isLogLevelEnabled(cnct, LOG_LEVEL_TRACE)) {
    std::stringstream logStream;
    std::string handleTypeStr = "UNKNOWN";
    switch (handleType) {
    case SQL_HANDLE_ENV:
      handleTypeStr = "ENV";
      break;
    case SQL_HANDLE_DBC:
      handleTypeStr = "DBC";
      break;
    case SQL_HANDLE_STMT:
      handleTypeStr = "STMT";
      break;
    default:
      handleTypeStr = "UNKNOWN";
      break;
    }
    std::string completionTypeStr = "UNKNOWN";
    switch (completionType) {
    case SQL_COMMIT:
      completionTypeStr = "COMMIT";
      break;
    case SQL_ROLLBACK:
      completionTypeStr = "ROLLBACK";
      break;
    }
    logStream << "SQLEndTran parameters: "
              << ", handleType = " << handleTypeStr << "(" << handleType << ")"
              << ", handle = " << handleToString(handle)
              << ", completionType = " << completionTypeStr << "(" << completionType << ")";
    logMessage(cnct, logStream.str(), LOG_LEVEL_TRACE);
  }

  // Determine which handle to attach diagnostics to.
  // Prefer connection diagnostics (most callers use DBC/STMT); fall back to stmt/env when needed.
  ODBCHandle* diagTarget = nullptr;
  if (cnct) {
    diagTarget = cnct;
  } else if (env) {
    diagTarget = env;
  } else if (baseHandle) {
    diagTarget = baseHandle;
  }

  // Validate handleType
  if (handleType != SQL_HANDLE_ENV && handleType != SQL_HANDLE_DBC) {
    logMessage(cnct, "SQLEndTran: Invalid HandleType", LOG_LEVEL_ERROR);
    if (diagTarget) {
      diagTarget->addDiagnostic("HY092", "Invalid attribute/option identifier");
    }
    return SQL_ERROR;
  }

  // Validate completionType
  if (completionType != SQL_COMMIT && completionType != SQL_ROLLBACK) {
    logMessage(cnct, "SQLEndTran: Invalid CompletionType", LOG_LEVEL_ERROR);
    if (diagTarget) {
      diagTarget->addDiagnostic("HY012", "Invalid transaction operation code");
    }
    return SQL_ERROR;
  }

  // IoTDB reports SQL_TXN_CAPABLE = SQL_TC_NONE (no transactions).
  if (completionType == SQL_COMMIT) {
    logMessage(cnct, "SQLEndTran: COMMIT requested; IoTDB has no transactions, no-op",
               LOG_LEVEL_TRACE);
    return SQL_SUCCESS;
  }

  logMessage(cnct, "SQLEndTran: ROLLBACK requested but IoTDB has no transactions", LOG_LEVEL_DEBUG);
  if (diagTarget) {
    diagTarget->addDiagnostic("HYC00", "IoTDB does not support transactions; rollback ignored");
  }
  return SQL_ERROR;
}

SQLRETURN SQLError(SQLHENV environmentHandle, // Input: Environment handle
                   SQLHDBC connectionHandle,  // Input: Connection handle
                   SQLHSTMT statementHandle,  // Input: Statement handle
                   SQLCHAR* sqlstate,         // Output: SQLSTATE code
                   SQLINTEGER* nativeError,   // Output: Native error code
                   SQLCHAR* messageText,      // Output: Error message text
                   SQLSMALLINT bufferLength,  // Input: Message text buffer length
                   SQLSMALLINT* textLength    // Output: Actual length of message text
) {
  // Determine the appropriate connection handle for logging
  ConnectionHandle* cnct = nullptr;
  if (statementHandle != SQL_NULL_HSTMT) {
    const auto stmt = reinterpret_cast<StatementHandle*>(statementHandle);
    cnct = stmt->getConnection();
  } else if (connectionHandle != SQL_NULL_HDBC) {
    cnct = reinterpret_cast<ConnectionHandle*>(connectionHandle);
  }

  logMessage(cnct, "SQLError: Entering", LOG_LEVEL_TRACE);

  // Log input parameters if TRACE level is enabled
  if (isLogLevelEnabled(cnct, LOG_LEVEL_TRACE)) {
    std::stringstream logStream;
    logStream << "SQLError parameters: "
              << "EnvironmentHandle = " << environmentHandle
              << ", ConnectionHandle = " << connectionHandle
              << ", StatementHandle = " << statementHandle
              << ", SqlState = " << (sqlstate ? reinterpret_cast<const char*>(sqlstate) : "nullptr")
              << ", NativeError = " << (nativeError ? std::to_string(*nativeError) : "nullptr")
              << ", MessageText = "
              << (messageText ? reinterpret_cast<const char*>(messageText) : "nullptr")
              << ", BufferLength = " << bufferLength
              << ", TextLength = " << (textLength ? std::to_string(*textLength) : "nullptr");
    logMessage(cnct, logStream.str(), LOG_LEVEL_TRACE);
  }

  SQLHANDLE handle;
  SQLSMALLINT handle_type;

  // Determine handle type and handle
  if (statementHandle != SQL_NULL_HSTMT) {
    handle = statementHandle;
    handle_type = SQL_HANDLE_STMT;
    logMessage(cnct, "SQLError: Using statement handle", LOG_LEVEL_DEBUG);
  } else if (connectionHandle != SQL_NULL_HDBC) {
    handle = connectionHandle;
    handle_type = SQL_HANDLE_DBC;
    logMessage(cnct, "SQLError: Using connection handle", LOG_LEVEL_DEBUG);
  } else if (environmentHandle != SQL_NULL_HENV) {
    handle = environmentHandle;
    handle_type = SQL_HANDLE_ENV;
    logMessage(cnct, "SQLError: Using environment handle", LOG_LEVEL_DEBUG);
  } else {
    logMessage(cnct, "SQLError: No valid handle provided", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE; // Invalid handle
  }

  logMessage(cnct, "SQLError: Calling SQLGetDiagRec", LOG_LEVEL_DEBUG);

  // Call SQLGetDiagRec to retrieve error information
  SQLRETURN retcode = SQLGetDiagRec(handle_type, handle, 1, sqlstate, nativeError, messageText,
                                    bufferLength, textLength);
  if (SQL_SUCCEEDED(retcode))
    static_cast<ODBCHandle*>(handle)->consumeFirstDiagnostic();

  // Log the result of SQLGetDiagRec
  if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
    std::stringstream resultStream;
    resultStream << "SQLError: Retrieved error - SQLSTATE: "
                 << (sqlstate ? reinterpret_cast<const char*>(sqlstate) : "null")
                 << ", NativeError: " << (nativeError ? std::to_string(*nativeError) : "null")
                 << ", Message: "
                 << (messageText ? reinterpret_cast<const char*>(messageText) : "null");
    logMessage(cnct, resultStream.str(), LOG_LEVEL_DEBUG);
  } else if (retcode == SQL_NO_DATA) {
    logMessage(cnct, "SQLError: No error information available", LOG_LEVEL_DEBUG);
  } else {
    logMessage(cnct, "SQLError: SQLGetDiagRec failed", LOG_LEVEL_ERROR);
  }

  // Return appropriate SQLError return value based on SQLGetDiagRec's return code
  logMessage(cnct, "SQLError: Exiting", LOG_LEVEL_TRACE);

  switch (retcode) {
  case SQL_SUCCESS:
  case SQL_SUCCESS_WITH_INFO:
    return SQL_SUCCESS;
  case SQL_NO_DATA:
    return SQL_NO_DATA;
  default:
    return SQL_ERROR;
  }
}

/**
 * Safely converts SQLCHAR array to std::string with proper handling of unsigned char
 *
 * This function ensures that unsigned char values are properly converted to char
 * without sign extension issues, making it safe for all character values (0-255).
 *
 * @param statementText Pointer to SQLCHAR array (unsigned char*)
 * @param textLength Length of the text or SQL_NTS for null-terminated string
 * @return std::string containing the converted text, or empty string on error
 */
std::string ConvertSQLCHARToString(SQLCHAR* statementText, SQLINTEGER textLength) {
  if (statementText == nullptr) {
    return std::string();
  }

  // Determine the actual length
  size_t length = 0;
  if (textLength == SQL_NTS) {
    // Calculate length for null-terminated string
    const SQLCHAR* ptr = statementText;
    while (*ptr != '\0') {
      ++length;
      ++ptr;
    }
  } else if (textLength > 0) {
    length = static_cast<size_t>(textLength);
  } else {
    return std::string(); // Invalid length
  }

  // Convert each character safely
  std::string result;
  result.reserve(length);
  for (size_t i = 0; i < length; ++i) {
    // Safe conversion: static_cast ensures proper value preservation
    result.push_back(static_cast<char>(statementText[i]));
  }

  return result;
}

/**
 * If the SQL is a SELECT whose first column is exactly "Time" followed by a comma,
 * removes that "Time," so IoTDB receives a SELECT without the Time column.
 * (IoTDB tree model always returns a time column; PowerBI adds Time to the SELECT
 * list, which the server does not accept.) Only the leading column name "Time" is
 * matched; other columns or "Timestamp" etc. are left unchanged.
 * @param sql Statement to modify in place; unchanged if pattern does not match.
 */
static void StripLeadingTimeColumnFromSelect(std::string& sql) {
  size_t i = 0;
  while (i < sql.size() && (sql[i] == ' ' || sql[i] == '\t' || sql[i] == '\r' || sql[i] == '\n')) {
    ++i;
  }
  if (i + 6 > sql.size())
    return;
  const char* p = sql.c_str() + i;
  if ((p[0] == 's' || p[0] == 'S') && (p[1] == 'e' || p[1] == 'E') &&
      (p[2] == 'l' || p[2] == 'L') && (p[3] == 'e' || p[3] == 'E') &&
      (p[4] == 'c' || p[4] == 'C') && (p[5] == 't' || p[5] == 'T')) {
    i += 6;
  } else {
    return;
  }
  while (i < sql.size() && (sql[i] == ' ' || sql[i] == '\t' || sql[i] == '\r' || sql[i] == '\n')) {
    ++i;
  }
  if (i + 4 > sql.size())
    return;
  p = sql.c_str() + i;
  if ((p[0] != 't' && p[0] != 'T') || (p[1] != 'i' && p[1] != 'I') ||
      (p[2] != 'm' && p[2] != 'M') || (p[3] != 'e' && p[3] != 'E')) {
    return;
  }
  size_t timeStart = i;
  i += 4;
  if (i < sql.size()) {
    char c = sql[i];
    if (c != ',' && c != ' ' && c != '\t' && c != '\r' && c != '\n')
      return;
  }
  while (i < sql.size() && (sql[i] == ' ' || sql[i] == '\t' || sql[i] == '\r' || sql[i] == '\n')) {
    ++i;
  }
  if (i >= sql.size() || sql[i] != ',')
    return;
  sql.erase(timeStart, i - timeStart + 1);
}

// saveStatementText: Optional, defaults to true
SQLRETURN IoTDB_ExecDirect(StatementHandle* const stmt, const std::string& statementText,
                           bool saveStatementText) {
  ConnectionHandle* cnct = stmt->getConnection();
  logMessage(cnct, "IoTDB_ExecDirect: Entering", LOG_LEVEL_TRACE);

  std::string sqlForServer = statementText;
  if (!cnct->isTableModel)
    StripLeadingTimeColumnFromSelect(sqlForServer);
  if (saveStatementText) {
    stmt->statementText = sqlForServer;
    logMessage(cnct, "IoTDB_ExecDirect: Saved statement text", LOG_LEVEL_DEBUG);
  }

  // Choose API based on connection settings
  if (cnct->useRestful) {
    logMessage(cnct, "IoTDB_ExecDirect: Using RESTful API", LOG_LEVEL_DEBUG);
    return IoTDB_ExecDirect_Rest(stmt, sqlForServer);
  } else {
    logMessage(cnct, "IoTDB_ExecDirect: Using Session API", LOG_LEVEL_DEBUG);
    return IoTDB_ExecDirect_Session(stmt, sqlForServer);
  }
}

static void PopulateImplementationRowDescriptor(StatementHandle* stmt) {
  auto* descriptor = stmt->implicitImpRowDesc;
  descriptor->records.clear();
  descriptor->count = 0;
  if (!stmt->resultSetPtr)
    return;
  descriptor->records.resize(static_cast<size_t>(stmt->resultSetPtr->getNumColumns()));
  descriptor->count =
      static_cast<SQLSMALLINT>(std::min(stmt->resultSetPtr->getNumColumns(),
                                        static_cast<int>(std::numeric_limits<SQLSMALLINT>::max())));
  for (SQLSMALLINT index = 0; index < descriptor->count; ++index) {
    DescriptorRecord& record = descriptor->records[static_cast<size_t>(index)];
    record.name = stmt->resultSetPtr->getColumnName(static_cast<size_t>(index));
    record.unnamed = record.name.empty() ? SQL_UNNAMED : SQL_NAMED;
    std::string type = stmt->resultSetPtr->getColumnType(static_cast<size_t>(index));
    std::transform(type.begin(), type.end(), type.begin(), [](unsigned char character) {
      return static_cast<char>(std::toupper(character));
    });
    if (type == "BOOLEAN") {
      record.type = record.conciseType = SQL_BIT;
      record.length = record.octetLength = 1;
    } else if (type == "INT16") {
      record.type = record.conciseType = SQL_SMALLINT;
      record.length = record.octetLength = sizeof(SQLSMALLINT);
      record.precision = 5;
      record.numPrecRadix = 10;
    } else if (type == "INT32") {
      record.type = record.conciseType = SQL_INTEGER;
      record.length = record.octetLength = sizeof(SQLINTEGER);
      record.precision = 10;
      record.numPrecRadix = 10;
    } else if (type == "INT64" || type == "TIMESTAMP") {
      record.type = record.conciseType = SQL_BIGINT;
      record.length = record.octetLength = sizeof(SQLBIGINT);
      record.precision = 19;
      record.numPrecRadix = 10;
    } else if (type == "FLOAT") {
      record.type = record.conciseType = SQL_REAL;
      record.length = record.octetLength = sizeof(float);
      record.precision = 7;
      record.numPrecRadix = 2;
    } else if (type == "DOUBLE") {
      record.type = record.conciseType = SQL_DOUBLE;
      record.length = record.octetLength = sizeof(double);
      record.precision = 15;
      record.numPrecRadix = 2;
    } else if (type == "DATE") {
      record.setConciseType(SQL_TYPE_DATE);
      record.length = 10;
      record.octetLength = sizeof(DATE_STRUCT);
    } else if (type == "BLOB") {
      record.type = record.conciseType = SQL_LONGVARBINARY;
    } else {
      record.type = record.conciseType = SQL_WVARCHAR;
    }
    record.nullable = SQL_NULLABLE_UNKNOWN;
  }
}

static SQLRETURN ExecuteBoundStatement(StatementHandle* stmt);

SQLRETURN SQL_API SQLExecDirect(SQLHSTMT statementHandle, SQLCHAR* statementText,
                                SQLINTEGER textLength) {
  StatementHandle* stmt = static_cast<StatementHandle*>(statementHandle);
  ConnectionHandle* cnct = stmt ? stmt->getConnection() : nullptr;
  logMessage(cnct, "SQLExecDirect: Entering", LOG_LEVEL_TRACE);

  if (isLogLevelEnabled(cnct, LOG_LEVEL_TRACE)) {
    std::stringstream logStream;
    logStream << "SQLExecDirect parameters: "
              << "statementHandle = " << handleToString(statementHandle)
              << ", statementText = " << (statementText ? "provided" : "nullptr")
              << ", textLength = " << textLength;
    logMessage(cnct, logStream.str(), LOG_LEVEL_TRACE);
  }

  if (!statementHandle) {
    logMessage(cnct, "SQLExecDirect: Invalid statement handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  stmt->clearDiagnostics();
  if (!statementText) {
    stmt->addDiagnostic("HY009", "Invalid use of null pointer");
    return SQL_ERROR;
  }
  if (textLength < 0 && textLength != SQL_NTS) {
    stmt->addDiagnostic("HY090", "Invalid string or buffer length");
    return SQL_ERROR;
  }
  if (stmt->needsParameterData) {
    stmt->addDiagnostic("HY010", "Function sequence error");
    return SQL_ERROR;
  }
  stmt->prepared = false;
  stmt->rowsReturned = 0;
  stmt->curRow = -1;
  stmt->streamCurRow = 0;
  stmt->isStream = false;

  // Convert SQLCHAR to std::string
  std::string sqlStatement = ConvertSQLCHARToString(statementText, textLength);
  logMessage(cnct, "SQLExecDirect: Executing statement", LOG_LEVEL_DEBUG);

  stmt->statementText = sqlStatement;
  const SQLRETURN returnCode = ExecuteBoundStatement(stmt);
  if (returnCode != SQL_SUCCESS) {
    logMessage(cnct, "SQLExecDirect: Execution failed with code: " + std::to_string(returnCode),
               LOG_LEVEL_ERROR);
  } else {
    logMessage(cnct, "SQLExecDirect: Execution completed successfully", LOG_LEVEL_INFO);
  }

  logMessage(cnct, "SQLExecDirect: Exiting", LOG_LEVEL_TRACE);
  return returnCode;
}

static bool IsDataAtExecution(SQLLEN indicator) {
  return indicator == SQL_DATA_AT_EXEC || indicator <= SQL_LEN_DATA_AT_EXEC_OFFSET;
}

static size_t CTypeSize(SQLSMALLINT type, SQLLEN bufferLength) {
  switch (type) {
  case SQL_C_BIT:
  case SQL_C_TINYINT:
  case SQL_C_STINYINT:
  case SQL_C_UTINYINT:
    return 1;
  case SQL_C_SSHORT:
  case SQL_C_SHORT:
  case SQL_C_USHORT:
    return sizeof(SQLSMALLINT);
  case SQL_C_SLONG:
  case SQL_C_LONG:
  case SQL_C_ULONG:
    return sizeof(SQLINTEGER);
  case SQL_C_SBIGINT:
  case SQL_C_UBIGINT:
    return sizeof(SQLBIGINT);
  case SQL_C_FLOAT:
    return sizeof(float);
  case SQL_C_DOUBLE:
    return sizeof(double);
  case SQL_C_TYPE_DATE:
  case SQL_C_DATE:
    return sizeof(DATE_STRUCT);
  case SQL_C_TYPE_TIME:
  case SQL_C_TIME:
    return sizeof(TIME_STRUCT);
  case SQL_C_TYPE_TIMESTAMP:
  case SQL_C_TIMESTAMP:
    return sizeof(TIMESTAMP_STRUCT);
  default:
    return static_cast<size_t>(std::max<SQLLEN>(bufferLength, 1));
  }
}

static SQLSMALLINT DefaultParameterCType(SQLSMALLINT sqlType) {
  switch (sqlType) {
  case SQL_BIT:
    return SQL_C_BIT;
  case SQL_TINYINT:
    return SQL_C_STINYINT;
  case SQL_SMALLINT:
    return SQL_C_SSHORT;
  case SQL_INTEGER:
    return SQL_C_SLONG;
  case SQL_BIGINT:
    return SQL_C_SBIGINT;
  case SQL_REAL:
    return SQL_C_FLOAT;
  case SQL_FLOAT:
  case SQL_DOUBLE:
    return SQL_C_DOUBLE;
  case SQL_DECIMAL:
  case SQL_NUMERIC:
    return SQL_C_CHAR;
  case SQL_WCHAR:
  case SQL_WVARCHAR:
  case SQL_WLONGVARCHAR:
    return SQL_C_WCHAR;
  case SQL_TYPE_DATE:
  case SQL_DATE:
    return SQL_C_TYPE_DATE;
  case SQL_TYPE_TIME:
  case SQL_TIME:
    return SQL_C_TYPE_TIME;
  case SQL_TYPE_TIMESTAMP:
  case SQL_TIMESTAMP:
    return SQL_C_TYPE_TIMESTAMP;
  case SQL_BINARY:
  case SQL_VARBINARY:
  case SQL_LONGVARBINARY:
    return SQL_C_BINARY;
  default:
    return SQL_C_CHAR;
  }
}

static SQLPOINTER ParameterValueAt(const StatementHandle* stmt, const ParameterBinding& binding,
                                   SQLULEN set) {
  if (!binding.valuePtr)
    return nullptr;
  const SQLULEN offset = stmt->paramBindOffsetPtr ? *stmt->paramBindOffsetPtr : 0;
  const size_t stride = stmt->paramBindType == SQL_BIND_BY_COLUMN
                            ? CTypeSize(binding.valueType == SQL_C_DEFAULT
                                            ? DefaultParameterCType(binding.parameterType)
                                            : binding.valueType,
                                        binding.bufferLength)
                            : static_cast<size_t>(stmt->paramBindType);
  return static_cast<char*>(binding.valuePtr) + offset + set * stride;
}

static SQLLEN ParameterIndicatorAt(const StatementHandle* stmt, const ParameterBinding& binding,
                                   SQLULEN set) {
  const SQLULEN offset = stmt->paramBindOffsetPtr ? *stmt->paramBindOffsetPtr : 0;
  const SQLULEN stride =
      stmt->paramBindType == SQL_BIND_BY_COLUMN ? sizeof(SQLLEN) : stmt->paramBindType;
  auto read = [&](const SQLLEN* pointer) {
    SQLLEN result;
    std::memcpy(&result, reinterpret_cast<const char*>(pointer) + offset + set * stride,
                sizeof(result));
    return result;
  };
  if (binding.indicatorPtr && read(binding.indicatorPtr) == SQL_NULL_DATA)
    return SQL_NULL_DATA;
  if (binding.octetLengthPtr)
    return read(binding.octetLengthPtr);
  const SQLSMALLINT type = binding.valueType == SQL_C_DEFAULT
                               ? DefaultParameterCType(binding.parameterType)
                               : binding.valueType;
  return type == SQL_C_CHAR || type == SQL_C_WCHAR ? SQL_NTS : binding.bufferLength;
}

static std::string QuoteSqlString(const std::string& value) {
  std::string result("'");
  for (char character : value) {
    result.push_back(character);
    if (character == '\'')
      result.push_back('\'');
  }
  result.push_back('\'');
  return result;
}

static bool ParameterLiteral(StatementHandle* stmt, ParameterBinding& binding, SQLULEN set,
                             std::string& literal) {
  const SQLSMALLINT valueType = binding.valueType == SQL_C_DEFAULT
                                    ? DefaultParameterCType(binding.parameterType)
                                    : binding.valueType;
  SQLLEN indicator = ParameterIndicatorAt(stmt, binding, set);
  SQLPOINTER value = ParameterValueAt(stmt, binding, set);
  alignas(long double) char streamedValue[sizeof(TIMESTAMP_STRUCT)]{};
  if (indicator == SQL_NULL_DATA) {
    literal = "NULL";
    return true;
  }
  if (IsDataAtExecution(indicator)) {
    if (set >= binding.streamedData.size() || set >= binding.streamedNull.size()) {
      stmt->addDiagnostic("HY010", "Parameter data has not been supplied");
      return false;
    }
    if (binding.streamedNull[set]) {
      literal = "NULL";
      return true;
    }
    const auto& bytes = binding.streamedData[set];
    indicator = static_cast<SQLLEN>(bytes.size());
    value = const_cast<char*>(bytes.data());
    if (valueType != SQL_C_CHAR && valueType != SQL_C_WCHAR && valueType != SQL_C_BINARY) {
      const size_t required = CTypeSize(valueType, binding.bufferLength);
      if (required > sizeof(streamedValue) || bytes.size() != required) {
        stmt->addDiagnostic("HY090", "Invalid string or buffer length");
        return false;
      }
      std::memcpy(streamedValue, bytes.data(), required);
      value = streamedValue;
    }
  }
  if (!value) {
    stmt->addDiagnostic("HY009", "Invalid use of null parameter value pointer");
    return false;
  }
  std::ostringstream output;
  if (indicator < 0 &&
      !(indicator == SQL_NTS && (valueType == SQL_C_CHAR || valueType == SQL_C_WCHAR))) {
    stmt->addDiagnostic("HY090", "Invalid string or buffer length");
    return false;
  }
  output.imbue(std::locale::classic());
  output << std::setprecision(std::numeric_limits<double>::max_digits10);
  switch (valueType) {
  case SQL_C_CHAR: {
    const char* text = static_cast<const char*>(value);
    const size_t length = indicator == SQL_NTS ? std::strlen(text) : static_cast<size_t>(indicator);
    literal = QuoteSqlString(std::string(text, length));
    return true;
  }
  case SQL_C_WCHAR: {
    const auto* text = static_cast<const SQLWCHAR*>(value);
    const size_t length = indicator == SQL_NTS ? std::char_traits<SQLWCHAR>::length(text)
                                               : static_cast<size_t>(indicator) / sizeof(SQLWCHAR);
    std::u16string utf16(length, u'\0');
    if (length)
      std::memcpy(&utf16[0], text, length * sizeof(SQLWCHAR));
    literal = QuoteSqlString(
        std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t>().to_bytes(utf16));
    return true;
  }
  case SQL_C_BIT:
    output << (*static_cast<const unsigned char*>(value) ? 1 : 0);
    break;
  case SQL_C_STINYINT:
  case SQL_C_TINYINT:
    output << static_cast<int>(*static_cast<const signed char*>(value));
    break;
  case SQL_C_UTINYINT:
    output << static_cast<unsigned int>(*static_cast<const unsigned char*>(value));
    break;
  case SQL_C_SSHORT:
  case SQL_C_SHORT:
    output << *static_cast<const SQLSMALLINT*>(value);
    break;
  case SQL_C_USHORT:
    output << *static_cast<const SQLUSMALLINT*>(value);
    break;
  case SQL_C_SLONG:
  case SQL_C_LONG:
    output << *static_cast<const SQLINTEGER*>(value);
    break;
  case SQL_C_ULONG:
    output << *static_cast<const SQLUINTEGER*>(value);
    break;
  case SQL_C_SBIGINT:
    output << *static_cast<const SQLBIGINT*>(value);
    break;
  case SQL_C_UBIGINT:
    output << *static_cast<const SQLUBIGINT*>(value);
    break;
  case SQL_C_FLOAT:
    output << *static_cast<const float*>(value);
    break;
  case SQL_C_DOUBLE:
    output << *static_cast<const double*>(value);
    break;
  case SQL_C_BINARY: {
    const size_t length = static_cast<size_t>(indicator);
    static constexpr char digits[] = "0123456789ABCDEF";
    literal = "X'";
    for (size_t index = 0; index < length; ++index) {
      const unsigned char byte = static_cast<const unsigned char*>(value)[index];
      literal.push_back(digits[byte >> 4]);
      literal.push_back(digits[byte & 0x0F]);
    }
    literal.push_back('\'');
    return true;
  }
  case SQL_C_TYPE_DATE:
  case SQL_C_DATE: {
    const auto* date = static_cast<const DATE_STRUCT*>(value);
    output << "'" << std::setfill('0') << std::setw(4) << date->year << "-" << std::setw(2)
           << date->month << "-" << std::setw(2) << date->day << "'";
    break;
  }
  case SQL_C_TYPE_TIME:
  case SQL_C_TIME: {
    const auto* time = static_cast<const TIME_STRUCT*>(value);
    output << "'" << std::setfill('0') << std::setw(2) << time->hour << ":" << std::setw(2)
           << time->minute << ":" << std::setw(2) << time->second << "'";
    break;
  }
  case SQL_C_TYPE_TIMESTAMP:
  case SQL_C_TIMESTAMP: {
    const auto* timestamp = static_cast<const TIMESTAMP_STRUCT*>(value);
    output << "'" << std::setfill('0') << std::setw(4) << timestamp->year << "-" << std::setw(2)
           << timestamp->month << "-" << std::setw(2) << timestamp->day << " " << std::setw(2)
           << timestamp->hour << ":" << std::setw(2) << timestamp->minute << ":" << std::setw(2)
           << timestamp->second << "." << std::setw(9) << timestamp->fraction << "'";
    break;
  }
  default:
    stmt->addDiagnostic("HYC00", "Unsupported parameter C data type");
    return false;
  }
  literal = output.str();
  return true;
}

static bool RenderParameters(StatementHandle* stmt, SQLULEN set, std::string& sql) {
  const std::vector<size_t> markers = ParameterMarkerPositions(stmt->statementText);
  if (markers.size() > stmt->parameterBindings.size()) {
    stmt->addDiagnostic("07002", "COUNT field incorrect: not all parameters are bound");
    return false;
  }
  sql.clear();
  size_t previous = 0;
  for (size_t index = 0; index < markers.size(); ++index) {
    ParameterBinding& binding = stmt->parameterBindings[index];
    if (!binding.bound) {
      stmt->addDiagnostic("07002", "COUNT field incorrect: parameter is not bound");
      return false;
    }
    std::string literal;
    if (!ParameterLiteral(stmt, binding, set, literal))
      return false;
    sql.append(stmt->statementText, previous, markers[index] - previous);
    sql += literal;
    previous = markers[index] + 1;
  }
  sql.append(stmt->statementText, previous, std::string::npos);
  return true;
}

static SQLRETURN ExecuteParameterSets(StatementHandle* stmt) {
  SQLRETURN result = SQL_SUCCESS;
  if (stmt->paramsProcessedPtr)
    *stmt->paramsProcessedPtr = 0;
  const SQLULEN setCount =
      ParameterMarkerPositions(stmt->statementText).empty() ? 1 : stmt->paramSetSize;
  for (SQLULEN set = 0; set < setCount; ++set) {
    std::string sql;
    if (!RenderParameters(stmt, set, sql)) {
      result = SQL_ERROR;
    } else {
      logMessage(stmt->getConnection(), "SQLExecute: Executing prepared statement",
                 LOG_LEVEL_DEBUG);
      stmt->curRow = -1;
      stmt->streamCurRow = 0;
      stmt->isStream = false;
      result = IoTDB_ExecDirect(stmt, sql, false);
      if (SQL_SUCCEEDED(result))
        PopulateImplementationRowDescriptor(stmt);
    }
    if (stmt->paramStatusPtr)
      stmt->paramStatusPtr[set] = SQL_SUCCEEDED(result) ? SQL_PARAM_SUCCESS : SQL_PARAM_ERROR;
    if (stmt->paramsProcessedPtr)
      *stmt->paramsProcessedPtr = set + 1;
    if (!SQL_SUCCEEDED(result))
      return result;
  }
  return result;
}

static void SyncParameterBindingsFromDescriptors(StatementHandle* stmt) {
  auto* apd = static_cast<DescriptorHandle*>(stmt->appParamDesc);
  auto* ipd = static_cast<DescriptorHandle*>(stmt->impParamDesc);
  stmt->paramSetSize = apd->arraySize;
  stmt->paramBindType = apd->bindType;
  stmt->paramBindOffsetPtr = reinterpret_cast<SQLULEN*>(apd->bindOffsetPtr);
  stmt->paramsProcessedPtr = ipd->rowsProcessedPtr;
  stmt->paramStatusPtr = ipd->arrayStatusPtr;
  const SQLSMALLINT count = apd->count;
  stmt->parameterBindings.assign(static_cast<size_t>(count), ParameterBinding());
  for (int number = 1; number <= count; ++number) {
    const DescriptorRecord* app = apd->findRecord(number);
    const DescriptorRecord* implementation = ipd->findRecord(number);
    if (!app)
      continue;
    ParameterBinding& binding = stmt->parameterBindings[static_cast<size_t>(number - 1)];
    binding.valueType = app->conciseType;
    binding.valuePtr = app->dataPtr;
    binding.bufferLength = app->octetLength;
    binding.indicatorPtr = app->indicatorPtr;
    binding.octetLengthPtr = app->octetLengthPtr;
    if (implementation) {
      binding.inputOutputType = implementation->parameterType;
      binding.parameterType = implementation->conciseType;
      binding.columnSize = implementation->length;
      binding.decimalDigits = implementation->scale;
    }
    binding.bound = app->dataPtr != nullptr || binding.indicatorPtr != nullptr ||
                    binding.octetLengthPtr != nullptr;
  }
}

SQLRETURN SQL_API SQLExecute(SQLHSTMT statementHandle) {
  if (!statementHandle) {
    logMessage(nullptr, "SQLExecute: Invalid statement handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  auto stmt = static_cast<StatementHandle*>(statementHandle);
  stmt->clearDiagnostics();
  if (stmt->needsParameterData) {
    stmt->addDiagnostic("HY010", "Function sequence error");
    return SQL_ERROR;
  }
  if (!stmt->prepared) {
    stmt->addDiagnostic("HY010", "Statement has not been prepared");
    return SQL_ERROR;
  }
  return ExecuteBoundStatement(stmt);
}

static SQLRETURN ExecuteBoundStatement(StatementHandle* stmt) {
  stmt->rowsReturned = 0;
  SyncParameterBindingsFromDescriptors(stmt);
  const std::vector<size_t> markers = ParameterMarkerPositions(stmt->statementText);
  if (stmt->paramsProcessedPtr)
    *stmt->paramsProcessedPtr = 0;
  if (stmt->paramStatusPtr)
    std::fill(stmt->paramStatusPtr, stmt->paramStatusPtr + stmt->paramSetSize, SQL_PARAM_UNUSED);
  for (size_t index = 0; index < markers.size(); ++index) {
    if (index >= stmt->parameterBindings.size() || !stmt->parameterBindings[index].bound) {
      stmt->addDiagnostic("07002", "COUNT field incorrect: not all parameters are bound");
      return SQL_ERROR;
    }
  }
  for (size_t index = 0; index < markers.size(); ++index) {
    for (SQLULEN set = 0; set < stmt->paramSetSize; ++set) {
      if (IsDataAtExecution(ParameterIndicatorAt(stmt, stmt->parameterBindings[index], set))) {
        stmt->needsParameterData = true;
        stmt->nextDataSet = 0;
        stmt->activeDataSet = std::numeric_limits<SQLULEN>::max();
        stmt->nextDataParameter = 0;
        stmt->activeDataParameter = std::numeric_limits<size_t>::max();
        for (auto& binding : stmt->parameterBindings) {
          binding.streamedData.assign(stmt->paramSetSize, std::string());
          binding.streamedNull.assign(stmt->paramSetSize, false);
        }
        return SQL_NEED_DATA;
      }
    }
  }
  try {
    return ExecuteParameterSets(stmt);
  } catch (const std::exception& error) {
    stmt->addDiagnostic("HY000", error.what());
    return SQL_ERROR;
  }
}

static void SyncColumnBindingsFromDescriptor(StatementHandle* stmt) {
  auto* ard = static_cast<DescriptorHandle*>(stmt->appRowDesc);
  stmt->rowArraySize = ard->arraySize;
  stmt->rowBindType = ard->bindType;
  stmt->rowBindOffsetPtr = reinterpret_cast<SQLULEN*>(ard->bindOffsetPtr);
  auto* ird = static_cast<DescriptorHandle*>(stmt->impRowDesc);
  stmt->rowsFetchedPtr = ird->rowsProcessedPtr;
  stmt->rowStatusPtr = ird->arrayStatusPtr;
  stmt->columnBindings.assign(static_cast<size_t>(ard->count) + 1, BindColInfo());
  for (int number = 1; number <= ard->count; ++number) {
    const DescriptorRecord* record = ard->findRecord(number);
    if (!record)
      continue;
    BindColInfo& binding = stmt->columnBindings[static_cast<size_t>(number)];
    binding.targetType = record->conciseType == SQL_C_DEFAULT && stmt->resultSetPtr
                             ? stmt->resultSetPtr->getDefaultCTypeForColumn(number)
                             : record->conciseType;
    binding.targetValuePtr = record->dataPtr;
    binding.bufferLength = record->octetLength;
    binding.strLen_or_IndPtr = record->indicatorPtr ? record->indicatorPtr : record->octetLengthPtr;
    binding.isBound = record->dataPtr != nullptr || binding.strLen_or_IndPtr != nullptr;
  }
  if (stmt->resultSetPtr)
    stmt->resultSetPtr->bindColInfo = stmt->columnBindings;
}

SQLRETURN fillColBindBuffer(StatementHandle* stmt, SQLUSMALLINT col, const ODBCField& value,
                            SQLULEN rowsetIndex) {
  BindColInfo* binding = stmt->resultSetPtr->getBindColInfo(col);
  if (!binding || !binding->isBound) {
    return SQL_ERROR;
  }
  ConnectionHandle* cnct = stmt ? stmt->getConnection() : nullptr;
  const SQLULEN offset = stmt->rowBindOffsetPtr ? *stmt->rowBindOffsetPtr : 0;
  const size_t valueStride = stmt->rowBindType == SQL_BIND_BY_COLUMN
                                 ? CTypeSize(binding->targetType, binding->bufferLength)
                                 : static_cast<size_t>(stmt->rowBindType);
  const size_t indicatorStride = stmt->rowBindType == SQL_BIND_BY_COLUMN
                                     ? sizeof(SQLLEN)
                                     : static_cast<size_t>(stmt->rowBindType);
  SQLPOINTER target = binding->targetValuePtr
                          ? static_cast<void*>(static_cast<char*>(binding->targetValuePtr) +
                                               offset + rowsetIndex * valueStride)
                          : nullptr;
  const auto* record = static_cast<DescriptorHandle*>(stmt->appRowDesc)->findRecord(col);
  auto pointerAt = [&](SQLLEN* pointer) -> SQLLEN* {
    return pointer ? reinterpret_cast<SQLLEN*>(reinterpret_cast<char*>(pointer) + offset +
                                               rowsetIndex * indicatorStride)
                   : nullptr;
  };
  SQLLEN* indicator = pointerAt(record->indicatorPtr);
  SQLLEN* octetLength = pointerAt(record->octetLengthPtr);

  if (value.isNull()) {
    if (indicator) {
      *indicator = SQL_NULL_DATA;
    } else {
      stmt->addDiagnostic("22002", "Indicator variable required but not supplied");
      return SQL_ERROR;
    }

    if (target && binding->bufferLength > 0) {
      if (binding->targetType == SQL_C_CHAR) {
        static_cast<char*>(target)[0] = '\0';
      } else if (binding->targetType == SQL_C_WCHAR &&
                 binding->bufferLength >= static_cast<SQLLEN>(sizeof(SQLWCHAR))) {
        static_cast<SQLWCHAR*>(target)[0] = 0;
      }
    }
    return SQL_SUCCESS;
  }

  SQLLEN length = 0;
  SQLRETURN result = CopyFieldToTarget(stmt, value, binding->targetType, target,
                                       binding->bufferLength, &length, true, col);
  if (SQL_SUCCEEDED(result)) {
    if (indicator && indicator != octetLength)
      *indicator = 0;
    if (octetLength)
      *octetLength = length;
  }
  return result;
}

SQLRETURN SQL_API SQLFetch(SQLHSTMT statementHandle) {
  const auto stmt = static_cast<StatementHandle*>(statementHandle);
  ConnectionHandle* cnct = stmt ? stmt->getConnection() : nullptr;

  logMessage(cnct, "SQLFetch: Entering", LOG_LEVEL_TRACE);

  // Prepare log message for the parameters
  if (isLogLevelEnabled(cnct, LOG_LEVEL_TRACE)) {
    std::stringstream logStream;
    logStream << "SQLFetch parameters: "
              << "statementHandle = " << handleToString(statementHandle);
    logMessage(cnct, logStream.str(), LOG_LEVEL_TRACE);
  }

  // In SQLAllocHandle we're allocating a NULL_Handle, so I disabled this.
  if (!statementHandle) {
    logMessage(cnct, "SQLFetch: Invalid statement handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  stmt->clearDiagnostics();
  SyncColumnBindingsFromDescriptor(stmt);
  stmt->lastGetDataRow = -1;
  stmt->lastGetDataCol = 0;
  stmt->getDataOffsets.clear();
  if (stmt->rowsFetchedPtr)
    *stmt->rowsFetchedPtr = 0;
  if (stmt->rowStatusPtr) {
    for (SQLULEN index = 0; index < stmt->rowArraySize; ++index)
      stmt->rowStatusPtr[index] = SQL_ROW_NOROW;
  }
  if (stmt->maxRows > 0 && stmt->rowsReturned >= stmt->maxRows)
    return SQL_NO_DATA;

  if (stmt->resultSetPtr == nullptr) {
    logMessage(cnct, "SQLFetch: No result set available", LOG_LEVEL_ERROR);
    stmt->addDiagnostic("HY000", "No result set available");
    return SQL_ERROR;
  }

  SQLULEN fetched = 0;
  SQLULEN failed = 0;
  SQLRETURN overall = SQL_SUCCESS;
  while (fetched < stmt->rowArraySize &&
         (stmt->maxRows == 0 || stmt->rowsReturned < stmt->maxRows)) {
    stmt->curRow = stmt->curRow < 0 ? 0 : stmt->curRow + 1;
    if (stmt->curRow == stmt->resultSetPtr->getNumRows()) {
      const SQLSMALLINT streamResult = streamNextBatch(stmt);
      if (streamResult == SQL_NO_DATA)
        break;
      if (streamResult != SQL_SUCCESS)
        return streamResult;
      stmt->curRow = 0;
    }
    if (stmt->curRow < 0 || stmt->curRow >= stmt->resultSetPtr->getNumRows())
      break;

    SQLRETURN rowResult = SQL_SUCCESS;
    for (SQLUSMALLINT col = 1; col <= stmt->resultSetPtr->getNumColumns(); ++col) {
      BindColInfo* binding = stmt->resultSetPtr->getBindColInfo(col);
      if (!binding || !binding->isBound)
        continue;
      const ODBCField& value = stmt->resultSetPtr->getValue(stmt->curRow, col - 1);
      const SQLRETURN result = fillColBindBuffer(stmt, col, value, fetched);
      if (result != SQL_SUCCESS && result != SQL_SUCCESS_WITH_INFO) {
        rowResult = SQL_ERROR;
        overall = SQL_SUCCESS_WITH_INFO;
        ++failed;
        break;
      }
      if (result == SQL_SUCCESS_WITH_INFO)
        rowResult = overall = SQL_SUCCESS_WITH_INFO;
    }
    if (stmt->rowStatusPtr)
      stmt->rowStatusPtr[fetched] = rowResult == SQL_ERROR               ? SQL_ROW_ERROR
                                    : rowResult == SQL_SUCCESS_WITH_INFO ? SQL_ROW_SUCCESS_WITH_INFO
                                                                         : SQL_ROW_SUCCESS;
    ++fetched;
    ++stmt->rowsReturned;
  }
  if (stmt->rowsFetchedPtr)
    *stmt->rowsFetchedPtr = fetched;
  return fetched == 0 ? SQL_NO_DATA : failed == fetched ? SQL_ERROR : overall;
}

SQLRETURN SQL_API SQLFetchScroll(SQLHSTMT statementHandle, SQLSMALLINT fetchOrientation,
                                 SQLLEN fetchOffset) {
  if (!statementHandle) {
    logMessage(nullptr, "SQLFetchScroll: Invalid statement handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  auto stmt = static_cast<StatementHandle*>(statementHandle);
  stmt->clearDiagnostics();
  (void)fetchOffset;
  if (fetchOrientation == SQL_FETCH_NEXT)
    return SQLFetch(statementHandle);
  stmt->addDiagnostic("HYC00", "Only forward SQL_FETCH_NEXT is supported");
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLFreeConnect(SQLHDBC connectionHandle) {
  logMessage("SQLFreeConnect");
  SQLRETURN retcode = SQLFreeHandle(SQL_HANDLE_DBC, connectionHandle);
  if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
    std::ostringstream oss;
    oss << "SQLFreeConnect: Failed to free connection handle. retcode = " << retcode << "\n";
    logMessage(oss.str());
    return retcode;
  }
  return retcode;
}

SQLRETURN SQL_API SQLFreeEnv(SQLHENV environmentHandle) {
  logMessage("SQLFreeEnv");
  // Deprecated function, delegate to SQLAllocHandle for env allocation.
  SQLRETURN retcode = SQLFreeHandle(SQL_HANDLE_ENV, environmentHandle);
  if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
    std::ostringstream oss;
    oss << "SQLAllocEnv: Failed to free environment handle. retcode = " << retcode << "\n";
    logMessage(oss.str());
    return retcode;
  }
  return retcode;
}

SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT handleType, SQLHANDLE handle) {
  logMessage("Entering SQLFreeHandle");

  // Check if handle is NULL
  if (!handle) {
    logMessage("SQLFreeHandle: handle is already NULL.");
    return SQL_INVALID_HANDLE; // Invalid handle
  }

  // Check if handleType is valid
  if (handleType != SQL_HANDLE_ENV && handleType != SQL_HANDLE_DBC &&
      handleType != SQL_HANDLE_STMT && handleType != SQL_HANDLE_DESC) {
    logMessage("SQLFreeHandle: invalid handle type.");
    return SQL_ERROR; // Invalid handle type
  }

  // Convert handle to ODBCHandle type
  ODBCHandle* odbcHandle = static_cast<ODBCHandle*>(handle);

  if (odbcHandle->getHandleType() != handleType) {
    logMessage("SQLFreeHandle: handle type does not match the allocated handle.");
    return SQL_INVALID_HANDLE;
  }

  // Check if handle has already been freed
  if (odbcHandle->isHandleFreed()) {
    logMessage("SQLFreeHandle: handle is already freed.");
    return SQL_SUCCESS; // Handle already freed, return success directly
  }

  if (handleType == SQL_HANDLE_DESC && static_cast<DescriptorHandle*>(handle)->implicit) {
    odbcHandle->clearDiagnostics();
    odbcHandle->addDiagnostic("HY017", "Invalid use of an automatically allocated descriptor");
    return SQL_ERROR;
  }

  // Mark handle as freed
  odbcHandle->markAsFreed();

  // Release resources based on handleType
  switch (handleType) {
  case SQL_HANDLE_ENV: {
    logMessage("SQLFreeHandle: freeing environment handle.");
    delete static_cast<EnvironmentHandle*>(handle);
    break;
  }
  case SQL_HANDLE_DBC: {
    logMessage("SQLFreeHandle: freeing connection handle.");
    delete static_cast<ConnectionHandle*>(handle);
    break;
  }
  case SQL_HANDLE_STMT: {
    logMessage("SQLFreeHandle: freeing statement handle.");
    delete static_cast<StatementHandle*>(handle);
    break;
  }
  case SQL_HANDLE_DESC: {
    auto* descriptor = static_cast<DescriptorHandle*>(handle);
    for (auto* statement : descriptor->connection->statements) {
      if (statement->appRowDesc == descriptor)
        statement->appRowDesc = statement->implicitAppRowDesc;
      if (statement->appParamDesc == descriptor)
        statement->appParamDesc = statement->implicitAppParamDesc;
    }
    delete descriptor;
    break;
  }
  default: {
    logMessage("SQLFreeHandle: unknown handle type.");
    return SQL_ERROR; // Unknown handle type
  }
  }

  logMessage("Exiting SQLFreeHandle");
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT statementHandle, SQLUSMALLINT option) {
  const auto stmt = static_cast<StatementHandle*>(statementHandle);
  ConnectionHandle* cnct = stmt ? stmt->getConnection() : nullptr;
  logMessage(cnct, "SQLFreeStmt: Entering", LOG_LEVEL_TRACE);

  if (isLogLevelEnabled(cnct, LOG_LEVEL_TRACE)) {
    std::stringstream logStream;
    logStream << "SQLFreeStmt parameters: "
              << "statementHandle = " << handleToString(statementHandle) << ", option = " << option;
    logMessage(cnct, logStream.str(), LOG_LEVEL_TRACE);
  }

  if (!statementHandle) {
    logMessage(cnct, "SQLFreeStmt: Invalid statement handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  stmt->clearDiagnostics();

  switch (option) {
  case SQL_CLOSE: {
    logMessage(cnct, "SQLFreeStmt: Processing SQL_CLOSE option", LOG_LEVEL_DEBUG);
    // Close cursor and reset result set
    stmt->curRow = -1; // Reset current row
    stmt->ClearResultSet();
    logMessage(cnct, "SQLFreeStmt: Cursor closed and result set reset", LOG_LEVEL_DEBUG);
    break;
  }

  case SQL_UNBIND: {
    logMessage(cnct, "SQLFreeStmt: Processing SQL_UNBIND option", LOG_LEVEL_DEBUG);
    // Unbind columns
    if (stmt->resultSetPtr) {
      stmt->resultSetPtr->unbindColumns();
    }
    stmt->columnBindings.clear();
    auto* ard = static_cast<DescriptorHandle*>(stmt->appRowDesc);
    ard->records.clear();
    ard->count = 0;
    logMessage(cnct, "SQLFreeStmt: Column bindings released", LOG_LEVEL_DEBUG);
    break;
  }

  case SQL_RESET_PARAMS: {
    logMessage(cnct, "SQLFreeStmt: Processing SQL_RESET_PARAMS option", LOG_LEVEL_DEBUG);
    stmt->parameterBindings.clear();
    stmt->needsParameterData = false;
    stmt->nextDataSet = 0;
    stmt->activeDataSet = std::numeric_limits<SQLULEN>::max();
    stmt->nextDataParameter = 0;
    stmt->activeDataParameter = std::numeric_limits<size_t>::max();
    auto* apd = static_cast<DescriptorHandle*>(stmt->appParamDesc);
    apd->records.clear();
    apd->count = 0;
    auto* ipd = static_cast<DescriptorHandle*>(stmt->impParamDesc);
    ipd->records.clear();
    ipd->count = 0;
    break;
  }

  case SQL_DROP: {
    logMessage(cnct, "SQLFreeStmt: Processing SQL_DROP option", LOG_LEVEL_DEBUG);
    // This option is deprecated in ODBC 3.0, recommend using SQLFreeHandle instead
    logMessage(cnct, "SQLFreeStmt: SQL_DROP is deprecated in ODBC 3.0, use SQLFreeHandle instead",
               LOG_LEVEL_WARN);
    return SQLFreeHandle(SQL_HANDLE_STMT, statementHandle);
  }

  default: {
    logMessage(cnct, "SQLFreeStmt: Unrecognized option: " + std::to_string(option), LOG_LEVEL_WARN);
    stmt->addDiagnostic("HY092", "Invalid option value");
    return SQL_ERROR;
  }
  }
  logMessage(cnct, "SQLFreeStmt: Exiting successfully", LOG_LEVEL_TRACE);
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC connectionHandle, SQLINTEGER attribute,
                                    SQLPOINTER valuePtr, SQLINTEGER bufferLength,
                                    SQLINTEGER* stringLength) {
  ConnectionHandle* cnct = static_cast<ConnectionHandle*>(connectionHandle);

  logMessage(cnct, "SQLGetConnectAttr: Entering", LOG_LEVEL_TRACE);

  // Only build parameter log message if TRACE level is enabled
  if (isLogLevelEnabled(cnct, LOG_LEVEL_TRACE)) {
    std::stringstream logStream;
    logStream << "SQLGetConnectAttr parameters: "
              << "ConnectionHandle = " << handleToString(connectionHandle)
              << ", Attribute = " << attribute << ", ValuePtr = " << valueToString(valuePtr)
              << ", BufferLength = " << bufferLength
              << ", StringLengthPtr = " << valueToString(stringLength);
    logMessage(cnct, logStream.str(), LOG_LEVEL_TRACE);
  }

  // In SQLAllocHandle we're allocating a NULL_Handle, so I disabled this.
  if (!connectionHandle) {
    logMessage(cnct, "SQLGetConnectAttr: Invalid connection handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  cnct->clearDiagnostics();

  switch (attribute) {
  // Connection metadata attributes
  case SQL_ATTR_CONNECTION_DEAD:
    logMessage(cnct, "SQLGetConnectAttr: Processing SQL_ATTR_CONNECTION_DEAD", LOG_LEVEL_DEBUG);
    if (valuePtr) {
      *((SQLUINTEGER*)valuePtr) = SQL_CD_FALSE; // Assume connection is alive
      logMessage(cnct, "SQLGetConnectAttr: Connection status set to ALIVE", LOG_LEVEL_DEBUG);
    }
    break;

  case SQL_ATTR_CONNECTION_TIMEOUT:
    logMessage(cnct, "SQLGetConnectAttr: Processing SQL_ATTR_CONNECTION_TIMEOUT", LOG_LEVEL_DEBUG);
    if (valuePtr) {
      *((SQLUINTEGER*)valuePtr) = cnct->timeoutConnection;
      logMessage(cnct,
                 "SQLGetConnectAttr: Connection timeout set to " +
                     std::to_string(cnct->timeoutConnection),
                 LOG_LEVEL_DEBUG);
    }
    break;

  case SQL_ATTR_LOGIN_TIMEOUT:
    if (valuePtr)
      *static_cast<SQLUINTEGER*>(valuePtr) = cnct->timeoutLogin;
    break;

  // Transaction control attributes
  case SQL_ATTR_AUTOCOMMIT:
    logMessage(cnct, "SQLGetConnectAttr: Processing SQL_ATTR_AUTOCOMMIT", LOG_LEVEL_DEBUG);
    if (valuePtr) {
      *((SQLUINTEGER*)valuePtr) = cnct->autoCommit ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF;
      logMessage(cnct, "SQLGetConnectAttr: Auto-commit set to ON", LOG_LEVEL_DEBUG);
    }
    break;

  case SQL_ATTR_TXN_ISOLATION:
    logMessage(cnct, "SQLGetConnectAttr: Processing SQL_ATTR_TXN_ISOLATION", LOG_LEVEL_DEBUG);
    if (valuePtr) {
      *((SQLUINTEGER*)valuePtr) = 0; // SQL_TXN_CAPABLE is SQL_TC_NONE.
    }
    break;

  // Catalog/schema attributes
  case SQL_ATTR_CURRENT_CATALOG:
    logMessage(cnct, "SQLGetConnectAttr: Processing SQL_ATTR_CURRENT_CATALOG", LOG_LEVEL_DEBUG);
    {
      const SQLINTEGER requiredLength = static_cast<SQLINTEGER>(cnct->database.size());
      if (stringLength)
        *stringLength = requiredLength;
      if (!valuePtr)
        break;
      if (bufferLength <= 0) {
        cnct->addDiagnostic("HY090", "Invalid string or buffer length");
        return SQL_ERROR;
      }
      const size_t copyLength =
          std::min(cnct->database.size(), static_cast<size_t>(bufferLength - 1));
      std::memcpy(valuePtr, cnct->database.data(), copyLength);
      static_cast<char*>(valuePtr)[copyLength] = '\0';
      if (copyLength < cnct->database.size()) {
        cnct->addDiagnostic("01004", "String data, right truncated");
        return SQL_SUCCESS_WITH_INFO;
      }
      break;
    }

  // ODBC behavior control attributes
  case SQL_ATTR_ODBC_CURSORS:
    logMessage(cnct, "SQLGetConnectAttr: Processing SQL_ATTR_ODBC_CURSORS", LOG_LEVEL_DEBUG);
    if (valuePtr) {
      *((SQLUINTEGER*)valuePtr) = SQL_CUR_USE_DRIVER; // Use driver cursors
      logMessage(cnct, "SQLGetConnectAttr: ODBC cursor type set to USE_DRIVER", LOG_LEVEL_DEBUG);
    }
    break;

  case SQL_ATTR_ACCESS_MODE:
    logMessage(cnct, "SQLGetConnectAttr: Processing SQL_ATTR_ACCESS_MODE", LOG_LEVEL_DEBUG);
    if (valuePtr) {
      *((SQLUINTEGER*)valuePtr) = SQL_MODE_READ_WRITE; // Default read-write mode
      logMessage(cnct, "SQLGetConnectAttr: Access mode set to READ_WRITE", LOG_LEVEL_DEBUG);
    }
    break;

  default:
    logMessage(cnct, "SQLGetConnectAttr: Unsupported attribute: " + std::to_string(attribute),
               LOG_LEVEL_WARN);
    cnct->addDiagnostic("HY092", "Invalid attribute/option identifier");
    return SQL_ERROR;
  }

  if (stringLength && attribute != SQL_ATTR_CURRENT_CATALOG)
    *stringLength = 0;

  logMessage(cnct, "SQLGetConnectAttr: Exiting successfully", LOG_LEVEL_TRACE);
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetConnectOption(SQLHDBC connectionHandle, SQLUSMALLINT option,
                                      SQLPOINTER value) {
  if (!connectionHandle) {
    logMessage(nullptr, "SQLGetConnectOption: Invalid connection handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  switch (option) {
  case SQL_ACCESS_MODE:
    return SQLGetConnectAttr(connectionHandle, SQL_ATTR_ACCESS_MODE, value, 0, nullptr);
  case SQL_AUTOCOMMIT:
    return SQLGetConnectAttr(connectionHandle, SQL_ATTR_AUTOCOMMIT, value, 0, nullptr);
  case SQL_LOGIN_TIMEOUT:
    return SQLGetConnectAttr(connectionHandle, SQL_ATTR_LOGIN_TIMEOUT, value, 0, nullptr);
  case SQL_TXN_ISOLATION:
    return SQLGetConnectAttr(connectionHandle, SQL_ATTR_TXN_ISOLATION, value, 0, nullptr);
  case SQL_CURRENT_QUALIFIER:
    return SQLGetConnectAttr(connectionHandle, SQL_ATTR_CURRENT_CATALOG, value,
                             SQL_MAX_OPTION_STRING_LENGTH, nullptr);
  default:
    auto* cnct = static_cast<ConnectionHandle*>(connectionHandle);
    cnct->clearDiagnostics();
    cnct->addDiagnostic("HY092", "Invalid attribute/option identifier");
    return SQL_ERROR;
  }
}

SQLRETURN SQL_API SQLGetCursorName(SQLHSTMT statementHandle, SQLCHAR* cursorName,
                                   SQLSMALLINT bufferLength, SQLSMALLINT* nameLengthPtr) {
  if (!statementHandle) {
    logMessage(nullptr, "SQLGetCursorName: Invalid statement handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  auto stmt = static_cast<StatementHandle*>(statementHandle);
  stmt->clearDiagnostics();
  if (bufferLength < 0) {
    stmt->addDiagnostic("HY090", "Invalid string or buffer length");
    return SQL_ERROR;
  }
  SQLRETURN result = setString(stmt->cursorName, cursorName, bufferLength, nameLengthPtr);
  if (result == SQL_SUCCESS_WITH_INFO)
    stmt->addDiagnostic("01004", "Cursor name was truncated");
  return result;
}

// REST responses are decoded from the server's JSON result representation.
SQLRETURN SQL_API SQLGetData(const SQLHSTMT statementHandle, const SQLUSMALLINT columnNumber,
                             const SQLSMALLINT targetType, const SQLPOINTER targetValue,
                             const SQLLEN bufferLength, SQLLEN* strLenOrInd) {
  const auto stmt = static_cast<StatementHandle*>(statementHandle);
  ConnectionHandle* cnct = stmt ? stmt->getConnection() : nullptr;

  logMessage(cnct, "SQLGetData: Entering", LOG_LEVEL_TRACE);

  // Prepare log message for the parameters
  if (isLogLevelEnabled(cnct, LOG_LEVEL_TRACE)) {
    std::stringstream logStream;
    logStream << "SQLGetData parameters: "
              << "statementHandle = " << handleToString(statementHandle)
              << ", columnNumber = " << columnNumber
              << ", targetType = " << CDataTypeName(targetType) << ", targetValue = " << targetValue
              << ", bufferLength = " << bufferLength << ", strLenOrInd = " << strLenOrInd;
    logMessage(cnct, logStream.str(), LOG_LEVEL_TRACE);
  }

  // In SQLAllocHandle we're allocating a NULL_Handle, so I disabled this.
  if (!statementHandle) {
    logMessage(cnct, "SQLGetData: Invalid statement handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  stmt->clearDiagnostics();
  if (!stmt->resultSetPtr || stmt->curRow < 0 || stmt->curRow >= stmt->resultSetPtr->getNumRows()) {
    stmt->addDiagnostic("24000", "Invalid cursor state");
    return SQL_ERROR;
  }
  if (columnNumber == 0 || columnNumber > stmt->resultSetPtr->getNumColumns()) {
    stmt->addDiagnostic("07009", "Invalid descriptor index");
    return SQL_ERROR;
  }
  if (bufferLength < 0) {
    stmt->addDiagnostic("HY090", "Invalid string or buffer length");
    return SQL_ERROR;
  }

  logMessage(cnct, "SQLGetData: stmt->curRow = " + std::to_string(stmt->curRow), LOG_LEVEL_TRACE);

  if (stmt->curRow != stmt->lastGetDataRow) {
    stmt->lastGetDataRow = stmt->curRow;
    stmt->lastGetDataCol = 0;
    const int numCols = stmt->resultSetPtr ? stmt->resultSetPtr->getNumColumns() : 0;
    stmt->getDataOffsets.assign(static_cast<size_t>(numCols + 1), 0);
  }

  const ODBCField* valuePtr = nullptr;

  // Retrieve data as string and parse as JSON (works for both Session and REST APIs with our new approach)
  logMessage(cnct, "SQLGetData: Retrieving data for processing", LOG_LEVEL_DEBUG);

  try {
    valuePtr = &stmt->resultSetPtr->getValue(stmt->curRow, columnNumber - 1);
    if (valuePtr->isNull()) {
      logMessage(cnct,
                 "SQLGetData: NULL data retrieved for column " + std::to_string(columnNumber) +
                     ", row " + std::to_string(stmt->curRow),
                 LOG_LEVEL_DEBUG);
      if (strLenOrInd != nullptr) {
        logMessage(cnct, "SQLGetData: Setting strLenOrInd to SQL_NULL_DATA", LOG_LEVEL_DEBUG);
        *strLenOrInd = SQL_NULL_DATA; // Complies with ODBC specification, marks data as NULL
        return SQL_SUCCESS;
      } else {
        // Indicator pointer not provided, triggers ODBC standard error: 22002
        logMessage(cnct,
                   "SQLGetData: Error - Indicator variable (strLenOrInd) required but not supplied "
                   "for NULL value",
                   LOG_LEVEL_ERROR);
        stmt->addDiagnostic("22002", "Indicator variable required but not supplied");
        return SQL_ERROR;
      }
    } else {
      logMessage(cnct, "SQLGetData: Successfully retrieved value", LOG_LEVEL_DEBUG);
    }
  } catch (const std::exception& e) {
    logMessage(cnct, "SQLGetData: Unexpected error during data retrieval: " + std::string(e.what()),
               LOG_LEVEL_ERROR);
    stmt->addDiagnostic("HY000", "Failed to retrieve data value");
    return SQL_ERROR;
  }

  if (columnNumber != stmt->lastGetDataCol &&
      stmt->getDataOffsets.size() > static_cast<size_t>(columnNumber)) {
    stmt->getDataOffsets[columnNumber] = 0;
  }
  stmt->lastGetDataCol = columnNumber;

  SQLSMALLINT effectiveTargetType = (targetType == SQL_C_DEFAULT)
                                        ? stmt->resultSetPtr->getDefaultCTypeForColumn(columnNumber)
                                        : targetType;

  SQLRETURN copyResult = SQL_SUCCESS;
  if (effectiveTargetType == SQL_C_CHAR || effectiveTargetType == SQL_C_WCHAR ||
      effectiveTargetType == SQL_C_BINARY) {
    copyResult = CopyFieldToTargetVariable(stmt, *valuePtr, effectiveTargetType,
                                           const_cast<SQLPOINTER>(targetValue), bufferLength,
                                           strLenOrInd, columnNumber);
  } else {
    copyResult =
        CopyFieldToTarget(stmt, *valuePtr, effectiveTargetType, const_cast<SQLPOINTER>(targetValue),
                          bufferLength, strLenOrInd, false, 0);
  }
  if (copyResult != SQL_SUCCESS && copyResult != SQL_SUCCESS_WITH_INFO &&
      copyResult != SQL_NO_DATA) {
    logMessage(cnct,
               "SQLGetData: Unknown target type: " + CDataTypeName(targetType) +
                   ", returning SQL_ERROR",
               LOG_LEVEL_TRACE);
    return copyResult;
  }

  logMessage(cnct, "SQLGetData: Exiting successfully", LOG_LEVEL_TRACE);
  return copyResult;
}

SQLRETURN SQL_API SQLGetDescField(SQLHDESC descriptorHandle, SQLSMALLINT recNumber,
                                  SQLSMALLINT fieldIdentifier, SQLPOINTER value,
                                  SQLINTEGER bufferLength, SQLINTEGER* stringLength) {
  if (!descriptorHandle)
    return SQL_INVALID_HANDLE;
  auto* descriptor = static_cast<DescriptorHandle*>(descriptorHandle);
  if (descriptor->getHandleType() != SQL_HANDLE_DESC)
    return SQL_INVALID_HANDLE;
  descriptor->clearDiagnostics();

  if (descriptor->role == DescriptorRole::IMPLEMENTATION_ROW && descriptor->owner)
    PopulateImplementationRowDescriptor(descriptor->owner);

  auto setStringField = [&](const std::string& text) -> SQLRETURN {
    if (stringLength)
      *stringLength = static_cast<SQLINTEGER>(text.size());
    if (!value)
      return SQL_SUCCESS;
    if (bufferLength < 0) {
      descriptor->addDiagnostic("HY090", "Invalid string or buffer length");
      return SQL_ERROR;
    }
    if (bufferLength == 0)
      return text.empty() ? SQL_SUCCESS : SQL_SUCCESS_WITH_INFO;
    const size_t copied = std::min(text.size(), static_cast<size_t>(bufferLength - 1));
    std::memcpy(value, text.data(), copied);
    static_cast<char*>(value)[copied] = '\0';
    return copied < text.size() ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
  };
  auto setSmallInt = [&](SQLSMALLINT result) {
    if (value)
      *static_cast<SQLSMALLINT*>(value) = result;
    if (stringLength)
      *stringLength = sizeof(SQLSMALLINT);
    return SQL_SUCCESS;
  };
  auto setLength = [&](SQLLEN result) {
    if (value)
      *static_cast<SQLLEN*>(value) = result;
    if (stringLength)
      *stringLength = sizeof(SQLLEN);
    return SQL_SUCCESS;
  };
  auto setUnsignedLength = [&](SQLULEN result) {
    if (value)
      *static_cast<SQLULEN*>(value) = result;
    if (stringLength)
      *stringLength = sizeof(SQLULEN);
    return SQL_SUCCESS;
  };
  auto setPointer = [&](SQLPOINTER result) {
    if (value)
      *static_cast<SQLPOINTER*>(value) = result;
    if (stringLength)
      *stringLength = sizeof(SQLPOINTER);
    return SQL_SUCCESS;
  };

  switch (fieldIdentifier) {
  case SQL_DESC_ALLOC_TYPE:
    return setSmallInt(descriptor->implicit ? SQL_DESC_ALLOC_AUTO : SQL_DESC_ALLOC_USER);
  case SQL_DESC_ARRAY_SIZE:
    return setUnsignedLength(descriptor->arraySize);
  case SQL_DESC_ARRAY_STATUS_PTR:
    return setPointer(descriptor->arrayStatusPtr);
  case SQL_DESC_BIND_OFFSET_PTR:
    return setPointer(descriptor->bindOffsetPtr);
  case SQL_DESC_BIND_TYPE:
    return setUnsignedLength(descriptor->bindType);
  case SQL_DESC_COUNT:
    return setSmallInt(descriptor->count);
  case SQL_DESC_ROWS_PROCESSED_PTR:
    return setPointer(descriptor->rowsProcessedPtr);
  default:
    break;
  }

  if (descriptor->role == DescriptorRole::IMPLEMENTATION_ROW && descriptor->owner &&
      !descriptor->owner->resultSetPtr) {
    descriptor->addDiagnostic("HY007", "Associated statement is not prepared or executed");
    return SQL_ERROR;
  }

  if (recNumber < 0) {
    descriptor->addDiagnostic("07009", "Invalid descriptor index");
    return SQL_ERROR;
  }
  if (recNumber == 0 || recNumber > descriptor->count)
    return SQL_NO_DATA;
  const DescriptorRecord* record = descriptor->findRecord(recNumber);
  if (!record)
    return SQL_NO_DATA;
  switch (fieldIdentifier) {
  case SQL_DESC_CONCISE_TYPE:
    return setSmallInt(record->conciseType);
  case SQL_DESC_TYPE:
    return setSmallInt(record->type);
  case SQL_DESC_DATETIME_INTERVAL_CODE:
    return setSmallInt(record->datetimeIntervalCode);
  case SQL_DESC_OCTET_LENGTH:
    return setLength(record->octetLength);
  case SQL_DESC_LENGTH:
    return setUnsignedLength(record->length);
  case SQL_DESC_PRECISION:
    return setSmallInt(record->precision);
  case SQL_DESC_SCALE:
    return setSmallInt(record->scale);
  case SQL_DESC_NULLABLE:
    return setSmallInt(record->nullable);
  case SQL_DESC_PARAMETER_TYPE:
    return setSmallInt(record->parameterType);
  case SQL_DESC_UNNAMED:
    return setSmallInt(record->unnamed);
  case SQL_DESC_NUM_PREC_RADIX:
    if (value)
      *static_cast<SQLINTEGER*>(value) = record->numPrecRadix;
    if (stringLength)
      *stringLength = sizeof(SQLINTEGER);
    return SQL_SUCCESS;
  case SQL_DESC_DATA_PTR:
    return setPointer(record->dataPtr);
  case SQL_DESC_INDICATOR_PTR:
    return setPointer(record->indicatorPtr);
  case SQL_DESC_OCTET_LENGTH_PTR:
    return setPointer(record->octetLengthPtr);
  case SQL_DESC_NAME:
    return setStringField(record->name);
  default:
    descriptor->addDiagnostic("HY091", "Invalid descriptor field identifier");
    return SQL_ERROR;
  }
}

SQLRETURN SQL_API SQLGetDescRec(SQLHDESC descriptorHandle, SQLSMALLINT recNumber, SQLCHAR* name,
                                SQLSMALLINT bufferLength, SQLSMALLINT* stringLengthPtr,
                                SQLSMALLINT* typePtr, SQLSMALLINT* subTypePtr, SQLLEN* lengthPtr,
                                SQLSMALLINT* precisionPtr, SQLSMALLINT* scalePtr,
                                SQLSMALLINT* nullablePtr) {
  if (!descriptorHandle)
    return SQL_INVALID_HANDLE;
  auto* descriptor = static_cast<DescriptorHandle*>(descriptorHandle);
  if (descriptor->getHandleType() != SQL_HANDLE_DESC)
    return SQL_INVALID_HANDLE;
  descriptor->clearDiagnostics();
  if (descriptor->role == DescriptorRole::IMPLEMENTATION_ROW && descriptor->owner &&
      !descriptor->owner->resultSetPtr) {
    descriptor->addDiagnostic("HY007", "Associated statement is not prepared or executed");
    return SQL_ERROR;
  }
  if (recNumber < 1) {
    descriptor->addDiagnostic("07009", "Invalid descriptor index");
    return SQL_ERROR;
  }
  if (descriptor->role == DescriptorRole::IMPLEMENTATION_ROW && descriptor->owner)
    PopulateImplementationRowDescriptor(descriptor->owner);
  const DescriptorRecord* record = descriptor->findRecord(recNumber);
  if (!record || recNumber > descriptor->count)
    return SQL_NO_DATA;
  SQLINTEGER nameLength32 = 0;
  SQLRETURN result = SQLGetDescField(descriptorHandle, recNumber, SQL_DESC_NAME, name, bufferLength,
                                     &nameLength32);
  record = descriptor->findRecord(recNumber);
  if (stringLengthPtr)
    *stringLengthPtr = static_cast<SQLSMALLINT>(std::min<SQLINTEGER>(nameLength32, SHRT_MAX));
  if (typePtr)
    *typePtr = record->type;
  if (subTypePtr)
    *subTypePtr = record->datetimeIntervalCode;
  if (lengthPtr)
    *lengthPtr = static_cast<SQLLEN>(record->length);
  if (precisionPtr)
    *precisionPtr = record->precision;
  if (scalePtr)
    *scalePtr = record->scale;
  if (nullablePtr)
    *nullablePtr = record->nullable;
  return result;
}

SQLRETURN SQL_API SQLGetDiagField(const SQLSMALLINT handleType, const SQLHANDLE handle,
                                  const SQLSMALLINT recNumber, const SQLSMALLINT diagIdentifier,
                                  const SQLPOINTER diagInfoPtr, const SQLSMALLINT bufferLength,
                                  SQLSMALLINT* stringLengthPtr) {
  if (!handle)
    return SQL_INVALID_HANDLE;
  auto* odbcHandle = static_cast<ODBCHandle*>(handle);
  if (odbcHandle->getHandleType() != handleType)
    return SQL_INVALID_HANDLE;
  if (bufferLength < 0)
    return SQL_ERROR;

  if (diagIdentifier == SQL_DIAG_NUMBER) {
    if (recNumber != 0)
      return SQL_ERROR;
    if (diagInfoPtr)
      *static_cast<SQLINTEGER*>(diagInfoPtr) =
          static_cast<SQLINTEGER>(odbcHandle->diagnosticCount());
    if (stringLengthPtr)
      *stringLengthPtr = sizeof(SQLINTEGER);
    return SQL_SUCCESS;
  }
  if (recNumber <= 0)
    return SQL_ERROR;

  std::string sqlState;
  std::string message;
  int nativeError = 0;
  if (!odbcHandle->getDiagnostic(recNumber, sqlState, message, nativeError))
    return SQL_NO_DATA;

  switch (diagIdentifier) {
  case SQL_DIAG_SQLSTATE:
    return setString(sqlState, diagInfoPtr, bufferLength, stringLengthPtr);
  case SQL_DIAG_NATIVE:
    if (diagInfoPtr)
      *static_cast<SQLINTEGER*>(diagInfoPtr) = nativeError;
    if (stringLengthPtr)
      *stringLengthPtr = sizeof(SQLINTEGER);
    return SQL_SUCCESS;
  case SQL_DIAG_MESSAGE_TEXT:
    return setString(message, diagInfoPtr, bufferLength, stringLengthPtr);
  case SQL_DIAG_CLASS_ORIGIN:
  case SQL_DIAG_SUBCLASS_ORIGIN:
    return setString(sqlState.compare(0, 2, "IM") == 0 || sqlState.compare(0, 2, "HY") == 0
                         ? "ODBC 3.0"
                         : "ISO 9075",
                     diagInfoPtr, bufferLength, stringLengthPtr);
  case SQL_DIAG_CONNECTION_NAME:
    return setString("", diagInfoPtr, bufferLength, stringLengthPtr);
  case SQL_DIAG_SERVER_NAME:
    return setString("Apache IoTDB", diagInfoPtr, bufferLength, stringLengthPtr);
  case SQL_DIAG_ROW_NUMBER:
    if (diagInfoPtr)
      *static_cast<SQLLEN*>(diagInfoPtr) = SQL_NO_ROW_NUMBER;
    return SQL_SUCCESS;
  case SQL_DIAG_COLUMN_NUMBER:
    if (diagInfoPtr)
      *static_cast<SQLINTEGER*>(diagInfoPtr) = SQL_NO_COLUMN_NUMBER;
    return SQL_SUCCESS;
  default:
    return SQL_ERROR;
  }
}

SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT handleType, SQLHANDLE handle, SQLSMALLINT recNumber,
                                SQLCHAR* sqlStatePtr, SQLINTEGER* nativeErrorPtr,
                                SQLCHAR* messageTextPtr, SQLSMALLINT bufferLength,
                                SQLSMALLINT* textLengthPtr) {
  if (!handle)
    return SQL_INVALID_HANDLE;
  auto* odbcHandle = static_cast<ODBCHandle*>(handle);
  if (odbcHandle->getHandleType() != handleType)
    return SQL_INVALID_HANDLE;
  if (recNumber <= 0 || bufferLength < 0)
    return SQL_ERROR;

  std::string sqlState;
  std::string message;
  int nativeError = 0;
  if (!odbcHandle->getDiagnostic(recNumber, sqlState, message, nativeError))
    return SQL_NO_DATA;

  if (sqlStatePtr) {
    std::memcpy(sqlStatePtr, sqlState.data(), std::min<size_t>(5, sqlState.size()));
    sqlStatePtr[std::min<size_t>(5, sqlState.size())] = '\0';
  }
  if (nativeErrorPtr)
    *nativeErrorPtr = nativeError;
  if (textLengthPtr)
    *textLengthPtr = static_cast<SQLSMALLINT>(
        std::min(message.size(), static_cast<size_t>(std::numeric_limits<SQLSMALLINT>::max())));

  if (!messageTextPtr)
    return SQL_SUCCESS;
  if (bufferLength == 0)
    return message.empty() ? SQL_SUCCESS : SQL_SUCCESS_WITH_INFO;
  const size_t copyLength = std::min(message.size(), static_cast<size_t>(bufferLength - 1));
  std::memcpy(messageTextPtr, message.data(), copyLength);
  messageTextPtr[copyLength] = '\0';
  return copyLength < message.size() ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetEnvAttr(SQLHENV environmentHandle, SQLINTEGER attribute, SQLPOINTER value,
                                SQLINTEGER bufferLength, SQLINTEGER* stringLength) {
  if (!environmentHandle) {
    logMessage(nullptr, "SQLGetEnvAttr: Invalid environment handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  auto env = static_cast<EnvironmentHandle*>(environmentHandle);
  env->clearDiagnostics();
  if (!value) {
    env->addDiagnostic("HY009", "Invalid use of null pointer");
    return SQL_ERROR;
  }
  (void)bufferLength;
  switch (attribute) {
  case SQL_ATTR_ODBC_VERSION:
    *static_cast<SQLINTEGER*>(value) = env->odbcVersion;
    break;
  case SQL_ATTR_OUTPUT_NTS:
    *static_cast<SQLINTEGER*>(value) = SQL_TRUE;
    break;
  default:
    env->addDiagnostic("HY092", "Invalid attribute/option identifier");
    return SQL_ERROR;
  }
  if (stringLength)
    *stringLength = sizeof(SQLINTEGER);
  return SQL_SUCCESS;
}

static bool IsSupportedOdbcFunction(SQLUSMALLINT functionId) {
  static const SQLUSMALLINT supportedFunctions[] = {
      SQL_API_SQLALLOCCONNECT,   SQL_API_SQLALLOCENV,         SQL_API_SQLALLOCHANDLE,
      SQL_API_SQLALLOCSTMT,      SQL_API_SQLBINDCOL,          SQL_API_SQLBINDPARAM,
      SQL_API_SQLBINDPARAMETER,  SQL_API_SQLCANCEL,           SQL_API_SQLCLOSECURSOR,
      SQL_API_SQLCOLATTRIBUTE,   SQL_API_SQLCOLUMNS,          SQL_API_SQLCONNECT,
      SQL_API_SQLCOPYDESC,       SQL_API_SQLDATASOURCES,      SQL_API_SQLDESCRIBECOL,
      SQL_API_SQLDISCONNECT,     SQL_API_SQLDRIVERS,          SQL_API_SQLENDTRAN,
      SQL_API_SQLERROR,          SQL_API_SQLEXECDIRECT,       SQL_API_SQLEXECUTE,
      SQL_API_SQLFETCH,          SQL_API_SQLFETCHSCROLL,      SQL_API_SQLFREECONNECT,
      SQL_API_SQLFREEENV,        SQL_API_SQLFREEHANDLE,       SQL_API_SQLFREESTMT,
      SQL_API_SQLGETCONNECTATTR, SQL_API_SQLGETCONNECTOPTION, SQL_API_SQLGETCURSORNAME,
      SQL_API_SQLGETDATA,        SQL_API_SQLGETDESCFIELD,     SQL_API_SQLGETDESCREC,
      SQL_API_SQLGETDIAGFIELD,   SQL_API_SQLGETDIAGREC,       SQL_API_SQLGETENVATTR,
      SQL_API_SQLGETFUNCTIONS,   SQL_API_SQLGETINFO,          SQL_API_SQLGETSTMTATTR,
      SQL_API_SQLGETSTMTOPTION,  SQL_API_SQLGETTYPEINFO,      SQL_API_SQLMORERESULTS,
      SQL_API_SQLNUMPARAMS,      SQL_API_SQLNUMRESULTCOLS,    SQL_API_SQLNATIVESQL,
      SQL_API_SQLPARAMDATA,      SQL_API_SQLPREPARE,          SQL_API_SQLPUTDATA,
      SQL_API_SQLROWCOUNT,       SQL_API_SQLSETCONNECTATTR,   SQL_API_SQLSETCONNECTOPTION,
      SQL_API_SQLSETCURSORNAME,  SQL_API_SQLSETDESCFIELD,     SQL_API_SQLSETDESCREC,
      SQL_API_SQLSETENVATTR,     SQL_API_SQLSETPARAM,         SQL_API_SQLSETSTMTATTR,
      SQL_API_SQLSETSTMTOPTION,  SQL_API_SQLSPECIALCOLUMNS,   SQL_API_SQLSTATISTICS,
      SQL_API_SQLTABLES,         SQL_API_SQLTRANSACT,         SQL_API_SQLDRIVERCONNECT};
#ifdef SQL_API_SQLCANCELHANDLE
  if (functionId == SQL_API_SQLCANCELHANDLE)
    return true;
#endif
  return std::find(std::begin(supportedFunctions), std::end(supportedFunctions), functionId) !=
         std::end(supportedFunctions);
}

SQLRETURN SQL_API SQLGetFunctions(SQLHDBC connectionHandle, SQLUSMALLINT functionId,
                                  SQLUSMALLINT* supportedPtr) {
  if (!connectionHandle)
    return SQL_INVALID_HANDLE;
  auto* cnct = static_cast<ConnectionHandle*>(connectionHandle);
  cnct->clearDiagnostics();
  if (!supportedPtr) {
    cnct->addDiagnostic("HY009", "Invalid use of null pointer");
    return SQL_ERROR;
  }

  if (functionId == SQL_API_ALL_FUNCTIONS) {
    std::fill(supportedPtr, supportedPtr + 100, SQL_FALSE);
    for (SQLUSMALLINT id = 0; id < 100; ++id) {
      if (IsSupportedOdbcFunction(id))
        supportedPtr[id] = SQL_TRUE;
    }
    return SQL_SUCCESS;
  }
  if (functionId == SQL_API_ODBC3_ALL_FUNCTIONS) {
    std::fill(supportedPtr, supportedPtr + SQL_API_ODBC3_ALL_FUNCTIONS_SIZE, 0);
    for (SQLUSMALLINT id = 0; id < SQL_API_ODBC3_ALL_FUNCTIONS_SIZE * 16; ++id) {
      if (IsSupportedOdbcFunction(id))
        supportedPtr[id >> 4] |= static_cast<SQLUSMALLINT>(1U << (id & 0x000F));
    }
    return SQL_SUCCESS;
  }

  *supportedPtr = IsSupportedOdbcFunction(functionId) ? SQL_TRUE : SQL_FALSE;
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLNativeSql(SQLHDBC connectionHandle, SQLCHAR* inStatementText,
                               SQLINTEGER textLength1, SQLCHAR* outStatementText,
                               SQLINTEGER bufferLength, SQLINTEGER* textLength2Ptr) {
  if (!connectionHandle)
    return SQL_INVALID_HANDLE;
  auto* cnct = static_cast<ConnectionHandle*>(connectionHandle);
  cnct->clearDiagnostics();
  if (!inStatementText) {
    cnct->addDiagnostic("HY009", "Invalid use of null pointer");
    return SQL_ERROR;
  }
  if ((textLength1 < 0 && textLength1 != SQL_NTS) || bufferLength < 0) {
    cnct->addDiagnostic("HY090", "Invalid string or buffer length");
    return SQL_ERROR;
  }

  const size_t inputLength = textLength1 == SQL_NTS
                                 ? std::strlen(reinterpret_cast<const char*>(inStatementText))
                                 : static_cast<size_t>(textLength1);
  if (textLength2Ptr) {
    *textLength2Ptr = static_cast<SQLINTEGER>(
        std::min(inputLength, static_cast<size_t>(std::numeric_limits<SQLINTEGER>::max())));
  }
  if (!outStatementText)
    return SQL_SUCCESS;
  if (bufferLength == 0) {
    cnct->addDiagnostic("01004", "String data, right truncated");
    return inputLength == 0 ? SQL_SUCCESS : SQL_SUCCESS_WITH_INFO;
  }

  const size_t copyLength = std::min(inputLength, static_cast<size_t>(bufferLength - 1));
  std::memcpy(outStatementText, inStatementText, copyLength);
  outStatementText[copyLength] = '\0';
  if (copyLength < inputLength) {
    cnct->addDiagnostic("01004", "String data, right truncated");
    return SQL_SUCCESS_WITH_INFO;
  }
  return SQL_SUCCESS;
}

/*
See the following link for data type mapping:
https://learn.microsoft.com/en-us/sql/odbc/reference/appendixes/c-data-types?view=sql-server-ver16

SQL_C_USHORT = SQLUSMALLINT = unsigned short int (default)
SQL_C_ULONG = SQLUINTEGER = unsigned long int
SQL_C_SSHORT = SQLSMALLINT = short int
SQL_C_SLONG = SQLINTEGER = long int
*/
void SQLGetInfoSetNumeric(ConnectionHandle* cnct, std::string name, SQLUINTEGER value,
                          SQLPOINTER infoValuePtr, SQLSMALLINT valueType,
                          SQLSMALLINT* stringLengthPtr) {
  logMessage(cnct, "SQLGetInfoSetNumeric: name = " + name, LOG_LEVEL_TRACE);
  if (!infoValuePtr) {
    logMessage(cnct,
               "SQLGetInfoSetString: infoValuePtr = nullptr. Application is probing for required "
               "buffer size.",
               LOG_LEVEL_DEBUG);
    if (stringLengthPtr) {
      // Return required buffer size in bytes for the numeric type
      switch (valueType) {
      case SQL_C_USHORT:
        *stringLengthPtr = sizeof(SQLUSMALLINT);
        break;
      case SQL_C_ULONG:
        *stringLengthPtr = sizeof(SQLUINTEGER);
        break;
      case SQL_C_SSHORT:
        *stringLengthPtr = sizeof(SQLSMALLINT);
        break;
      case SQL_C_SLONG:
        *stringLengthPtr = sizeof(SQLINTEGER);
        break;
      default:
        logMessage(cnct, "SQLGetInfoSetNumeric: Unsupported valueType during probing",
                   LOG_LEVEL_ERROR);
        *stringLengthPtr = 0;
      }
    }
    return;
  }

  switch (valueType) {
  case SQL_C_USHORT:
    *static_cast<SQLUSMALLINT*>(infoValuePtr) = static_cast<SQLUSMALLINT>(value);
    break;

  case SQL_C_ULONG:
    *static_cast<SQLUINTEGER*>(infoValuePtr) = value;
    break;

  case SQL_C_SSHORT:
    *static_cast<SQLSMALLINT*>(infoValuePtr) = static_cast<SQLSMALLINT>(value);
    break;

  case SQL_C_SLONG:
    *static_cast<SQLINTEGER*>(infoValuePtr) = static_cast<SQLINTEGER>(value);
    break;

  default:
    logMessage(cnct, "SQLGetInfoSetNumeric: Invalid valueType\n", LOG_LEVEL_TRACE);
  }

  if (stringLengthPtr) {
    switch (valueType) {
    case SQL_C_USHORT:
      *stringLengthPtr = sizeof(SQLUSMALLINT);
      break;
    case SQL_C_ULONG:
      *stringLengthPtr = sizeof(SQLUINTEGER);
      break;
    case SQL_C_SSHORT:
      *stringLengthPtr = sizeof(SQLSMALLINT);
      break;
    case SQL_C_SLONG:
      *stringLengthPtr = sizeof(SQLINTEGER);
      break;
    default:
      *stringLengthPtr = 0;
    }
  }

  if (isLogLevelEnabled(cnct, LOG_LEVEL_TRACE)) {
    std::ostringstream oss;
    oss << "SQLGetInfoSetNumeric returning " << name << " = " << value;
    logMessage(cnct, oss.str(), LOG_LEVEL_TRACE);
  }
}

void SQLGetInfoSetString(ConnectionHandle* cnct, std::string name, std::string value,
                         SQLPOINTER infoValuePtr, SQLSMALLINT bufferLength,
                         SQLSMALLINT* stringLengthPtr) {
  logMessage(cnct, "SQLGetInfoSetString: name = " + name, LOG_LEVEL_TRACE);

  if (!infoValuePtr) {
    logMessage(cnct,
               "SQLGetInfoSetString: infoValuePtr = nullptr. Application is probing for required "
               "buffer size.",
               LOG_LEVEL_DEBUG);
    if (stringLengthPtr) {
      *stringLengthPtr = static_cast<SQLSMALLINT>(
          std::min(value.size(), static_cast<size_t>(std::numeric_limits<SQLSMALLINT>::max())));
    }
    return;
  }

  if (bufferLength <= 0) {
    logMessage(cnct, "SQLGetInfoSetString: bufferLength <= 0. Exiting.", LOG_LEVEL_ERROR);
    cnct->addDiagnostic(bufferLength < 0 ? "HY090" : "01004",
                        bufferLength < 0 ? "Invalid string or buffer length"
                                         : "String data, right truncated");
    return;
  }

  size_t max_copy = std::min(value.size(), static_cast<size_t>(bufferLength - 1));
  std::strncpy(static_cast<char*>(infoValuePtr), value.data(), max_copy);
  static_cast<char*>(infoValuePtr)[max_copy] = '\0';
  if (max_copy < value.size())
    cnct->addDiagnostic("01004", "String data, right truncated");

  if (stringLengthPtr) {
    // return the actual length of answer, not the copied length.
    *stringLengthPtr = static_cast<SQLSMALLINT>(value.size());
  }

  logMessage(cnct, "SQLGetInfoSetString returning " + name, LOG_LEVEL_TRACE);
}

std::string getServerVersion(SQLHDBC connectionHandle) {
  ConnectionHandle* cnct = static_cast<ConnectionHandle*>(connectionHandle);
  logMessage(cnct, "getServerVersion: Entering.", LOG_LEVEL_TRACE);

  // Create a temporary statement handle for the query
  std::unique_ptr<StatementHandle> stmt(new StatementHandle(cnct));
  SQLRETURN ret = IoTDB_ExecDirect(stmt.get(), "show version");

  if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
    logMessage(cnct, "getServerVersion: Failed to execute show version query", LOG_LEVEL_ERROR);
    return "Unknown";
  }

  if (stmt->resultSetPtr == nullptr) {
    logMessage(cnct, "getServerVersion: No result set available", LOG_LEVEL_ERROR);
    return "Unknown";
  }

  // Get the version string from the first cell
  std::string version = stmt->resultSetPtr->getValue(0, 0).toString();
  if (version.empty()) {
    logMessage(cnct, "getServerVersion: Empty version string", LOG_LEVEL_WARN);
    version = "Unknown";
  }

  logMessage(cnct, "getServerVersion: Retrieved version: " + version, LOG_LEVEL_DEBUG);

  logMessage(cnct, "getServerVersion: Exiting.", LOG_LEVEL_TRACE);
  return version;
}

std::string formatVersion(const std::string& ver) {
  // ODBC requires ##.##.####. Accept suffixes such as "-SNAPSHOT" and
  // return a stable unknown value instead of throwing through the C ABI.
  const int widths[] = {2, 2, 4};
  unsigned int parts[] = {0, 0, 0};
  size_t offset = 0;
  bool foundNumber = false;
  for (size_t part = 0; part < 3 && offset < ver.size(); ++part) {
    size_t end = offset;
    unsigned int parsed = 0;
    const unsigned int maximum = part < 2 ? 99U : 9999U;
    while (end < ver.size() && std::isdigit(static_cast<unsigned char>(ver[end]))) {
      foundNumber = true;
      const unsigned int digit = static_cast<unsigned int>(ver[end] - '0');
      parsed = parsed > (maximum - digit) / 10U ? maximum : parsed * 10U + digit;
      ++end;
    }
    parts[part] = parsed;
    if (end >= ver.size() || ver[end] != '.')
      break;
    offset = end + 1;
  }
  if (!foundNumber)
    return "00.00.0000";

  std::ostringstream oss;
  for (size_t i = 0; i < 3; ++i) {
    if (i > 0)
      oss << ".";
    oss << std::setw(widths[i]) << std::setfill('0') << parts[i];
  }
  return oss.str();
}

SQLRETURN SQL_API SQLGetInfo(SQLHDBC connectionHandle, SQLUSMALLINT infoType,
                             SQLPOINTER infoValuePtr, SQLSMALLINT bufferLength,
                             SQLSMALLINT* stringLengthPtr) {
  /*
    About this function:
    https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetinfo-function?view=sql-server-ver16
    */
  ConnectionHandle* cnct = static_cast<ConnectionHandle*>(connectionHandle);
  logMessage(cnct, "Entering SQLGetInfo", LOG_LEVEL_TRACE);

  if (isLogLevelEnabled(cnct, LOG_LEVEL_TRACE)) {
    std::stringstream logStream;
    logStream << "Parameters: "
              << "connectionHandle = " << handleToString(connectionHandle)
              << ", infoValuePtr = " << infoValuePtr << ", infoType = " << infoType
              << ", bufferLength = " << bufferLength << ", stringLengthPtr = " << stringLengthPtr;
    logMessage(cnct, logStream.str(), LOG_LEVEL_TRACE);
  }

  SQLUINTEGER value = 0;

  if (!connectionHandle) {
    logMessage(cnct, "SQLGetInfo: Invalid connectionHandle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  cnct->clearDiagnostics();

  switch (infoType) {
  case SQL_DRIVER_NAME: // 6 sqlext.h
#ifdef _WIN32
    SQLGetInfoSetString(cnct, "SQL_DRIVER_NAME", "apache_iotdb_odbc.dll", infoValuePtr,
                        bufferLength, stringLengthPtr);
#else
    SQLGetInfoSetString(cnct, "SQL_DRIVER_NAME", "libapache_iotdb_odbc.so", infoValuePtr,
                        bufferLength, stringLengthPtr);
#endif
    break;

  case SQL_DRIVER_ODBC_VER: // 77 sqlext.h
    SQLGetInfoSetString(cnct, "SQL_DRIVER_ODBC_VER", "03.80", infoValuePtr, bufferLength,
                        stringLengthPtr);
    break;

  case SQL_DRIVER_VER: // 7 sqlext.h
    SQLGetInfoSetString(cnct, "SQL_DRIVER_VER", DRIVER_VERSION, infoValuePtr, bufferLength,
                        stringLengthPtr);
    break;

  case SQL_DATABASE_NAME: // 16 sqlext.h
    SQLGetInfoSetString(cnct, "SQL_DATABASE_NAME", cnct->database, infoValuePtr, bufferLength,
                        stringLengthPtr);
    break;

  case SQL_ACTIVE_CONNECTIONS: // 0 sqlext.h (Redefinition of SQL_MAX_DRIVER_CONNECTIONS from sql.h)
    // Zero means that the limit is unknown or not defined.
    SQLGetInfoSetNumeric(cnct, "SQL_ACTIVE_CONNECTIONS", 0, infoValuePtr, SQL_C_USHORT,
                         stringLengthPtr);
    break;

  case SQL_MAX_TABLE_NAME_LEN: // 35 sql.h
    SQLGetInfoSetNumeric(cnct, "SQL_MAX_TABLE_NAME_LEN", 64, infoValuePtr, SQL_C_USHORT,
                         stringLengthPtr); // Example value
    break;

  case SQL_ACTIVE_STATEMENTS: // 1 sqlext.h (Redefinition of SQL_MAX_CONCURRENT_ACTIVITIES from swl.h)
    SQLGetInfoSetNumeric(cnct, "SQL_ACTIVE_STATEMENTS", 0, infoValuePtr, SQL_C_USHORT,
                         stringLengthPtr);
    break;

// iODBC only seems to support ODBC 3.0
#ifdef SQL_OV_ODBC3_80
  case SQL_ASYNC_DBC_FUNCTIONS: // 10023 sqlext.h
    SQLGetInfoSetNumeric(cnct, "SQL_ASYNC_DBC_FUNCTIONS", 0, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    // Most drivers do not support this
    break;

  case SQL_ASYNC_NOTIFICATION: // 10025 sqlext.h
    SQLGetInfoSetNumeric(cnct, "SQL_ASYNC_NOTIFICATION", SQL_ASYNC_NOTIFICATION_NOT_CAPABLE,
                         infoValuePtr, SQL_C_USHORT, stringLengthPtr);
    // Default for simpler drivers
    break;
#endif

  case SQL_CURSOR_COMMIT_BEHAVIOR: // 23
    SQLGetInfoSetNumeric(cnct, "SQL_CURSOR_COMMIT_BEHAVIOR", SQL_CB_CLOSE, infoValuePtr,
                         SQL_C_USHORT, stringLengthPtr);
    // Specifies what happens to cursors on a commit.
    // Possible values:
    // SQL_CB_CLOSE: Cursors are closed on commit.
    // SQL_CB_DELETE: Cursors are closed and rows are deleted on commit.
    // SQL_CB_PRESERVE: Cursors remain open on commit.
    break;

  case SQL_CURSOR_ROLLBACK_BEHAVIOR: // 24
    SQLGetInfoSetNumeric(cnct, "SQL_CURSOR_ROLLBACK_BEHAVIOR", SQL_CB_CLOSE, infoValuePtr,
                         SQL_C_USHORT, stringLengthPtr);
    // Specifies what happens to cursors on a rollback.
    // Possible values:
    // SQL_CB_CLOSE: Cursors are closed on rollback.
    // SQL_CB_DELETE: Cursors are closed and rows are deleted on rollback.
    // SQL_CB_PRESERVE: Cursors remain open on rollback.
    break;

  case SQL_GETDATA_EXTENSIONS: // 81
    SQLGetInfoSetNumeric(cnct, "SQL_GETDATA_EXTENSIONS", SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER,
                         infoValuePtr, SQL_C_ULONG, stringLengthPtr);
    // Indicates which `SQLGetData` features are supported.
    // Possible values (bitmask):
    // SQL_GD_BLOCK: Allows block fetch with SQLGetData.
    // SQL_GD_BOUND: Allows data to be retrieved to bound columns and buffers.
    // SQL_GD_ANY_COLUMN: Allows retrieval of data from any column.
    // SQL_GD_ANY_ORDER: Allows retrieval of data in any order of columns.
    break;

  case SQL_DTC_TRANSITION_COST: // 1750
    SQLGetInfoSetNumeric(cnct, "SQL_DTC_TRANSITION_COST", 0, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    // Cost (in milliseconds) for a Distributed Transaction Coordinator (DTC) transaction transition.
    // This value should be adjusted based on the driver and system's transaction coordination cost.
    break;

  case SQL_DESCRIBE_PARAMETER: // 10002
    SQLGetInfoSetString(cnct, "SQL_DESCRIBE_PARAMETER", "N", infoValuePtr, bufferLength,
                        stringLengthPtr);
    break;

  case SQL_DBMS_NAME: // 17
    // Return the name of the data management system. Returned Apache IoTDB
    SQLGetInfoSetString(cnct, "SQL_DBMS_NAME", "Apache IoTDB", infoValuePtr, bufferLength,
                        stringLengthPtr);
    break;

  case SQL_DBMS_VER: // 18
  {
    // Query and normalize the connected server's version.
    std::string versionStr = getServerVersion(connectionHandle);
    versionStr = formatVersion(versionStr);
    SQLGetInfoSetString(cnct, "SQL_DBMS_VER", versionStr, infoValuePtr, bufferLength,
                        stringLengthPtr);
    break;
  }

  case SQL_IDENTIFIER_QUOTE_CHAR: // 29
    // Identifier quote character. For IoTDB tree model this character is backtick (`), for table model it's "
    if (cnct->isTableModel) {
      SQLGetInfoSetString(cnct, "SQL_IDENTIFIER_QUOTE_CHAR", "\"", infoValuePtr, bufferLength,
                          stringLengthPtr);
    } else {
      SQLGetInfoSetString(cnct, "SQL_IDENTIFIER_QUOTE_CHAR", " ", infoValuePtr, bufferLength,
                          stringLengthPtr);
    }
    break;

  case SQL_CATALOG_NAME: //10003
                         /*
    If the server supports catalog names, returns "Y"; if not, returns "N".
    Drivers conforming to SQL-92 Full level standard will always return "Y".
     */
    SQLGetInfoSetString(cnct, "SQL_CATALOG_NAME", cnct->isTableModel ? "Y" : "N", infoValuePtr,
                        bufferLength, stringLengthPtr);
    break;

  case SQL_SCHEMA_USAGE: // 91
    // IoTDB exposes no separate schema component through this driver.
    SQLGetInfoSetNumeric(cnct, "SQL_SCHEMA_USAGE", 0, infoValuePtr, SQL_C_ULONG, stringLengthPtr);
    break;
    /*
        This attribute is a byte mask.
        SQL_SU_DML_STATEMENTS mode can be used for all Data Manipulation Language (DML) statements:
        • SELECT, INSERT, UPDATE, DELETE
        • If supported, also includes SELECT FOR UPDATE and positioned update/delete statements.

        SQL_SU_PROCEDURE_INVOCATION mode can be used for ODBC stored procedure call statements (such as {CALL schema.proc_name}).

        SQL_SU_TABLE_DEFINITION mode can be used for all table definition statements:
        • CREATE TABLE, CREATE VIEW
        • ALTER TABLE, DROP TABLE, DROP VIEW.

        SQL_SU_INDEX_DEFINITION mode can be used for all index definition statements:
        • CREATE INDEX, DROP INDEX.

        SQL_SU_PRIVILEGE_DEFINITION mode can be used for all privilege definition statements:
        • GRANT, REVOKE.

         */

  case SQL_CATALOG_USAGE: // 92
    // Table-model databases occupy the ODBC catalog component. Tree model has no catalog.
    value = cnct->isTableModel ? SQL_SU_DML_STATEMENTS | SQL_SU_TABLE_DEFINITION : 0;
    SQLGetInfoSetNumeric(cnct, "SQL_CATALOG_USAGE", value, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    break;

  case SQL_CATALOG_NAME_SEPARATOR: // 41
    // Returns the character or string that the data source uses to separate catalog names from subsequent or preceding qualified name elements.
    SQLGetInfoSetString(cnct, "SQL_CATALOG_NAME_SEPARATOR", cnct->isTableModel ? "." : "",
                        infoValuePtr, bufferLength, stringLengthPtr);
    break;

  case SQL_CATALOG_LOCATION: // 114
    // This value indicates the position of the catalog name in the full table name:
    // SQL_CL_START Catalog name is at the beginning of the table name (like file system path style)
    SQLGetInfoSetNumeric(cnct, "SQL_CATALOG_LOCATION", cnct->isTableModel ? SQL_CL_START : 0,
                         infoValuePtr, SQL_C_USHORT, stringLengthPtr);
    break;

  case SQL_SQL_CONFORMANCE:
    // IoTDB SQL is model-specific; do not claim a complete SQL-92 conformance level.
    SQLGetInfoSetNumeric(cnct, "SQL_SQL_CONFORMANCE", 0, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    break;
    /*
    This value is used to identify the SQL standard level supported by the driver, mainly including the following four compliance levels:

        Return Value	Description
        SQL_SC_SQL92_ENTRY	Conforms to SQL-92 Entry Level standard
        SQL_SC_SQL92_INTERMEDIATE	Conforms to SQL-92 Intermediate Level standard
        SQL_SC_SQL92_FULL	Conforms to SQL-92 Full Level standard
        SQL_SC_FIPS127_2_TRANSITIONAL	Conforms to FIPS 127-2 Transitional Level standard
    */

  case SQL_MAX_COLUMNS_IN_ORDER_BY: //99
    // This value specifies the maximum number of columns allowed in ORDER BY clause
    SQLGetInfoSetNumeric(cnct, "SQL_MAX_COLUMNS_IN_ORDER_BY", 0, infoValuePtr, SQL_C_USHORT,
                         stringLengthPtr);
    break;

  case SQL_MAX_IDENTIFIER_LEN: // 10005
    // Zero means no driver-enforced limit is known.
    SQLGetInfoSetNumeric(cnct, "SQL_MAX_IDENTIFIER_LEN", 0, infoValuePtr, SQL_C_USHORT,
                         stringLengthPtr);
    break;

  case SQL_MAX_COLUMNS_IN_GROUP_BY: // 97
    // This value specifies the maximum number of columns allowed in a GROUP BY clause
    SQLGetInfoSetNumeric(cnct, "SQL_MAX_COLUMNS_IN_GROUP_BY", 0, infoValuePtr, SQL_C_USHORT,
                         stringLengthPtr);
    break;

  case SQL_MAX_COLUMNS_IN_SELECT: // 100
    // This attribute specifies the maximum number of columns that can be returned in a SELECT statement
    SQLGetInfoSetNumeric(cnct, "SQL_MAX_COLUMNS_IN_SELECT", 0, infoValuePtr, SQL_C_USHORT,
                         stringLengthPtr);
    break;

  case SQL_ORDER_BY_COLUMNS_IN_SELECT: // 90
    // "Y" if columns in ORDER BY clause must be in select list; otherwise "N".
    SQLGetInfoSetString(cnct, "SQL_ORDER_BY_COLUMNS_IN_SELECT", "N", infoValuePtr, bufferLength,
                        stringLengthPtr);
    break;

  case SQL_STRING_FUNCTIONS: // 50
    // ODBC scalar escape translation ({fn ...}) is not implemented.
    SQLGetInfoSetNumeric(cnct, "SQL_STRING_FUNCTIONS", 0, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    break;

  case SQL_AGGREGATE_FUNCTIONS: // 169
    SQLGetInfoSetNumeric(cnct, "SQL_AGGREGATE_FUNCTIONS", 0, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    break;

  case SQL_SQL92_PREDICATES: // 160
    // No portable SQL-92 predicate subset is advertised.
    SQLGetInfoSetNumeric(cnct, "SQL_SQL92_PREDICATES", 0, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    break;

  case SQL_SQL92_RELATIONAL_JOIN_OPERATORS: // 161
    // Relational join operators supported in SELECT statements
    SQLGetInfoSetNumeric(cnct, "SQL_SQL92_RELATIONAL_JOIN_OPERATORS", 0, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    break;

  case SQL_SQL92_VALUE_EXPRESSIONS: // 165
    SQLGetInfoSetNumeric(cnct, "SQL_SQL92_VALUE_EXPRESSIONS", 0, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    break;

  case SQL_COLUMN_ALIAS: // 87
    SQLGetInfoSetString(cnct, "SQL_COLUMN_ALIAS", "Y", infoValuePtr, bufferLength, stringLengthPtr);
    break;

  case SQL_GROUP_BY: // 88
    // The portable ODBC GROUP BY grammar is not advertised.
    SQLGetInfoSetNumeric(cnct, "SQL_GROUP_BY", SQL_GB_NOT_SUPPORTED, infoValuePtr, SQL_C_USHORT,
                         stringLengthPtr);
    break;

  case SQL_NUMERIC_FUNCTIONS: // 49
    SQLGetInfoSetNumeric(cnct, "SQL_NUMERIC_FUNCTIONS", 0, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    break;

  case SQL_TIMEDATE_FUNCTIONS: // 52
    SQLGetInfoSetNumeric(cnct, "SQL_TIMEDATE_FUNCTIONS", 0, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    break;

  case SQL_SYSTEM_FUNCTIONS: // 51
    SQLGetInfoSetNumeric(cnct, "SQL_SYSTEM_FUNCTIONS", 0, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    break;

  case SQL_TIMEDATE_ADD_INTERVALS:
    // ODBC TIMESTAMPADD escape intervals are not translated.

    SQLGetInfoSetNumeric(cnct, "SQL_TIMEDATE_ADD_INTERVALS", 0, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    break;

  case SQL_TIMEDATE_DIFF_INTERVALS:
    // ODBC TIMESTAMPDIFF escape intervals are not translated.
    SQLGetInfoSetNumeric(cnct, "SQL_TIMEDATE_DIFF_INTERVALS", 0, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    break;

  case SQL_CONCAT_NULL_BEHAVIOR:
    // Indicates how the data source handles concatenation of NULL valued character data type columns with non-NULL valued character data type columns.
    // SQL_CB_NULL = Result is NULL value.
    // SQL_CB_NON_NULL = Result is concatenation of non-NULL valued column or columns.
    // Drivers conforming to SQL-92 Entry Level standard will always return SQL_CB_NULL.
    SQLGetInfoSetNumeric(cnct, "SQL_CONCAT_NULL_BEHAVIOR", SQL_CB_NULL, infoValuePtr, SQL_C_USHORT,
                         stringLengthPtr);
    break;

  case SQL_CATALOG_TERM:
    // A character string that gives the name that the data source vendor uses for catalogs, such as "database" or "directory". This string can be upper case, lower case, or mixed case.
    SQLGetInfoSetString(cnct, "SQL_CATALOG_TERM", cnct->isTableModel ? "database" : "",
                        infoValuePtr, bufferLength, stringLengthPtr);
    break;

  case SQL_SCHEMA_TERM:
    // A character string that gives the name that the data source vendor uses for schemas, which can be upper case, lower case, or mixed case.
    // Returns an empty string if the data source does not support schemas.
    SQLGetInfoSetString(cnct, "SQL_SCHEMA_TERM", "", infoValuePtr, bufferLength, stringLengthPtr);
    break;

  case SQL_ODBC_INTERFACE_CONFORMANCE:
    // A SQLUINTEGER value that indicates the level of ODBC 3.x interface conformance of the driver.
    /*
        SQL_OIC_CORE: The minimum level that all ODBC drivers should conform to. This level includes basic interface elements such as connection functions, functions for preparing and executing SQL statements, basic result set metadata functions, basic catalog functions, etc.
        SQL_OIC_LEVEL1: Includes core standard compliance level features, plus scrollable cursors, bookmarks, positioned updates and deletes, etc.
        SQL_OIC_LEVEL2: Includes Level 1 standard compliance level features, plus advanced features such as sensitive cursors; update, delete and refresh by bookmarks; stored procedure support; catalog functions for primary and foreign keys; multi-catalog support, etc.
     */
    SQLGetInfoSetNumeric(cnct, "SQL_ODBC_INTERFACE_CONFORMANCE", SQL_OIC_CORE, infoValuePtr,
                         SQL_C_USHORT, stringLengthPtr);
    break;

  case SQL_SEARCH_PATTERN_ESCAPE:
    /*
    A character string that specifies the escape character supported by the driver, which allows using pattern matching metacharacters underscore (_) and percent (%) as literal characters in search patterns.
    This escape character only applies to catalog function parameters that support search strings. If this string is empty, the driver does not support search pattern escape characters.
    */
    SQLGetInfoSetString(cnct, "SQL_SEARCH_PATTERN_ESCAPE", "\\", infoValuePtr, bufferLength,
                        stringLengthPtr);
    break;

  case SQL_CONVERT_FUNCTIONS:
    /*
    A SQLUINTEGER bitmask that enumerates the scalar conversion functions supported by the driver and associated data source.
    The following bitmasks are used to determine which conversion functions are supported:
    SQL_FN_CVT_CAST
    SQL_FN_CVT_CONVERT
    */
    SQLGetInfoSetNumeric(cnct, "SQL_CONVERT_FUNCTIONS", 0, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    break;

  case SQL_CONVERT_BIGINT:
  case SQL_CONVERT_BINARY:
  case SQL_CONVERT_BIT:
  case SQL_CONVERT_CHAR:
  case SQL_CONVERT_DATE:
  case SQL_CONVERT_DECIMAL:
  case SQL_CONVERT_DOUBLE:
  case SQL_CONVERT_FLOAT:
  case SQL_CONVERT_GUID:
  case SQL_CONVERT_INTEGER:
  case SQL_CONVERT_LONGVARCHAR:
  case SQL_CONVERT_NUMERIC:
  case SQL_CONVERT_REAL:
  case SQL_CONVERT_SMALLINT:
  case SQL_CONVERT_TIME:
  case SQL_CONVERT_TIMESTAMP:
  case SQL_CONVERT_TINYINT:
  case SQL_CONVERT_VARBINARY:
  case SQL_CONVERT_VARCHAR:
  case SQL_CONVERT_LONGVARBINARY:
    /*
    This series of attributes identifies the data type conversion capabilities supported by the data source through bitmasks, used for the CONVERT() scalar function. Each SQL_CONVERT_<type> corresponds to conversion support for a specific data type.

    The mask content is composed of multiple SQL_CVT_<type> ORed together.

    No ODBC CONVERT escape conversions are advertised.
     */
    SQLGetInfoSetNumeric(cnct, "SQL_CONVERT_xxx", 0, infoValuePtr, SQL_C_ULONG, stringLengthPtr);
    break;

  case SQL_CONVERT_WCHAR:
  case SQL_CONVERT_WLONGVARCHAR:
  case SQL_CONVERT_WVARCHAR:
    /*
        These three seem to be in the same series as the above. The corresponding SQL_CVT_<type> also exists.
        But these three things cannot be found in the official documentation. These macros can only be found in header files.
     */
    SQLGetInfoSetNumeric(cnct, "SQL_CONVERT_xxx_wide", 0, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    break;

  case SQL_SPECIAL_CHARACTERS:
    /* A string containing all special characters that can be included in identifiers
    besides letters, numbers, and underscores. Returns empty string if none.
    */
    SQLGetInfoSetString(cnct, "SQL_SPECIAL_CHARACTERS", "", infoValuePtr, bufferLength,
                        stringLengthPtr);
    break;

    /********
     * The following parameters have been deprecated in ODBC 3.0,
     * but Excel uses ODBC 2.0, so support is needed for Excel compatibility ********/

  case SQL_POS_OPERATIONS: // 79
    // Bitmask indicating the types of operations supported by the data source through the SQLSetPos function.
    // SQLSetPos is not supported, so no positioned operations are advertised.
    SQLGetInfoSetNumeric(cnct, "SQL_POS_OPERATIONS", 0, infoValuePtr, SQL_C_SLONG, stringLengthPtr);
    break;

  case SQL_STATIC_SENSITIVITY: // 83
    // Indicates whether the application can detect changes made to static or keyset-driven cursors by SQLSetPos or positioned update/delete statements
    // Static cursors and SQLSetPos are not supported.
    SQLGetInfoSetNumeric(cnct, "SQL_STATIC_SENSITIVITY", 0, infoValuePtr, SQL_C_SLONG,
                         stringLengthPtr);
    break;

  case SQL_LOCK_TYPES: // 78
    // Bitmask enumerating the lock types supported in the fLock argument of SQLSetPos function
    // SQLSetPos lock modes are not supported.
    SQLGetInfoSetNumeric(cnct, "SQL_LOCK_TYPES", 0, infoValuePtr, SQL_C_SLONG, stringLengthPtr);
    break;

  case SQL_TXN_ISOLATION_OPTION: // 72。
    // Bitmask indicating the transaction isolation levels supported by the driver and data source. Each bit corresponds to a specific isolation level
    // IoTDB has no transaction concept
    SQLGetInfoSetNumeric(cnct, "SQL_TXN_ISOLATION_OPTION", 0, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    break;

  case SQL_BOOKMARK_PERSISTENCE: // 82
    // Bitmask enumerating the operations in which bookmarks remain valid
    SQLGetInfoSetNumeric(cnct, "SQL_BOOKMARK_PERSISTENCE", 0, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    break;

  case SQL_SCROLL_OPTIONS: // 44:
    // Bitmask enumerating the scroll options supported for scrollable cursors.
    SQLGetInfoSetNumeric(cnct, "SQL_SCROLL_OPTIONS", SQL_SO_FORWARD_ONLY, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    break;

  case SQL_SCROLL_CONCURRENCY: // 43
    // Bitmask enumerating the concurrency control options supported by cursors
    SQLGetInfoSetNumeric(cnct, "SQL_SCROLL_CONCURRENCY", SQL_SCCO_READ_ONLY, infoValuePtr,
                         SQL_C_SLONG, stringLengthPtr);
    break;

  case SQL_DYNAMIC_CURSOR_ATTRIBUTES1:
    // Bitmask describing the attributes of dynamic cursors supported by the driver.
    SQLGetInfoSetNumeric(cnct, "SQL_DYNAMIC_CURSOR_ATTRIBUTES1", 0, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    break;

  case SQL_KEYSET_CURSOR_ATTRIBUTES1:
    // Bitmask describing the attributes of keyset-driven cursors supported by the driver. This bitmask contains the first set of attributes; see SQL_KEYSET_CURSOR_ATTRIBUTES2 for the second set.
    SQLGetInfoSetNumeric(cnct, "SQL_KEYSET_CURSOR_ATTRIBUTES1", 0, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    break;

  case SQL_STATIC_CURSOR_ATTRIBUTES1:
    // Bitmask describing the attributes of static cursors supported by the driver. This bitmask contains the first set of attributes; see SQL_STATIC_CURSOR_ATTRIBUTES2 for the second set.
    SQLGetInfoSetNumeric(cnct, "SQL_STATIC_CURSOR_ATTRIBUTES1", 0, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    break;

  case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1: // 146
      // Bitmask describing the attributes of forward-only cursors supported by the driver. This bitmask contains the first set of attributes; see SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2 for the second set.
    SQLGetInfoSetNumeric(cnct, "SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1", SQL_CA1_NEXT, infoValuePtr,
                         SQL_C_ULONG, stringLengthPtr);
    break;

  case SQL_KEYSET_CURSOR_ATTRIBUTES2:
    // Bitmask describing the attributes of keyset-driven cursors supported by the driver. This bitmask contains the second set of attributes; see SQL_KEYSET_CURSOR_ATTRIBUTES1 for the first set.
    SQLGetInfoSetNumeric(cnct, "SQL_KEYSET_CURSOR_ATTRIBUTES2", 0, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    break;

  case SQL_STATIC_CURSOR_ATTRIBUTES2:
    // Bitmask describing the attributes of static cursors supported by the driver. This bitmask contains the second set of attributes; see SQL_STATIC_CURSOR_ATTRIBUTES1 for the first set.
    SQLGetInfoSetNumeric(cnct, "SQL_STATIC_CURSOR_ATTRIBUTES2", 0, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    break;

  case SQL_NEED_LONG_DATA_LEN:
    // A character string: "Y" if the data source needs to have the length of long data values (data type SQL_LONGVARCHAR, SQL_LONGVARBINARY, or a data source-specific long data type) before sending them to the data source; "N" if it does not.
    SQLGetInfoSetString(cnct, "SQL_NEED_LONG_DATA_LEN", "N", infoValuePtr, bufferLength,
                        stringLengthPtr);
    break;

  case SQL_TXN_CAPABLE:
    // A SQLUSMALLINT value describing transaction support in the driver or data source
    SQLGetInfoSetNumeric(cnct, "SQL_TXN_CAPABLE", SQL_TC_NONE, infoValuePtr, SQL_C_USHORT,
                         stringLengthPtr); //iotdb has no transactions
    break;

  case SQL_NON_NULLABLE_COLUMNS:
    // A SQLUSMALLINT value specifying whether the data source supports NOT NULL constraints in columns
    SQLGetInfoSetNumeric(cnct, "SQL_NON_NULLABLE_COLUMNS", SQL_NNC_NON_NULL, infoValuePtr,
                         SQL_C_USHORT, stringLengthPtr); //iotdb has no transactions
    break;

  case SQL_DATA_SOURCE_NAME:
    // A character string representing the data source name used during connection. If the application called SQLConnect, this is the value of the szDSN parameter. If the application called SQLDriverConnect or SQLBrowseConnect, this is the value of the DSN keyword in the connection string passed to the driver. If the connection string does not contain a DSN keyword (for example, when it contains a DRIVER keyword), this is an empty string.
    SQLGetInfoSetString(cnct, "SQL_DATA_SOURCE_NAME", cnct->dataSourceName, infoValuePtr,
                        bufferLength, stringLengthPtr);
    break;

  case SQL_DATA_SOURCE_READ_ONLY:
    // A character string. "Y" if the data source is set to read-only mode; "N" otherwise.
    // This characteristic is only related to the data source itself, not the driver used to access the data source. A driver that supports read-write operations can be used with a read-only data source. If a driver is read-only, all its data sources must be read-only and must return SQL_DATA_SOURCE_READ_ONLY.
    SQLGetInfoSetString(cnct, "SQL_DATA_SOURCE_READ_ONLY", "N", infoValuePtr, bufferLength,
                        stringLengthPtr);
    break;

  case SQL_IDENTIFIER_CASE:
    // A SQLUSMALLINT value indicating case sensitivity.
    /*
    SQL_IC_UPPER     = Identifiers in SQL are case-insensitive and stored in uppercase in system catalogs.
    SQL_IC_LOWER     = Identifiers in SQL are case-insensitive and stored in lowercase in system catalogs.
    SQL_IC_SENSITIVE = Identifiers in SQL are case-sensitive and stored in mixed case in system catalogs.
    SQL_IC_MIXED     = Identifiers in SQL are case-insensitive and stored in mixed case in system catalogs.
    */
    SQLGetInfoSetNumeric(cnct, "SQL_IDENTIFIER_CASE", SQL_IC_SENSITIVE, infoValuePtr, SQL_C_USHORT,
                         stringLengthPtr);
    break;

  case SQL_MAX_INDEX_SIZE:
    // A SQLUINTEGER value specifying the maximum number of bytes allowed in the combined fields of an index. This value is set to zero if no limit is specified or the limit is unknown.
    SQLGetInfoSetNumeric(cnct, "SQL_MAX_INDEX_SIZE", 0, infoValuePtr, SQL_C_ULONG, stringLengthPtr);
    break;

  case SQL_MAX_ROW_SIZE:
    /*
    A SQLUINTEGER value that specifies the maximum length of a single row in a table. If no limit is specified or the limit is unknown, this value is set to zero.
    Drivers conforming to FIPS Entry level standards will return at least 2,000. Drivers conforming to FIPS Intermediate level standards will return at least 8,000.
    */
    SQLGetInfoSetNumeric(cnct, "SQL_MAX_ROW_SIZE", 0, infoValuePtr, SQL_C_ULONG, stringLengthPtr);
    break;

  case SQL_MAX_ROW_SIZE_INCLUDES_LONG:
    /*
    A character string: "Y" if the returned SQL_MAX_ROW_SIZE maximum row size information includes the length of all SQL_LONGVARCHAR and SQL_LONGVARBINARY type columns in the row; otherwise "N". */
    SQLGetInfoSetString(cnct, "SQL_MAX_ROW_SIZE_INCLUDES_LONG", "N", infoValuePtr, bufferLength,
                        stringLengthPtr);
    break;

  case SQL_MAX_TABLES_IN_SELECT:
    /*
    A SQLUSMALLINT value that specifies the maximum number of tables allowed in the FROM clause of a SELECT statement. If no limit is specified or the limit is unknown, this value is set to zero.
    Drivers conforming to FIPS Entry level standards will return at least 15. Drivers conforming to FIPS Intermediate level standards will return at least 50.
    */
    SQLGetInfoSetNumeric(cnct, "SQL_MAX_TABLES_IN_SELECT", 0, infoValuePtr, SQL_C_USHORT,
                         stringLengthPtr);
    break;

  case SQL_NULL_COLLATION:
    /*A SQLUSMALLINT value that specifies the sort position of NULL values in result sets.
    The options do not consider adjustable NULL value placement, so we can only choose to place them at the end based on IoTDB's default behavior.
    */
    SQLGetInfoSetNumeric(cnct, "SQL_NULL_COLLATION", SQL_NC_END, infoValuePtr, SQL_C_USHORT,
                         stringLengthPtr);
    break;

  case SQL_PROCEDURE_TERM:
    // A character string giving the name that the data source vendor uses for "procedure".
    // IoTDB exposes no stored procedures through this driver.
    SQLGetInfoSetString(cnct, "SQL_PROCEDURE_TERM", "", infoValuePtr, bufferLength,
                        stringLengthPtr);
    break;

  case SQL_QUOTED_IDENTIFIER_CASE:
    // A SQLUSMALLINT value describing the case sensitivity and storage of quoted identifiers in SQL
    // In SQL-92 standard they are case sensitive, but IoTDB seems not to be sensitive.
    SQLGetInfoSetNumeric(cnct, "SQL_QUOTED_IDENTIFIER_CASE", SQL_IC_SENSITIVE, infoValuePtr,
                         SQL_C_USHORT, stringLengthPtr);
    break;

  case SQL_ODBC_SQL_CONFORMANCE:
    // The driver accepts the ODBC minimum grammar used by interoperable applications.
    SQLGetInfoSetNumeric(cnct, "SQL_ODBC_SQL_CONFORMANCE", SQL_OSC_MINIMUM, infoValuePtr,
                         SQL_C_SSHORT, stringLengthPtr);
    break;

  case SQL_INTEGRITY:
    // A string: "Y" if the data source supports Integrity Enhancement Facility; "N" if it does not.
    // Includes entity integrity, referential integrity, domain integrity, user-defined integrity.
    SQLGetInfoSetString(cnct, "SQL_INTEGRITY", "N", infoValuePtr, bufferLength, stringLengthPtr);
    break;

  case SQL_SUBQUERIES:
    // A SQLUINTEGER bitmask enumerating the types of predicates supported in subqueries
    // Drivers conforming to SQL-92 Entry Level standard always return a bitmask with all these bits set.
    // However, IoTDB does not support correlated subqueries.

    SQLGetInfoSetNumeric(cnct, "SQL_SUBQUERIES", 0, infoValuePtr, SQL_C_ULONG, stringLengthPtr);
    break;

  case SQL_TABLE_TERM: // 45
    // The keyword in SQL that represents "table". Drivers conforming to SQL-92 Entry Level standard will always return "table".
    SQLGetInfoSetString(cnct, "SQL_TABLE_TERM", "table", infoValuePtr, bufferLength,
                        stringLengthPtr);
    break;

  case SQL_USER_NAME:
    SQLGetInfoSetString(cnct, "SQL_USER_NAME", cnct->userName.c_str(), infoValuePtr, bufferLength,
                        stringLengthPtr);
    break;

  case SQL_MULT_RESULT_SETS:
    // A character string: "Y" if the data source supports multiple result sets; "N" if it does not.
    SQLGetInfoSetString(cnct, "SQL_MULT_RESULT_SETS", "N", infoValuePtr, bufferLength,
                        stringLengthPtr);
    break;

  case SQL_SERVER_NAME:
    // A character string representing the server name as determined by the actual data source; this information is useful when connecting using SQLConnect, SQLDriverConnect, and SQLBrowseConnect when a data source name is used.
    // Report the connected endpoint as the server name.
    SQLGetInfoSetString(cnct, "SQL_SERVER_NAME", cnct->serverHostName, infoValuePtr, bufferLength,
                        stringLengthPtr);
    break;

  case SQL_ACCESSIBLE_PROCEDURES:
    // A character string: "Y" if the user can execute all procedures returned by SQLProcedures; "N" if there may be procedures the user cannot execute.
    SQLGetInfoSetString(cnct, "SQL_ACCESSIBLE_PROCEDURES", "N", infoValuePtr, bufferLength,
                        stringLengthPtr);
    break;

  case SQL_ACCESSIBLE_TABLES: // 19
    // A character string: "Y" if the user has SELECT privileges on all tables returned by SQLTables; "N" if there may be tables the user cannot access.
    SQLGetInfoSetString(cnct, "SQL_ACCESSIBLE_TABLES", "N", infoValuePtr, bufferLength,
                        stringLengthPtr);
    break;

  case SQL_OJ_CAPABILITIES: // 115
    // A SQLUINTEGER bitmask enumerating the types of outer joins supported by the driver and data source.
    SQLGetInfoSetNumeric(cnct, "SQL_OJ_CAPABILITIES", 0, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    break;

  case SQL_PROCEDURES: // 21
    // A character string: "Y" if the data source supports stored procedures and the driver supports ODBC procedure call syntax; "N" otherwise.
    SQLGetInfoSetString(cnct, "SQL_PROCEDURES", "N", infoValuePtr, bufferLength, stringLengthPtr);
    break;

  case SQL_LIKE_ESCAPE_CLAUSE: // 113
    // A character string: "Y" if the data source supports defining escape characters for percent (%) and underscore (_) characters in LIKE predicates and the driver supports ODBC-defined LIKE predicate escape character syntax; "N" otherwise.
    // IoTDB does not support LIKE.
    SQLGetInfoSetString(cnct, "SQL_LIKE_ESCAPE_CLAUSE", "N", infoValuePtr, bufferLength,
                        stringLengthPtr);
    break;

  case SQL_MAX_COLUMNS_IN_INDEX: // 98
    // A SQLUSMALLINT value specifying the maximum number of columns allowed in an index. This value is set to zero if no limit is specified or the limit is unknown.
    SQLGetInfoSetNumeric(cnct, "SQL_MAX_COLUMNS_IN_INDEX", 0, infoValuePtr, SQL_C_USHORT,
                         stringLengthPtr);
    break;

  case SQL_MAX_COLUMNS_IN_TABLE: // 101
    // A SQLUSMALLINT value specifying the maximum number of columns allowed in a table. This value is set to zero if no limit is specified or the limit is unknown.
    SQLGetInfoSetNumeric(cnct, "SQL_MAX_COLUMNS_IN_TABLE", 0, infoValuePtr, SQL_C_USHORT,
                         stringLengthPtr);
    break;

  case SQL_OUTER_JOINS: // 38
    SQLGetInfoSetString(cnct, "SQL_OUTER_JOINS", "N", infoValuePtr, bufferLength, stringLengthPtr);
    break;

  case SQL_FILE_USAGE: // 84
    // A SQLUSMALLINT value that indicates how a single-tier driver directly handles files in the data source
    // The driver is not a single-tier driver.
    SQLGetInfoSetNumeric(cnct, "SQL_FILE_USAGE", SQL_FILE_NOT_SUPPORTED, infoValuePtr, SQL_C_USHORT,
                         stringLengthPtr);
    break;

  case SQL_DEFAULT_TXN_ISOLATION: // 26
    // A SQLUINTEGER value that indicates the default transaction isolation level supported by the driver or data source, returns zero if the data source does not support transactions.
    SQLGetInfoSetNumeric(cnct, "SQL_DEFAULT_TXN_ISOLATION", 0, infoValuePtr, SQL_C_ULONG,
                         stringLengthPtr);
    break;

  case SQL_MAX_OWNER_NAME_LEN:
    /*
    An SQLUSMALLINT value that specifies the maximum length of a schema name in the data source. If there is no maximum length or the length is unknown, this value is set to zero.

    An FIPS Entry level-conformant driver will return at least 18. An FIPS Intermediate level-conformant driver will return at least 128.

    This InfoType has been renamed for ODBC 3.0 from the ODBC 2.0 InfoType SQL_MAX_OWNER_NAME_LEN.

    there's no schema in IoTDB. the "Database" = "catalog". returning 0.
    */
    SQLGetInfoSetNumeric(cnct, "SQL_MAX_OWNER_NAME_LEN", 0, infoValuePtr, SQL_C_USHORT,
                         stringLengthPtr);
    break;

  case SQL_MAX_CATALOG_NAME_LEN:
    SQLGetInfoSetNumeric(cnct, "SQL_MAX_CATALOG_NAME_LEN", 64, infoValuePtr, SQL_C_USHORT,
                         stringLengthPtr);
    break;

  default:
    logMessage(cnct, "SQLGetInfo: received unsupported InfoType:" + std::to_string(infoType) + "\n",
               LOG_LEVEL_ERROR);
    cnct->addDiagnostic("HY096", "Unsupported information type");
    return SQL_ERROR;
  }

  logMessage(cnct, "SQLGetInfo: Exiting\n", LOG_LEVEL_TRACE);
  if (cnct->diagnosticCount() == 0)
    return SQL_SUCCESS;
  std::string state;
  std::string message;
  int nativeError = 0;
  cnct->getDiagnostic(1, state, message, nativeError);
  return state == "01004" ? SQL_SUCCESS_WITH_INFO : SQL_ERROR;
}

SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT statementHandle, SQLINTEGER attribute, SQLPOINTER value,
                                 SQLINTEGER bufferLength, SQLINTEGER* stringLength) {
  const auto stmt = static_cast<StatementHandle*>(statementHandle);
  ConnectionHandle* cnct = stmt ? stmt->getConnection() : nullptr;
  logMessage(cnct, "SQLGetStmtAttr: Entering", LOG_LEVEL_TRACE);

  if (isLogLevelEnabled(cnct, LOG_LEVEL_TRACE)) {
    std::stringstream logStream;
    logStream << "SQLGetStmtAttr parameters: "
              << "statementHandle = " << handleToString(statementHandle)
              << ", attribute = " << attribute << ", value = " << valueToString(value)
              << ", bufferLength = " << bufferLength << ", stringLength = " << stringLength;
    logMessage(cnct, logStream.str(), LOG_LEVEL_TRACE);
  }

  // In SQLAllocHandle we're allocating a NULL_Handle, so I disabled this.
  if (!statementHandle) {
    logMessage(cnct, "SQLGetStmtAttr: Invalid statement handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  stmt->clearDiagnostics();

  const auto* ard = static_cast<DescriptorHandle*>(stmt->appRowDesc);
  const auto* apd = static_cast<DescriptorHandle*>(stmt->appParamDesc);
  const auto* ird = static_cast<DescriptorHandle*>(stmt->impRowDesc);
  const auto* ipd = static_cast<DescriptorHandle*>(stmt->impParamDesc);
  stmt->rowArraySize = ard->arraySize;
  stmt->rowBindType = ard->bindType;
  stmt->rowBindOffsetPtr = reinterpret_cast<SQLULEN*>(ard->bindOffsetPtr);
  stmt->rowsFetchedPtr = ird->rowsProcessedPtr;
  stmt->rowStatusPtr = ird->arrayStatusPtr;
  stmt->paramSetSize = apd->arraySize;
  stmt->paramBindType = apd->bindType;
  stmt->paramBindOffsetPtr = reinterpret_cast<SQLULEN*>(apd->bindOffsetPtr);
  stmt->paramsProcessedPtr = ipd->rowsProcessedPtr;
  stmt->paramStatusPtr = ipd->arrayStatusPtr;

  switch (attribute) {
  case SQL_ATTR_APP_ROW_DESC:
    // Return descriptor handle if applicable
    logMessage(cnct, "SQLGetStmtAttr: Processing SQL_ATTR_APP_ROW_DESC", LOG_LEVEL_DEBUG);
    if (value) {
      *static_cast<SQLHANDLE*>(value) =
          stmt->appRowDesc; // Replace with actual descriptor handle if available
      logMessage(cnct, "SQLGetStmtAttr: Returning application row descriptor handle",
                 LOG_LEVEL_DEBUG);
    }
    break;

  case SQL_ATTR_APP_PARAM_DESC:
    // Return descriptor handle if applicable
    logMessage(cnct, "SQLGetStmtAttr: Processing SQL_ATTR_APP_PARAM_DESC", LOG_LEVEL_DEBUG);
    if (value) {
      *static_cast<SQLHANDLE*>(value) = stmt->appParamDesc;
      logMessage(cnct, "SQLGetStmtAttr: Returning application parameter descriptor handle",
                 LOG_LEVEL_DEBUG);
    }
    break;

  case SQL_ATTR_IMP_ROW_DESC:
    // Return descriptor handle if applicable
    logMessage(cnct, "SQLGetStmtAttr: Processing SQL_ATTR_IMP_ROW_DESC", LOG_LEVEL_DEBUG);
    if (value) {
      *static_cast<SQLHANDLE*>(value) =
          stmt->impRowDesc; // Replace with actual descriptor handle if available
      logMessage(cnct, "SQLGetStmtAttr: Returning implementation row descriptor handle",
                 LOG_LEVEL_DEBUG);
    }
    break;

  case SQL_ATTR_IMP_PARAM_DESC:
    // Return descriptor handle if applicable
    logMessage(cnct, "SQLGetStmtAttr: Processing SQL_ATTR_IMP_PARAM_DESC", LOG_LEVEL_DEBUG);
    if (value) {
      *static_cast<SQLHANDLE*>(value) = stmt->impParamDesc;
      logMessage(cnct, "SQLGetStmtAttr: Returning implementation parameter descriptor handle",
                 LOG_LEVEL_DEBUG);
    }
    break;

  case SQL_ATTR_CURSOR_SCROLLABLE:
    logMessage(cnct, "SQLGetStmtAttr: Processing SQL_ATTR_CURSOR_SCROLLABLE", LOG_LEVEL_DEBUG);
    if (value) {
      *static_cast<SQLULEN*>(value) = SQL_NONSCROLLABLE; // Example: default to non-scrollable
      logMessage(cnct, "SQLGetStmtAttr: Cursor scrollable set to SQL_NONSCROLLABLE",
                 LOG_LEVEL_DEBUG);
    }
    break;

  case SQL_ATTR_CURSOR_SENSITIVITY:
    logMessage(cnct, "SQLGetStmtAttr: Processing SQL_ATTR_CURSOR_SENSITIVITY", LOG_LEVEL_DEBUG);
    if (value) {
      *static_cast<SQLULEN*>(value) = SQL_UNSPECIFIED; // Example: unspecified sensitivity
      logMessage(cnct, "SQLGetStmtAttr: Cursor sensitivity set to SQL_UNSPECIFIED",
                 LOG_LEVEL_DEBUG);
    }
    break;

  case SQL_ATTR_ROW_BIND_TYPE:
    logMessage(cnct, "SQLGetStmtAttr: Processing SQL_ATTR_ROW_BIND_TYPE", LOG_LEVEL_DEBUG);
    if (value) {
      *static_cast<SQLULEN*>(value) = stmt->rowBindType;
      logMessage(cnct, "SQLGetStmtAttr: Row bind type set to SQL_BIND_BY_COLUMN", LOG_LEVEL_DEBUG);
    }
    break;

  case SQL_ATTR_ROW_ARRAY_SIZE:
    if (value)
      *static_cast<SQLULEN*>(value) = stmt->rowArraySize;
    break;
  case SQL_ATTR_ROWS_FETCHED_PTR:
    if (value)
      *static_cast<SQLULEN**>(value) = stmt->rowsFetchedPtr;
    break;
  case SQL_ATTR_ROW_STATUS_PTR:
    if (value)
      *static_cast<SQLUSMALLINT**>(value) = stmt->rowStatusPtr;
    break;
  case SQL_ATTR_ROW_BIND_OFFSET_PTR:
    if (value)
      *static_cast<SQLULEN**>(value) = stmt->rowBindOffsetPtr;
    break;
  case SQL_ATTR_PARAM_STATUS_PTR:
    if (value)
      *static_cast<SQLUSMALLINT**>(value) = stmt->paramStatusPtr;
    break;
  case SQL_ATTR_PARAMS_PROCESSED_PTR:
    if (value)
      *static_cast<SQLULEN**>(value) = stmt->paramsProcessedPtr;
    break;
  case SQL_ATTR_PARAMSET_SIZE:
    if (value)
      *static_cast<SQLULEN*>(value) = stmt->paramSetSize;
    break;
  case SQL_ATTR_PARAM_BIND_TYPE:
    if (value)
      *static_cast<SQLULEN*>(value) = stmt->paramBindType;
    break;
  case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
    if (value)
      *static_cast<SQLULEN**>(value) = stmt->paramBindOffsetPtr;
    break;
  case SQL_ATTR_ENABLE_AUTO_IPD:
    if (value)
      *static_cast<SQLULEN*>(value) = stmt->enableAutoIPD;
    break;
  case SQL_ATTR_CURSOR_TYPE:
    if (value)
      *static_cast<SQLULEN*>(value) = stmt->cursorType;
    break;
  case SQL_ATTR_CONCURRENCY:
    if (value)
      *static_cast<SQLULEN*>(value) = stmt->concurrency;
    break;
  case SQL_ATTR_USE_BOOKMARKS:
    if (value)
      *static_cast<SQLULEN*>(value) = stmt->useBookmarks;
    break;
  case SQL_ATTR_MAX_ROWS:
    if (value)
      *static_cast<SQLULEN*>(value) = stmt->maxRows;
    break;
  case SQL_ATTR_MAX_LENGTH:
    if (value)
      *static_cast<SQLULEN*>(value) = stmt->maxLength;
    break;
  case SQL_ATTR_NOSCAN:
    if (value)
      *static_cast<SQLULEN*>(value) = stmt->noScan;
    break;
  case SQL_ATTR_RETRIEVE_DATA:
    if (value)
      *static_cast<SQLULEN*>(value) = stmt->retrieveData;
    break;
  case SQL_ATTR_METADATA_ID:
    if (value)
      *static_cast<SQLULEN*>(value) = stmt->metadataId;
    break;

  default:
    logMessage(cnct, "SQLGetStmtAttr: Unsupported attribute: " + std::to_string(attribute),
               LOG_LEVEL_WARN);
    stmt->addDiagnostic("HY092", "Invalid attribute/option identifier");
    return SQL_ERROR;
  }

  // Update StringLength if required
  if (stringLength) {
    *stringLength = 0; // Update with actual size if applicable
  }

  logMessage(cnct, "SQLGetStmtAttr: Exiting successfully", LOG_LEVEL_TRACE);
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetStmtOption(SQLHSTMT statementHandle, SQLUSMALLINT option,
                                   SQLPOINTER value) {
  if (!statementHandle) {
    logMessage(nullptr, "SQLGetStmtOption: Invalid statement handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  switch (option) {
  case SQL_QUERY_TIMEOUT:
    if (value)
      *static_cast<SQLULEN*>(value) = 0;
    return SQL_SUCCESS;
  case SQL_MAX_ROWS:
    return SQLGetStmtAttr(statementHandle, SQL_ATTR_MAX_ROWS, value, 0, nullptr);
  case SQL_NOSCAN:
    return SQLGetStmtAttr(statementHandle, SQL_ATTR_NOSCAN, value, 0, nullptr);
  case SQL_MAX_LENGTH:
    return SQLGetStmtAttr(statementHandle, SQL_ATTR_MAX_LENGTH, value, 0, nullptr);
  case SQL_BIND_TYPE:
    return SQLGetStmtAttr(statementHandle, SQL_ATTR_ROW_BIND_TYPE, value, 0, nullptr);
  case SQL_CURSOR_TYPE:
    return SQLGetStmtAttr(statementHandle, SQL_ATTR_CURSOR_TYPE, value, 0, nullptr);
  case SQL_CONCURRENCY:
    return SQLGetStmtAttr(statementHandle, SQL_ATTR_CONCURRENCY, value, 0, nullptr);
  case SQL_ROWSET_SIZE:
    return SQLGetStmtAttr(statementHandle, SQL_ATTR_ROW_ARRAY_SIZE, value, 0, nullptr);
  case SQL_RETRIEVE_DATA:
    return SQLGetStmtAttr(statementHandle, SQL_ATTR_RETRIEVE_DATA, value, 0, nullptr);
  case SQL_USE_BOOKMARKS:
    return SQLGetStmtAttr(statementHandle, SQL_ATTR_USE_BOOKMARKS, value, 0, nullptr);
  default:
    auto* stmt = static_cast<StatementHandle*>(statementHandle);
    stmt->clearDiagnostics();
    stmt->addDiagnostic("HY092", "Invalid attribute/option identifier");
    return SQL_ERROR;
  }
}

SQLRETURN SQL_API SQLGetTypeInfo(SQLHSTMT statementHandle, SQLSMALLINT dataType) {
  const auto stmt = static_cast<StatementHandle*>(statementHandle);
  ConnectionHandle* cnct = stmt ? stmt->getConnection() : nullptr;
  if (!statementHandle) {
    logMessage(cnct, "SQLGetTypeInfo: Invalid statement handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }

  logMessage(cnct, "SQLGetTypeInfo: Entering", LOG_LEVEL_TRACE);
  if (isLogLevelEnabled(cnct, LOG_LEVEL_TRACE)) {
    std::stringstream logStream;
    logStream << "SQLGetTypeInfo parameters:"
              << "statementHandle = " << handleToString(statementHandle)
              << ", dataType = " << dataType;
    logMessage(cnct, logStream.str(), LOG_LEVEL_TRACE);
  }
  stmt->clearDiagnostics();
  stmt->ClearResultSet();
  stmt->AllocateSessionResultSet();
  stmt->curRow = -1;
  stmt->rowsReturned = 0;
  stmt->isQuery = true;
  stmt->isStream = false;
  stmt->lastGetDataRow = -1;
  stmt->getDataOffsets.clear();

  // Define the type information data
  std::vector<std::string> columnNames = {
      "TYPE_NAME",        "DATA_TYPE",          "COLUMN_SIZE",       "LITERAL_PREFIX",
      "LITERAL_SUFFIX",   "CREATE_PARAMS",      "NULLABLE",          "CASE_SENSITIVE",
      "SEARCHABLE",       "UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE",  "AUTO_UNIQUE_VALUE",
      "LOCAL_TYPE_NAME",  "MINIMUM_SCALE",      "MAXIMUM_SCALE",     "SQL_DATA_TYPE",
      "SQL_DATETIME_SUB", "NUM_PREC_RADIX",     "INTERVAL_PRECISION"};

  std::vector<std::string> columnTypes = {
      "TEXT",  "INT16", "INT32", "TEXT",  "TEXT",  "TEXT",  "INT16", "INT16", "INT16", "INT16",
      "INT16", "INT16", "TEXT",  "INT16", "INT16", "INT16", "INT16", "INT32", "INT16"};

  ODBCField null = ODBCField::null();
  std::vector<std::vector<ODBCField>> typeData = {
      {"BOOLEAN", -7, 1, null, null, null, 1, 0, 2, null, 1, null, "BOOLEAN", null, null, -7, null,
       null, null},
      {"INT64", -5, 19, null, null, null, 1, 0, 2, 0, 1, 0, "INT64", 0, 0, -5, null, 10, null},
      {"BLOB", -4, 0, null, null, null, 1, 0, 1, null, 0, null, "BLOB", null, null, -4, null, null,
       null},
      {"TEXT", -1, 0, null, null, null, 1, 1, 1, null, 0, null, "TEXT", null, null, -1, null, null,
       null},
      {"INT32", 4, 10, null, null, null, 1, 0, 2, 0, 1, 0, "INT32", 0, 0, 4, null, 10, null},
      {"FLOAT", 7, 24, null, null, null, 1, 0, 2, 0, 0, 0, "FLOAT", null, null, 7, null, 2, null},
      {"DOUBLE", 8, 53, null, null, null, 1, 0, 2, 0, 0, 0, "DOUBLE", null, null, 8, null, 2, null},
      {"STRING", 12, 0, null, null, "length", 1, 1, 1, null, 0, null, "STRING", null, null, 12,
       null, null, null},
      {"DATE", 91, 10, null, null, null, 1, 0, 2, null, 1, null, "DATE", 0, 0, 9, 1, null, null},
      {"TIMESTAMP", 93, 23, null, null, null, 1, 0, 2, 1, 1, 0, "TIMESTAMP", 3, 3, 9, 3, null,
       null}};

  // Unicode consumers such as MSDASQL select SQL_C_WCHAR from these SQL types.
  const size_t originalTypes = typeData.size();
  for (size_t i = 0; i < originalTypes; ++i) {
    SQLSMALLINT type = 0;
    typeData[i][1].toSShort(&type);
    if (type == SQL_VARCHAR || type == SQL_LONGVARCHAR) {
      auto wide = typeData[i];
      wide[1] = wide[15] = ODBCField(type == SQL_VARCHAR ? SQL_WVARCHAR : SQL_WLONGVARCHAR);
      typeData.push_back(std::move(wide));
    }
  }
  // Filter data if specific dataType is requested
  if (dataType != SQL_ALL_TYPES) {
    std::vector<std::vector<ODBCField>> filteredData;
    for (const auto& row : typeData) {
      if (row.size() > 1) {
        try {
          SQLSMALLINT rowDataType = 0;
          bool ret = row[1].toSShort(&rowDataType);
          if (ret && rowDataType == dataType) {
            filteredData.push_back(row);
          }
        } catch (const std::exception&) {
          // Skip rows that can't be parsed
          continue;
        }
      }
    }
    typeData = filteredData;
  }

  // Set up ODBCResultSet with the data
  stmt->resultSetPtr->columnNames = columnNames;
  stmt->resultSetPtr->columnTypes = columnTypes;
  stmt->resultSetPtr->data = typeData;
  stmt->resultSetPtr->numRows = typeData.size();
  stmt->resultSetPtr->numColumns = columnNames.size();
  stmt->resultSetPtr->isMetaData = true;

  logMessage(cnct,
             "SQLGetTypeInfo: Created ODBCResultSet with " + std::to_string(typeData.size()) +
                 " type records",
             LOG_LEVEL_DEBUG);
  stmt->resultSetPtr->outputTable();
  logMessage(cnct, "SQLGetTypeInfo: Exiting.\n", LOG_LEVEL_TRACE);
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT statementHandle, SQLSMALLINT* columnCount) {
  const auto stmt = static_cast<StatementHandle*>(statementHandle);
  ConnectionHandle* cnct = stmt ? stmt->getConnection() : nullptr;

  logMessage(cnct, "SQLNumResultCols: Entering", LOG_LEVEL_TRACE);

  // Prepare log message for the parameters
  if (isLogLevelEnabled(cnct, LOG_LEVEL_TRACE)) {
    std::stringstream logStream;
    logStream << "SQLNumResultCols parameters: "
              << "statementHandle = " << handleToString(statementHandle);
    logMessage(cnct, logStream.str(), LOG_LEVEL_TRACE);
  }

  // In SQLAllocHandle we're allocating a NULL_Handle, so I disabled this.
  if (!statementHandle) {
    logMessage(cnct, "SQLNumResultCols: Invalid statement handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }

  if (!columnCount) {
    logMessage(cnct, "SQLNumResultCols: columnCount pointer is null", LOG_LEVEL_ERROR);
    stmt->addDiagnostic("HY009", "Invalid use of null pointer");
    return SQL_ERROR;
  }

  if (stmt->resultSetPtr == nullptr) {
    logMessage(cnct, "SQLNumResultCols: Missing result set, returning 0", LOG_LEVEL_ERROR);
    *columnCount = 0;
    return SQL_SUCCESS;
  }

  PopulateImplementationRowDescriptor(stmt);
  *columnCount = stmt->resultSetPtr->getNumColumns();
  logMessage(cnct, "SQLNumResultCols: Returning column count: " + std::to_string(*columnCount),
             LOG_LEVEL_DEBUG);

  logMessage(cnct, "SQLNumResultCols: Exiting successfully", LOG_LEVEL_TRACE);
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLNumParams(SQLHSTMT statementHandle, SQLSMALLINT* parameterCountPtr) {
  if (!statementHandle)
    return SQL_INVALID_HANDLE;
  auto* stmt = static_cast<StatementHandle*>(statementHandle);
  stmt->clearDiagnostics();
  if (!parameterCountPtr) {
    stmt->addDiagnostic("HY009", "Invalid use of null pointer");
    return SQL_ERROR;
  }
  if (!stmt->prepared) {
    stmt->addDiagnostic("HY010", "Statement has not been prepared");
    return SQL_ERROR;
  }
  const size_t count = ParameterMarkerPositions(stmt->statementText).size();
  if (count > static_cast<size_t>(std::numeric_limits<SQLSMALLINT>::max())) {
    stmt->addDiagnostic("HY000", "Too many parameter markers");
    return SQL_ERROR;
  }
  *parameterCountPtr = static_cast<SQLSMALLINT>(count);
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLParamData(SQLHSTMT statementHandle, SQLPOINTER* value) {
  if (!statementHandle)
    return SQL_INVALID_HANDLE;
  auto* stmt = static_cast<StatementHandle*>(statementHandle);
  stmt->clearDiagnostics();
  if (!stmt->needsParameterData) {
    stmt->addDiagnostic("HY010", "Function sequence error");
    return SQL_ERROR;
  }
  const size_t markerCount = ParameterMarkerPositions(stmt->statementText).size();
  for (SQLULEN set = stmt->nextDataSet; set < stmt->paramSetSize; ++set) {
    const size_t firstParameter = set == stmt->nextDataSet ? stmt->nextDataParameter : 0;
    for (size_t index = firstParameter; index < markerCount; ++index) {
      ParameterBinding& binding = stmt->parameterBindings[index];
      if (IsDataAtExecution(ParameterIndicatorAt(stmt, binding, set))) {
        stmt->activeDataSet = set;
        stmt->activeDataParameter = index;
        stmt->nextDataSet = index + 1 < markerCount ? set : set + 1;
        stmt->nextDataParameter = index + 1 < markerCount ? index + 1 : 0;
        if (value)
          *value = ParameterValueAt(stmt, binding, set);
        return SQL_NEED_DATA;
      }
    }
  }
  stmt->needsParameterData = false;
  stmt->activeDataSet = std::numeric_limits<SQLULEN>::max();
  stmt->activeDataParameter = std::numeric_limits<size_t>::max();
  try {
    return ExecuteParameterSets(stmt);
  } catch (const std::exception& error) {
    stmt->addDiagnostic("HY000", error.what());
    return SQL_ERROR;
  }
}

SQLRETURN SQL_API SQLPrepare(SQLHSTMT statementHandle, SQLCHAR* statementText,
                             SQLINTEGER textLength) {
  if (!statementHandle) {
    logMessage(nullptr, "SQLPrepare: Invalid statement handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  auto stmt = static_cast<StatementHandle*>(statementHandle);
  stmt->clearDiagnostics();
  if (stmt->needsParameterData) {
    stmt->addDiagnostic("HY010", "Function sequence error");
    return SQL_ERROR;
  }
  if (!statementText) {
    stmt->addDiagnostic("HY009", "Invalid use of null pointer");
    return SQL_ERROR;
  }
  if (textLength < 0 && textLength != SQL_NTS) {
    stmt->addDiagnostic("HY090", "Invalid string or buffer length");
    return SQL_ERROR;
  }
  stmt->statementText = ConvertSQLCHARToString(statementText, textLength);
  stmt->prepared = true;
  stmt->ClearResultSet();
  stmt->curRow = -1;
  stmt->rowsReturned = 0;
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLPutData(SQLHSTMT statementHandle, SQLPOINTER data, SQLLEN strLen_or_Ind) {
  if (!statementHandle)
    return SQL_INVALID_HANDLE;
  auto* stmt = static_cast<StatementHandle*>(statementHandle);
  stmt->clearDiagnostics();
  if (!stmt->needsParameterData || stmt->activeDataSet == std::numeric_limits<SQLULEN>::max() ||
      stmt->activeDataParameter == std::numeric_limits<size_t>::max()) {
    stmt->addDiagnostic("HY010", "Function sequence error");
    return SQL_ERROR;
  }
  ParameterBinding& binding = stmt->parameterBindings[stmt->activeDataParameter];
  if (strLen_or_Ind == SQL_NULL_DATA) {
    binding.streamedNull[stmt->activeDataSet] = true;
    binding.streamedData[stmt->activeDataSet].clear();
    return SQL_SUCCESS;
  }
  if (strLen_or_Ind < 0 && strLen_or_Ind != SQL_NTS) {
    stmt->addDiagnostic("HY090", "Invalid string or buffer length");
    return SQL_ERROR;
  }
  if (!data) {
    stmt->addDiagnostic("HY009", "Invalid use of null pointer");
    return SQL_ERROR;
  }
  const char* bytes = static_cast<const char*>(data);
  const SQLSMALLINT valueType = binding.valueType == SQL_C_DEFAULT
                                    ? DefaultParameterCType(binding.parameterType)
                                    : binding.valueType;
  size_t length = static_cast<size_t>(strLen_or_Ind);
  if (valueType != SQL_C_CHAR && valueType != SQL_C_WCHAR && valueType != SQL_C_BINARY) {
    length = CTypeSize(valueType, binding.bufferLength);
  } else if (strLen_or_Ind == SQL_NTS) {
    if (valueType == SQL_C_BINARY) {
      stmt->addDiagnostic("HY090", "Invalid string or buffer length");
      return SQL_ERROR;
    }
    length = valueType == SQL_C_WCHAR
                 ? std::char_traits<SQLWCHAR>::length(static_cast<const SQLWCHAR*>(data)) *
                       sizeof(SQLWCHAR)
                 : std::strlen(bytes);
  }
  binding.streamedData[stmt->activeDataSet].append(bytes, length);
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLRowCount(SQLHSTMT statementHandle, SQLLEN* rowCount) {
  const auto stmt = static_cast<StatementHandle*>(statementHandle);
  ConnectionHandle* cnct = stmt ? stmt->getConnection() : nullptr;
  logMessage(cnct, "SQLRowCount: Entering", LOG_LEVEL_TRACE);

  // Prepare log message for the parameters
  if (isLogLevelEnabled(cnct, LOG_LEVEL_TRACE)) {
    std::stringstream logStream;
    logStream << "SQLRowCount parameters: "
              << "statementHandle = " << handleToString(statementHandle);
    logMessage(cnct, logStream.str(), LOG_LEVEL_TRACE);
  }

  if (!statementHandle) {
    logMessage(cnct, "SQLRowCount: Invalid statement handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }

  if (!rowCount) {
    stmt->addDiagnostic("HY009", "Invalid use of null pointer");
    return SQL_ERROR;
  }

  // IoTDB does not expose an affected-row count, and query row counts may be
  // incomplete while results are streamed.
  *rowCount = -1;
  logMessage(cnct, "SQLRowCount: Returning row count: " + std::to_string(*rowCount),
             LOG_LEVEL_DEBUG);

  logMessage(cnct, "SQLRowCount: Exiting successfully", LOG_LEVEL_TRACE);
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetConnectAttr(const SQLHDBC connectionHandle, const SQLINTEGER attribute,
                                    SQLPOINTER value, const SQLINTEGER stringLength) {
  ConnectionHandle* cnct = static_cast<ConnectionHandle*>(connectionHandle);

  logMessage(cnct, "SQLSetConnectAttr: Entering", LOG_LEVEL_TRACE);

  if (isLogLevelEnabled(cnct, LOG_LEVEL_TRACE)) {
    std::stringstream logStream;
    logStream << "SQLSetConnectAttr parameters: "
              << "connectionHandle = " << handleToString(connectionHandle)
              << ", attribute = " << attribute << ", value = " << valueToString(value)
              << ", stringLength = " << stringLength;
    logMessage(cnct, logStream.str(), LOG_LEVEL_TRACE);
  }

  if (cnct == nullptr) {
    logMessage("SQLSetConnectAttr: Invalid ConnectionHandle");
    return SQL_INVALID_HANDLE;
  }
  cnct->clearDiagnostics();

  // Handle supported attributes
  switch (attribute) {
  case SQL_ATTR_AUTOCOMMIT:
    logMessage(cnct, "SQLSetConnectAttr: Processing SQL_ATTR_AUTOCOMMIT", LOG_LEVEL_DEBUG);
    if (value == reinterpret_cast<SQLPOINTER>(SQL_AUTOCOMMIT_ON)) {
      cnct->autoCommit = true;
      logMessage(cnct, "SQLSetConnectAttr: Autocommit enabled", LOG_LEVEL_DEBUG);
    } else if (value == reinterpret_cast<SQLPOINTER>(SQL_AUTOCOMMIT_OFF)) {
      cnct->addDiagnostic("HYC00", "Transactions are not supported");
      return SQL_ERROR;
    } else {
      logMessage(cnct, "SQLSetConnectAttr: Invalid value for SQL_ATTR_AUTOCOMMIT", LOG_LEVEL_ERROR);
      cnct->addDiagnostic("HY024", "Invalid attribute value");
      return SQL_ERROR;
    }
    break;

  case SQL_ATTR_CONNECTION_TIMEOUT:
    logMessage(cnct, "SQLSetConnectAttr: Processing SQL_ATTR_CONNECTION_TIMEOUT", LOG_LEVEL_DEBUG);
    cnct->timeoutConnection = static_cast<SQLUINTEGER>(reinterpret_cast<uintptr_t>(value));
    break;

  case SQL_ATTR_LOGIN_TIMEOUT:
    logMessage(cnct, "SQLSetConnectAttr: Processing SQL_ATTR_LOGIN_TIMEOUT", LOG_LEVEL_DEBUG);
    {
      const SQLUINTEGER timeout = static_cast<SQLUINTEGER>(reinterpret_cast<uintptr_t>(value));
      cnct->timeoutLogin = timeout;
      break;
    }

  case SQL_ATTR_ACCESS_MODE:
    if (value != reinterpret_cast<SQLPOINTER>(SQL_MODE_READ_WRITE)) {
      cnct->addDiagnostic("HYC00", "Read-only connection mode is not supported");
      return SQL_ERROR;
    }
    break;

  case SQL_ATTR_TXN_ISOLATION:
    if (reinterpret_cast<uintptr_t>(value) != 0) {
      cnct->addDiagnostic("HYC00", "Transactions are not supported");
      return SQL_ERROR;
    }
    break;

  case SQL_ATTR_CURRENT_CATALOG: {
    logMessage(cnct, "SQLSetConnectAttr: Processing SQL_ATTR_CURRENT_CATALOG", LOG_LEVEL_DEBUG);
    if (!value) {
      logMessage(cnct, "SQLSetConnectAttr: NULL value for SQL_ATTR_CURRENT_CATALOG",
                 LOG_LEVEL_ERROR);
      cnct->addDiagnostic("HY024", "Invalid attribute value");
      return SQL_ERROR;
    }
    if (stringLength < 0 && stringLength != SQL_NTS) {
      cnct->addDiagnostic("HY090", "Invalid string or buffer length");
      return SQL_ERROR;
    }
    const char* databaseValue = static_cast<const char*>(value);
    std::string databaseName(databaseValue, stringLength == SQL_NTS
                                                ? std::strlen(databaseValue)
                                                : static_cast<size_t>(stringLength));
    if (databaseName.length() > 64) {
      logMessage(cnct, "SQLSetConnectAttr: Catalog name too long (max 64 characters)",
                 LOG_LEVEL_ERROR);
      cnct->addDiagnostic("HY090", "String data, right truncation");
      return SQL_ERROR;
    }
    if (databaseName.empty() || databaseName.find('\0') != std::string::npos) {
      cnct->addDiagnostic("HY024", "Invalid catalog name");
      return SQL_ERROR;
    }
    std::string quotedDatabase = "\"";
    for (char character : databaseName) {
      if (character == '"')
        quotedDatabase += '"';
      quotedDatabase += character;
    }
    quotedDatabase += '"';
    if (cnct->isTableModel) {
      logMessage(cnct, "SQLSetConnectAttr: Setting current database: USE " + cnct->database,
                 LOG_LEVEL_DEBUG);
      SQLHANDLE statementHandle = nullptr;
      SQLRETURN sqlreturn = SQLAllocHandle(SQL_HANDLE_STMT, connectionHandle, &statementHandle);
      if (!SQL_SUCCEEDED(sqlreturn))
        return sqlreturn;
      StatementHandle* stmt = static_cast<StatementHandle*>(statementHandle);
      sqlreturn = IoTDB_ExecDirect(stmt, "USE " + quotedDatabase);
      if (sqlreturn != SQL_SUCCESS && sqlreturn != SQL_SUCCESS_WITH_INFO) {
        logMessage(cnct, "SQLSetConnectAttr: Setting current catalog failed!", LOG_LEVEL_ERROR);
        std::string state, message;
        int native = 0;
        if (stmt->getDiagnostic(1, state, message, native))
          cnct->addDiagnostic(state, message, native);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        return sqlreturn;
      }
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    }
    cnct->database = databaseName;
    logMessage(cnct, "SQLSetConnectAttr: Set current catalog (= database) to " + cnct->database,
               LOG_LEVEL_DEBUG);
    break;
  }

  default:
    logMessage(cnct,
               "SQLSetConnectAttr: Unsupported attribute " + std::to_string(attribute) +
                   ", returning SQL_ERROR",
               LOG_LEVEL_ERROR);
    cnct->addDiagnostic("HY092", "Invalid attribute/option identifier");
    return SQL_ERROR; // Return error for unsupported attributes
  }

  logMessage(cnct, "SQLSetConnectAttr: Exiting successfully\n", LOG_LEVEL_WARN);
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetConnectOption(SQLHDBC connectionHandle, SQLUSMALLINT option,
                                      SQLULEN value) {
  if (!connectionHandle) {
    logMessage(nullptr, "SQLSetConnectOption: Invalid connection handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  switch (option) {
  case SQL_ACCESS_MODE:
    return SQLSetConnectAttr(connectionHandle, SQL_ATTR_ACCESS_MODE,
                             reinterpret_cast<SQLPOINTER>(value), 0);
  case SQL_AUTOCOMMIT:
    return SQLSetConnectAttr(connectionHandle, SQL_ATTR_AUTOCOMMIT,
                             reinterpret_cast<SQLPOINTER>(value), 0);
  case SQL_LOGIN_TIMEOUT:
    return SQLSetConnectAttr(connectionHandle, SQL_ATTR_LOGIN_TIMEOUT,
                             reinterpret_cast<SQLPOINTER>(value), 0);
  case SQL_TXN_ISOLATION:
    return SQLSetConnectAttr(connectionHandle, SQL_ATTR_TXN_ISOLATION,
                             reinterpret_cast<SQLPOINTER>(value), 0);
  case SQL_CURRENT_QUALIFIER:
    return SQLSetConnectAttr(connectionHandle, SQL_ATTR_CURRENT_CATALOG,
                             reinterpret_cast<SQLPOINTER>(value), SQL_NTS);
  default:
    auto* cnct = static_cast<ConnectionHandle*>(connectionHandle);
    cnct->clearDiagnostics();
    cnct->addDiagnostic("HY092", "Invalid attribute/option identifier");
    return SQL_ERROR;
  }
}

SQLRETURN SQL_API SQLSetCursorName(SQLHSTMT statementHandle, SQLCHAR* cursorName,
                                   SQLSMALLINT nameLength) {
  if (!statementHandle) {
    logMessage(nullptr, "SQLSetCursorName: Invalid statement handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  auto stmt = static_cast<StatementHandle*>(statementHandle);
  stmt->clearDiagnostics();
  if (!cursorName) {
    stmt->addDiagnostic("HY009", "Invalid use of null pointer");
    return SQL_ERROR;
  }
  if (nameLength < 0 && nameLength != SQL_NTS) {
    stmt->addDiagnostic("HY090", "Invalid string or buffer length");
    return SQL_ERROR;
  }
  stmt->cursorName.assign(reinterpret_cast<const char*>(cursorName),
                          nameLength == SQL_NTS
                              ? std::strlen(reinterpret_cast<const char*>(cursorName))
                              : static_cast<size_t>(nameLength));
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetDescField(SQLHDESC descriptorHandle, SQLSMALLINT recNumber,
                                  SQLSMALLINT fieldIdentifier, SQLPOINTER value,
                                  SQLINTEGER bufferLength) {
  if (!descriptorHandle)
    return SQL_INVALID_HANDLE;
  auto* descriptor = static_cast<DescriptorHandle*>(descriptorHandle);
  if (descriptor->getHandleType() != SQL_HANDLE_DESC)
    return SQL_INVALID_HANDLE;
  descriptor->clearDiagnostics();
  if (descriptor->role == DescriptorRole::IMPLEMENTATION_ROW &&
      fieldIdentifier != SQL_DESC_ARRAY_STATUS_PTR &&
      fieldIdentifier != SQL_DESC_ROWS_PROCESSED_PTR) {
    descriptor->addDiagnostic("HY016", "Cannot modify an implementation row descriptor");
    return SQL_ERROR;
  }
  const SQLULEN unsignedValue = reinterpret_cast<SQLULEN>(value);
  const SQLLEN signedValue = reinterpret_cast<SQLLEN>(value);
  switch (fieldIdentifier) {
  case SQL_DESC_ARRAY_SIZE:
    if (unsignedValue == 0) {
      descriptor->addDiagnostic("HY092", "Invalid descriptor array size");
      return SQL_ERROR;
    }
    descriptor->arraySize = unsignedValue;
    return SQL_SUCCESS;
  case SQL_DESC_ARRAY_STATUS_PTR:
    descriptor->arrayStatusPtr = static_cast<SQLUSMALLINT*>(value);
    return SQL_SUCCESS;
  case SQL_DESC_BIND_OFFSET_PTR:
    descriptor->bindOffsetPtr = static_cast<SQLLEN*>(value);
    return SQL_SUCCESS;
  case SQL_DESC_BIND_TYPE:
    descriptor->bindType = unsignedValue;
    return SQL_SUCCESS;
  case SQL_DESC_COUNT:
    if (signedValue < 0 || signedValue > std::numeric_limits<SQLSMALLINT>::max()) {
      descriptor->addDiagnostic("HY092", "Invalid descriptor count");
      return SQL_ERROR;
    }
    descriptor->count = static_cast<SQLSMALLINT>(signedValue);
    descriptor->records.resize(static_cast<size_t>(descriptor->count));
    return SQL_SUCCESS;
  case SQL_DESC_ROWS_PROCESSED_PTR:
    descriptor->rowsProcessedPtr = static_cast<SQLULEN*>(value);
    return SQL_SUCCESS;
  case SQL_DESC_ALLOC_TYPE:
    descriptor->addDiagnostic("HY091", "Descriptor allocation type is read-only");
    return SQL_ERROR;
  default:
    break;
  }

  if (recNumber < 1) {
    descriptor->addDiagnostic("07009", "Invalid descriptor index");
    return SQL_ERROR;
  }
  DescriptorRecord& record = descriptor->record(recNumber);
  descriptor->count = std::max(descriptor->count, recNumber);
  switch (fieldIdentifier) {
  case SQL_DESC_CONCISE_TYPE:
    record.setConciseType(static_cast<SQLSMALLINT>(signedValue));
    record.dataPtr = nullptr;
    break;
  case SQL_DESC_TYPE:
    record.type = static_cast<SQLSMALLINT>(signedValue);
    record.updateConciseType();
    record.dataPtr = nullptr;
    break;
  case SQL_DESC_DATETIME_INTERVAL_CODE:
    record.datetimeIntervalCode = static_cast<SQLSMALLINT>(signedValue);
    record.updateConciseType();
    record.dataPtr = nullptr;
    break;
  case SQL_DESC_OCTET_LENGTH:
    record.octetLength = signedValue;
    record.dataPtr = nullptr;
    break;
  case SQL_DESC_LENGTH:
    record.length = unsignedValue;
    record.dataPtr = nullptr;
    break;
  case SQL_DESC_PRECISION:
    record.precision = static_cast<SQLSMALLINT>(signedValue);
    record.dataPtr = nullptr;
    break;
  case SQL_DESC_SCALE:
    record.scale = static_cast<SQLSMALLINT>(signedValue);
    record.dataPtr = nullptr;
    break;
  case SQL_DESC_DATA_PTR:
    record.dataPtr = value;
    break;
  case SQL_DESC_INDICATOR_PTR:
    record.indicatorPtr = static_cast<SQLLEN*>(value);
    break;
  case SQL_DESC_OCTET_LENGTH_PTR:
    record.octetLengthPtr = static_cast<SQLLEN*>(value);
    break;
  case SQL_DESC_PARAMETER_TYPE:
    record.parameterType = static_cast<SQLSMALLINT>(signedValue);
    break;
  case SQL_DESC_NAME: {
    if (!value) {
      record.name.clear();
      break;
    }
    if (bufferLength < 0 && bufferLength != SQL_NTS) {
      descriptor->addDiagnostic("HY090", "Invalid string or buffer length");
      return SQL_ERROR;
    }
    const char* text = static_cast<const char*>(value);
    record.name.assign(text, bufferLength == SQL_NTS ? std::strlen(text)
                                                     : static_cast<size_t>(bufferLength));
    record.unnamed = record.name.empty() ? SQL_UNNAMED : SQL_NAMED;
    break;
  }
  default:
    descriptor->addDiagnostic("HY091", "Invalid descriptor field identifier");
    return SQL_ERROR;
  }
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetDescRec(SQLHDESC descriptorHandle, SQLSMALLINT recNumber, SQLSMALLINT type,
                                SQLSMALLINT subType, SQLLEN length, SQLSMALLINT precision,
                                SQLSMALLINT scale, SQLPOINTER data, SQLLEN* stringLength,
                                SQLLEN* indicator) {
  if (!descriptorHandle)
    return SQL_INVALID_HANDLE;
  auto* descriptor = static_cast<DescriptorHandle*>(descriptorHandle);
  if (descriptor->getHandleType() != SQL_HANDLE_DESC)
    return SQL_INVALID_HANDLE;
  descriptor->clearDiagnostics();
  if (descriptor->role == DescriptorRole::IMPLEMENTATION_ROW) {
    descriptor->addDiagnostic("HY016", "Cannot modify an implementation row descriptor");
    return SQL_ERROR;
  }
  if (recNumber < 1) {
    descriptor->addDiagnostic("07009", "Invalid descriptor index");
    return SQL_ERROR;
  }
  DescriptorRecord& record = descriptor->record(recNumber);
  descriptor->count = std::max(descriptor->count, recNumber);
  record.type = type;
  record.datetimeIntervalCode = subType;
  record.updateConciseType();
  record.octetLength = length;
  record.length = static_cast<SQLULEN>(std::max<SQLLEN>(0, length));
  record.precision = precision;
  record.scale = scale;
  record.dataPtr = data;
  record.octetLengthPtr = stringLength;
  record.indicatorPtr = indicator;
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetEnvAttr(const SQLHENV environmentHandle, const SQLINTEGER attribute,
                                SQLPOINTER value, const SQLINTEGER stringLength) {
  EnvironmentHandle* env = static_cast<EnvironmentHandle*>(environmentHandle);
  logMessage(nullptr, "SQLSetEnvAttr: Entering", LOG_LEVEL_TRACE);

  if (!env)
    return SQL_INVALID_HANDLE;
  env->clearDiagnostics();

  if (isLogLevelEnabled(nullptr, LOG_LEVEL_TRACE)) {
    std::stringstream logStream;
    logStream << "SQLSetEnvAttr parameters: "
              << "environmentHandle = " << handleToString(environmentHandle)
              << ", attribute = " << attribute << ", valuePtr address = " << value
              << ", stringLength = " << stringLength;
    logMessage(nullptr, logStream.str(), LOG_LEVEL_TRACE);
  }

  switch (attribute) {

  // In this case the client is passing int it's version of ODBC API.
  // We use that to double-check if this is supported by this driver.
  // We save it in the env, as we might need to respond differently
  // based on the clients API version.
  case SQL_ATTR_ODBC_VERSION:
    logMessage(nullptr, "SQLSetEnvAttr: Processing SQL_ATTR_ODBC_VERSION", LOG_LEVEL_DEBUG);
    if (value) {
      const SQLINTEGER odbcVersion = static_cast<SQLINTEGER>(reinterpret_cast<uintptr_t>(value));
      // Check for supported ODBC versions
      // We currently support 3.0 and 3.8
      if (odbcVersion == SQL_OV_ODBC3) {
        // Store the version in your environment structure
        const auto env = static_cast<EnvironmentHandle*>(environmentHandle);
        env->odbcVersion = odbcVersion;
        logMessage(nullptr, "SQLSetEnvAttr: ODBC version set to 3.0", LOG_LEVEL_INFO);
        return SQL_SUCCESS;
      }
// iODBC only seems to support ODBC 3.0
#ifdef SQL_OV_ODBC3_80
      if (odbcVersion == SQL_OV_ODBC3_80) {
        // Store the version in your environment structure
        const auto env = static_cast<EnvironmentHandle*>(environmentHandle);
        env->odbcVersion = odbcVersion;
        logMessage(nullptr, "SQLSetEnvAttr: ODBC version set to 3.8", LOG_LEVEL_INFO);
        return SQL_SUCCESS;
      }
#endif
      // MS Excel only support ODBC 2.0
      if (odbcVersion == SQL_OV_ODBC2) {
        const auto env = static_cast<EnvironmentHandle*>(environmentHandle);
        env->odbcVersion = odbcVersion;
        logMessage(nullptr, "SQLSetEnvAttr: ODBC version set to 2.0", LOG_LEVEL_INFO);
        return SQL_SUCCESS;
      }

      logMessage(nullptr, "SQLSetEnvAttr: Empty value for ODBC version", LOG_LEVEL_ERROR);
      env->addDiagnostic("HY009", "Invalid use of null pointer");
      return SQL_ERROR;
    }
    logMessage("Empty Value\n");
    return SQL_ERROR;

  case SQL_ATTR_CONNECTION_POOLING:
    logMessage(nullptr, "SQLSetEnvAttr: Processing SQL_ATTR_CONNECTION_POOLING", LOG_LEVEL_DEBUG);
    logMessage(nullptr, "SQLSetEnvAttr: Connection pooling is not supported", LOG_LEVEL_INFO);
    return SQL_SUCCESS;

  case SQL_ATTR_OUTPUT_NTS:
    if (reinterpret_cast<uintptr_t>(value) != SQL_TRUE) {
      env->addDiagnostic("HYC00", "Null-terminated output is required");
      return SQL_ERROR;
    }
    return SQL_SUCCESS;

  default:
    logMessage(nullptr, "SQLSetEnvAttr: Unsupported attribute: " + std::to_string(attribute),
               LOG_LEVEL_WARN);
    env->addDiagnostic("HY092", "Invalid attribute/option identifier");
    return SQL_ERROR;
  }
}

SQLRETURN SQL_API SQLSetParam(SQLHSTMT statementHandle, SQLUSMALLINT parameterNumber,
                              SQLSMALLINT valueType, SQLSMALLINT parameterType,
                              SQLULEN lengthPrecision, SQLSMALLINT parameterScale,
                              SQLPOINTER parameterValue, SQLLEN* strLen_or_Ind) {
  return SQLBindParameter(statementHandle, parameterNumber, SQL_PARAM_INPUT, valueType,
                          parameterType, lengthPrecision, parameterScale, parameterValue,
                          static_cast<SQLLEN>(lengthPrecision), strLen_or_Ind);
}

SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT statementHandle, SQLINTEGER attribute, SQLPOINTER value,
                                 SQLINTEGER stringLength) {
  const auto stmt = static_cast<StatementHandle*>(statementHandle);
  ConnectionHandle* cnct = stmt ? stmt->getConnection() : nullptr;

  logMessage(cnct, "SQLSetStmtAttr: Entering", LOG_LEVEL_TRACE);

  // Only build parameter log message if TRACE level is enabled
  if (isLogLevelEnabled(cnct, LOG_LEVEL_TRACE)) {
    std::stringstream logStream;
    logStream << "SQLSetStmtAttr parameters: "
              << "statementHandle = " << handleToString(statementHandle)
              << ", attribute = " << attribute << ", value = " << valueToString(value)
              << ", stringLength = " << stringLength;
    logMessage(cnct, logStream.str(), LOG_LEVEL_TRACE);
  }

  // In SQLSetStmtAttr we're allocating a NULL_Handle, so I disabled this.
  if (!statementHandle) {
    logMessage(cnct, "SQLSetStmtAttr: Invalid statement handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  stmt->clearDiagnostics();
  if (stmt->needsParameterData) {
    stmt->addDiagnostic("HY010", "Function sequence error");
    return SQL_ERROR;
  }

  switch (attribute) {
  case SQL_QUERY_TIMEOUT:
    if (reinterpret_cast<SQLULEN>(value) != 0) {
      stmt->addDiagnostic("HYC00", "Query timeout is not supported");
      return SQL_ERROR;
    }
    break;

  case SQL_ATTR_APP_ROW_DESC:
    if (!value) {
      stmt->appRowDesc = stmt->implicitAppRowDesc;
      break;
    }
    if (static_cast<ODBCHandle*>(value)->getHandleType() != SQL_HANDLE_DESC ||
        static_cast<DescriptorHandle*>(value)->connection != cnct) {
      stmt->addDiagnostic("HY024", "Invalid attribute value");
      return SQL_ERROR;
    }
    if (static_cast<DescriptorHandle*>(value)->implicit && value != stmt->implicitAppRowDesc) {
      stmt->addDiagnostic("HY017", "Invalid use of an automatically allocated descriptor");
      return SQL_ERROR;
    }
    stmt->appRowDesc = value;
    break;

  case SQL_ATTR_APP_PARAM_DESC:
    if (!value) {
      stmt->appParamDesc = stmt->implicitAppParamDesc;
      break;
    }
    if (static_cast<ODBCHandle*>(value)->getHandleType() != SQL_HANDLE_DESC ||
        static_cast<DescriptorHandle*>(value)->connection != cnct) {
      stmt->addDiagnostic("HY024", "Invalid attribute value");
      return SQL_ERROR;
    }
    if (static_cast<DescriptorHandle*>(value)->implicit && value != stmt->implicitAppParamDesc) {
      stmt->addDiagnostic("HY017", "Invalid use of an automatically allocated descriptor");
      return SQL_ERROR;
    }
    stmt->appParamDesc = value;
    break;

  case SQL_ATTR_ROWS_FETCHED_PTR: {
    logMessage(cnct, "SQLSetStmtAttr: Processing SQL_ATTR_ROWS_FETCHED_PTR", LOG_LEVEL_DEBUG);
    if (value == nullptr) {
      // Power BI passes value==nullptr for unknown reasons.
      // We can't modify Power BI's code, so we ignore this error condition.
      logMessage(cnct, "SQLSetStmtAttr: value is nullptr (Power BI compatibility)",
                 LOG_LEVEL_DEBUG);
      break;
    }
    if (stringLength != sizeof(SQLULEN)) {
      logMessage(cnct, "SQLSetStmtAttr: Warning - stringLength != sizeof(SQLULEN)", LOG_LEVEL_WARN);
    }
    // Store the pointer to the rows fetched counter
    stmt->rowsFetchedPtr = static_cast<SQLULEN*>(value);
    stmt->implicitImpRowDesc->rowsProcessedPtr = stmt->rowsFetchedPtr;
    logMessage(cnct, "SQLSetStmtAttr: Rows fetched pointer set", LOG_LEVEL_DEBUG);
    break;
  }

  case SQL_ATTR_ROW_STATUS_PTR:
    stmt->rowStatusPtr = static_cast<SQLUSMALLINT*>(value);
    stmt->implicitImpRowDesc->arrayStatusPtr = static_cast<SQLUSMALLINT*>(value);
    break;

  case SQL_ATTR_CURSOR_TYPE:
    logMessage(cnct, "SQLSetStmtAttr: Processing SQL_ATTR_CURSOR_TYPE", LOG_LEVEL_DEBUG);
    if (value != reinterpret_cast<SQLPOINTER>(SQL_CURSOR_FORWARD_ONLY)) {
      stmt->addDiagnostic("HYC00", "Only forward-only cursors are supported");
      return SQL_ERROR;
    }
    stmt->cursorType = static_cast<SQLINTEGER>(reinterpret_cast<intptr_t>(value));
    logMessage(cnct, "SQLSetStmtAttr: Cursor type set successfully", LOG_LEVEL_DEBUG);
    break;

  case SQL_ATTR_CONCURRENCY:
    logMessage(cnct, "SQLSetStmtAttr: Processing SQL_ATTR_CONCURRENCY", LOG_LEVEL_DEBUG);
    if (value != reinterpret_cast<SQLPOINTER>(SQL_CONCUR_READ_ONLY)) {
      stmt->addDiagnostic("HYC00", "Only read-only concurrency is supported");
      return SQL_ERROR;
    }
    stmt->concurrency = static_cast<SQLINTEGER>(reinterpret_cast<intptr_t>(value));
    logMessage(cnct, "SQLSetStmtAttr: Concurrency type set successfully", LOG_LEVEL_DEBUG);
    break;

  case SQL_ATTR_CURSOR_SCROLLABLE:
    logMessage(cnct, "SQLSetStmtAttr: Processing SQL_ATTR_CURSOR_SCROLLABLE", LOG_LEVEL_DEBUG);
    if (value != reinterpret_cast<SQLPOINTER>(SQL_NONSCROLLABLE)) {
      stmt->addDiagnostic("HYC00", "Scrollable cursors are not supported");
      return SQL_ERROR;
    }
    stmt->scrollable = static_cast<SQLINTEGER>(reinterpret_cast<intptr_t>(value));
    logMessage(cnct, "SQLSetStmtAttr: Cursor scrollable setting applied", LOG_LEVEL_DEBUG);
    break;

  case SQL_ATTR_CURSOR_SENSITIVITY:
    logMessage(cnct, "SQLSetStmtAttr: Processing SQL_ATTR_CURSOR_SENSITIVITY", LOG_LEVEL_DEBUG);
    if (value != reinterpret_cast<SQLPOINTER>(SQL_UNSPECIFIED)) {
      stmt->addDiagnostic("HYC00", "Cursor sensitivity is not supported");
      return SQL_ERROR;
    }
    stmt->sensitivity = static_cast<SQLINTEGER>(reinterpret_cast<intptr_t>(value));
    logMessage(cnct, "SQLSetStmtAttr: Cursor sensitivity set", LOG_LEVEL_DEBUG);
    break;

  case SQL_ATTR_USE_BOOKMARKS:
    logMessage(cnct, "SQLSetStmtAttr: Processing SQL_ATTR_USE_BOOKMARKS", LOG_LEVEL_DEBUG);
    if (value != reinterpret_cast<SQLPOINTER>(SQL_UB_OFF)) {
      stmt->addDiagnostic("HYC00", "Bookmarks are not supported");
      return SQL_ERROR;
    }
    stmt->useBookmarks = static_cast<SQLINTEGER>(reinterpret_cast<intptr_t>(value));
    logMessage(cnct, "SQLSetStmtAttr: Bookmark usage setting applied", LOG_LEVEL_DEBUG);
    break;

  case SQL_ATTR_FETCH_BOOKMARK_PTR:
    if (value) {
      stmt->addDiagnostic("HYC00", "Bookmarks are not supported");
      return SQL_ERROR;
    }
    stmt->bookmarkPtr = nullptr;
    break;

  case SQL_ATTR_ROW_ARRAY_SIZE:
    logMessage(cnct, "SQLSetStmtAttr: Processing SQL_ATTR_ROW_ARRAY_SIZE", LOG_LEVEL_DEBUG);
    if (reinterpret_cast<SQLULEN>(value) == 0) {
      stmt->addDiagnostic("HY024", "Row array size must be greater than zero");
      return SQL_ERROR;
    }
    stmt->rowArraySize = reinterpret_cast<SQLULEN>(value);
    static_cast<DescriptorHandle*>(stmt->appRowDesc)->arraySize = stmt->rowArraySize;
    logMessage(cnct, "SQLSetStmtAttr: Row array size set", LOG_LEVEL_DEBUG);
    break;

  case SQL_ATTR_ROW_BIND_TYPE:
    logMessage(cnct, "SQLSetStmtAttr: Processing SQL_ATTR_ROW_BIND_TYPE", LOG_LEVEL_DEBUG);
    stmt->rowBindType = reinterpret_cast<SQLULEN>(value);
    static_cast<DescriptorHandle*>(stmt->appRowDesc)->bindType = stmt->rowBindType;
    logMessage(cnct, "SQLSetStmtAttr: Row bind type set", LOG_LEVEL_DEBUG);
    break;

  case SQL_ATTR_PARAMSET_SIZE:
    logMessage(cnct, "SQLSetStmtAttr: Processing SQL_ATTR_PARAMSET_SIZE", LOG_LEVEL_DEBUG);
    if (reinterpret_cast<SQLULEN>(value) == 0) {
      stmt->addDiagnostic("HY024", "Parameter set size must be greater than zero");
      return SQL_ERROR;
    }
    stmt->paramSetSize = reinterpret_cast<SQLULEN>(value);
    static_cast<DescriptorHandle*>(stmt->appParamDesc)->arraySize = stmt->paramSetSize;
    logMessage(cnct, "SQLSetStmtAttr: Parameter set size set", LOG_LEVEL_DEBUG);
    break;

  case SQL_ATTR_PARAM_BIND_TYPE:
    stmt->paramBindType = reinterpret_cast<SQLULEN>(value);
    static_cast<DescriptorHandle*>(stmt->appParamDesc)->bindType = stmt->paramBindType;
    break;

  case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
    stmt->paramBindOffsetPtr = static_cast<SQLULEN*>(value);
    static_cast<DescriptorHandle*>(stmt->appParamDesc)->bindOffsetPtr =
        reinterpret_cast<SQLLEN*>(value);
    break;

  case SQL_ATTR_PARAMS_PROCESSED_PTR:
    logMessage(cnct, "SQLSetStmtAttr: Processing SQL_ATTR_PARAMS_PROCESSED_PTR", LOG_LEVEL_DEBUG);
    // Store processed parameter pointer
    stmt->paramsProcessedPtr = static_cast<SQLULEN*>(value);
    stmt->implicitImpParamDesc->rowsProcessedPtr = stmt->paramsProcessedPtr;
    logMessage(cnct, "SQLSetStmtAttr: Params processed pointer set", LOG_LEVEL_DEBUG);
    break;

  case SQL_ATTR_PARAM_STATUS_PTR:
    logMessage(cnct, "SQLSetStmtAttr: Processing SQL_ATTR_PARAM_STATUS_PTR", LOG_LEVEL_DEBUG);
    // Store parameter status pointer
    stmt->paramStatusPtr = static_cast<SQLUSMALLINT*>(value);
    stmt->implicitImpParamDesc->arrayStatusPtr = static_cast<SQLUSMALLINT*>(value);
    logMessage(cnct, "SQLSetStmtAttr: Parameter status pointer set", LOG_LEVEL_DEBUG);
    break;

  case SQL_ATTR_MAX_ROWS:
    logMessage(cnct, "SQLSetStmtAttr: Processing SQL_ATTR_MAX_ROWS", LOG_LEVEL_DEBUG);
    // Set maximum return row count
    stmt->maxRows = (SQLULEN)value;
    logMessage(cnct, "SQLSetStmtAttr: Max rows set", LOG_LEVEL_DEBUG);
    break;

  case SQL_ATTR_NOSCAN:
    logMessage(cnct, "SQLSetStmtAttr: Processing SQL_ATTR_NOSCAN", LOG_LEVEL_DEBUG);
    // Set whether to scan
    if (value != reinterpret_cast<SQLPOINTER>(SQL_NOSCAN_OFF) &&
        value != reinterpret_cast<SQLPOINTER>(SQL_NOSCAN_ON)) {
      logMessage(cnct, "SQLSetStmtAttr: Invalid noscan option specified", LOG_LEVEL_ERROR);
      stmt->addDiagnostic("HY024", "Invalid noscan option");
      return SQL_ERROR;
    }
    stmt->noScan = static_cast<SQLINTEGER>(reinterpret_cast<intptr_t>(value));
    logMessage(cnct, "SQLSetStmtAttr: NoScan option set", LOG_LEVEL_DEBUG);
    break;

  case SQL_ATTR_SIMULATE_CURSOR:
    stmt->addDiagnostic("HYC00", "Cursor simulation is not supported");
    return SQL_ERROR;

  case SQL_ATTR_RETRIEVE_DATA:
    logMessage(cnct, "SQLSetStmtAttr: Processing SQL_ATTR_RETRIEVE_DATA", LOG_LEVEL_DEBUG);
    if (value != reinterpret_cast<SQLPOINTER>(SQL_RD_ON)) {
      stmt->addDiagnostic("HYC00", "Disabling data retrieval is not supported");
      return SQL_ERROR;
    }
    stmt->retrieveData = static_cast<SQLINTEGER>(reinterpret_cast<intptr_t>(value));
    logMessage(cnct, "SQLSetStmtAttr: Retrieve data option set", LOG_LEVEL_DEBUG);
    break;

  case SQL_ATTR_METADATA_ID:
    logMessage(cnct, "SQLSetStmtAttr: Processing SQL_ATTR_METADATA_ID", LOG_LEVEL_DEBUG);
    if (value != reinterpret_cast<SQLPOINTER>(SQL_FALSE)) {
      stmt->addDiagnostic("HYC00", "Identifier-based catalog arguments are not supported");
      return SQL_ERROR;
    }
    stmt->metadataId = static_cast<SQLINTEGER>(reinterpret_cast<intptr_t>(value));
    logMessage(cnct, "SQLSetStmtAttr: Metadata ID mode set", LOG_LEVEL_DEBUG);
    break;

  case SQL_ATTR_ASYNC_STMT_EVENT:
    stmt->addDiagnostic("HYC00", "Asynchronous statements are not supported");
    return SQL_ERROR;

  case SQL_ATTR_ENABLE_AUTO_IPD:
    logMessage(cnct, "SQLSetStmtAttr: Processing SQL_ATTR_ENABLE_AUTO_IPD", LOG_LEVEL_DEBUG);
    if (value != reinterpret_cast<SQLPOINTER>(SQL_FALSE)) {
      stmt->addDiagnostic("HYC00", "Automatic parameter descriptors are not supported");
      return SQL_ERROR;
    }
    stmt->enableAutoIPD = static_cast<SQLINTEGER>(reinterpret_cast<intptr_t>(value));
    logMessage(cnct, "SQLSetStmtAttr: Auto IPD setting applied", LOG_LEVEL_DEBUG);
    break;

  // Note: The following attributes are not fully implemented as their exact behavior
  // may vary by database system or require additional context
  case SQL_ATTR_IMP_ROW_DESC:
  case SQL_ATTR_IMP_PARAM_DESC:
    stmt->addDiagnostic("HY092", "Implementation descriptor attributes are read-only");
    return SQL_ERROR;

  case SQL_ATTR_ROW_BIND_OFFSET_PTR:
    stmt->rowBindOffsetPtr = static_cast<SQLULEN*>(value);
    static_cast<DescriptorHandle*>(stmt->appRowDesc)->bindOffsetPtr =
        reinterpret_cast<SQLLEN*>(value);
    break;

  case SQL_ATTR_MAX_LENGTH:
    logMessage(cnct, "SQLSetStmtAttr: Processing SQL_ATTR_MAX_LENGTH", LOG_LEVEL_DEBUG);
    // Set maximum length for data retrieval
    stmt->maxLength = (SQLULEN)value;
    logMessage(cnct, "SQLSetStmtAttr: Max length set", LOG_LEVEL_DEBUG);
    break;

  default:
    logMessage(cnct, "SQLSetStmtAttr: Unsupported attribute: " + std::to_string(attribute),
               LOG_LEVEL_WARN);
    stmt->addDiagnostic("HY092", "Invalid attribute/option identifier");
    return SQL_ERROR;
  }

  logMessage(cnct, "SQLSetStmtAttr: Exiting successfully", LOG_LEVEL_TRACE);
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetStmtOption(SQLHSTMT statementHandle, SQLUSMALLINT option, SQLULEN value) {
  if (!statementHandle) {
    logMessage(nullptr, "SQLSetStmtOption: Invalid statement handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  SQLINTEGER attribute = 0;
  switch (option) {
  case SQL_QUERY_TIMEOUT:
    attribute = SQL_ATTR_QUERY_TIMEOUT;
    break;
  case SQL_MAX_ROWS:
    attribute = SQL_ATTR_MAX_ROWS;
    break;
  case SQL_NOSCAN:
    attribute = SQL_ATTR_NOSCAN;
    break;
  case SQL_MAX_LENGTH:
    attribute = SQL_ATTR_MAX_LENGTH;
    break;
  case SQL_BIND_TYPE:
    attribute = SQL_ATTR_ROW_BIND_TYPE;
    break;
  case SQL_CURSOR_TYPE:
    attribute = SQL_ATTR_CURSOR_TYPE;
    break;
  case SQL_CONCURRENCY:
    attribute = SQL_ATTR_CONCURRENCY;
    break;
  case SQL_ROWSET_SIZE:
    attribute = SQL_ATTR_ROW_ARRAY_SIZE;
    break;
  case SQL_RETRIEVE_DATA:
    attribute = SQL_ATTR_RETRIEVE_DATA;
    break;
  case SQL_USE_BOOKMARKS:
    attribute = SQL_ATTR_USE_BOOKMARKS;
    break;
  default:
    auto* stmt = static_cast<StatementHandle*>(statementHandle);
    stmt->clearDiagnostics();
    stmt->addDiagnostic("HY092", "Invalid attribute/option identifier");
    return SQL_ERROR;
  }
  return SQLSetStmtAttr(statementHandle, attribute, reinterpret_cast<SQLPOINTER>(value), 0);
}

static SQLRETURN SetEmptyCatalogResult(StatementHandle* stmt,
                                       const std::vector<std::string>& columnNames,
                                       const std::vector<std::string>& columnTypes) {
  stmt->ClearResultSet();
  stmt->AllocateSessionResultSet();
  if (!stmt->resultSetPtr) {
    stmt->addDiagnostic("HY001", "Unable to allocate catalog result set");
    return SQL_ERROR;
  }
  stmt->resultSetPtr->clear();
  stmt->resultSetPtr->columnNames = columnNames;
  stmt->resultSetPtr->columnTypes = columnTypes;
  stmt->resultSetPtr->numColumns = static_cast<int>(columnNames.size());
  stmt->resultSetPtr->numRows = 0;
  stmt->resultSetPtr->isMetaData = true;
  stmt->curRow = -1;
  stmt->rowsReturned = 0;
  stmt->isQuery = true;
  PopulateImplementationRowDescriptor(stmt);
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSpecialColumns(SQLHSTMT statementHandle, SQLUSMALLINT identifierType,
                                    SQLCHAR* catalogName, SQLSMALLINT nameLength1,
                                    SQLCHAR* schemaName, SQLSMALLINT nameLength2,
                                    SQLCHAR* tableName, SQLSMALLINT nameLength3, SQLUSMALLINT scope,
                                    SQLUSMALLINT nullable) {
  if (!statementHandle) {
    logMessage(nullptr, "SQLSpecialColumns: Invalid statement handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  auto* stmt = static_cast<StatementHandle*>(statementHandle);
  stmt->clearDiagnostics();
  if (identifierType != SQL_BEST_ROWID && identifierType != SQL_ROWVER) {
    stmt->addDiagnostic("HY097", "Column type out of range");
    return SQL_ERROR;
  }
  if (scope != SQL_SCOPE_CURROW && scope != SQL_SCOPE_TRANSACTION && scope != SQL_SCOPE_SESSION) {
    stmt->addDiagnostic("HY098", "Scope type out of range");
    return SQL_ERROR;
  }
  if (nullable != SQL_NO_NULLS && nullable != SQL_NULLABLE) {
    stmt->addDiagnostic("HY099", "Nullable type out of range");
    return SQL_ERROR;
  }
  (void)catalogName;
  (void)nameLength1;
  (void)schemaName;
  (void)nameLength2;
  (void)tableName;
  (void)nameLength3;
  return SetEmptyCatalogResult(
      stmt,
      {"SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH",
       "DECIMAL_DIGITS", "PSEUDO_COLUMN"},
      {"INT16", "TEXT", "INT16", "TEXT", "INT32", "INT32", "INT16", "INT16"});
}

SQLRETURN SQL_API SQLStatistics(SQLHSTMT statementHandle, SQLCHAR* catalogName,
                                SQLSMALLINT nameLength1, SQLCHAR* schemaName,
                                SQLSMALLINT nameLength2, SQLCHAR* tableName,
                                SQLSMALLINT nameLength3, SQLUSMALLINT unique,
                                SQLUSMALLINT reserved) {
  if (!statementHandle) {
    logMessage(nullptr, "SQLStatistics: Invalid statement handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  auto* stmt = static_cast<StatementHandle*>(statementHandle);
  stmt->clearDiagnostics();
  if (unique != SQL_INDEX_UNIQUE && unique != SQL_INDEX_ALL) {
    stmt->addDiagnostic("HY100", "Uniqueness option type out of range");
    return SQL_ERROR;
  }
  if (reserved != SQL_ENSURE && reserved != SQL_QUICK) {
    stmt->addDiagnostic("HY101", "Accuracy option type out of range");
    return SQL_ERROR;
  }
  (void)catalogName;
  (void)nameLength1;
  (void)schemaName;
  (void)nameLength2;
  (void)tableName;
  (void)nameLength3;
  // IoTDB exposes no relational indexes through ODBC, so the conformant
  // catalog result has the required schema and zero rows.
  return SetEmptyCatalogResult(stmt,
                               {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE",
                                "INDEX_QUALIFIER", "INDEX_NAME", "TYPE", "ORDINAL_POSITION",
                                "COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY", "PAGES",
                                "FILTER_CONDITION"},
                               {"TEXT", "TEXT", "TEXT", "INT16", "TEXT", "TEXT", "INT16", "INT16",
                                "TEXT", "TEXT", "INT32", "INT32", "TEXT"});
}

/*
when name is nullptr, return ""
dont deal with the % case here
 */
SQLRETURN SQL_API SQLTransact(SQLHENV environmentHandle, SQLHDBC connectionHandle,
                              SQLUSMALLINT completionType) {
  if (!environmentHandle && !connectionHandle) {
    logMessage(nullptr, "SQLTransact: Invalid environment/connection handle", LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }
  if (connectionHandle)
    return SQLEndTran(SQL_HANDLE_DBC, connectionHandle, completionType);
  return SQLEndTran(SQL_HANDLE_ENV, environmentHandle, completionType);
}

// sqlext.h

SQLRETURN SQL_API SQLDriverConnect(SQLHDBC connectionHandle, SQLHWND windowHandle,
                                   SQLCHAR* inConnectionString,
                                   SQLSMALLINT inConnectionStringLength,
                                   SQLCHAR* outConnectionString, SQLSMALLINT bufferLength,
                                   SQLSMALLINT* outConnectionStringLengthPtr,
                                   SQLUSMALLINT driverCompletion) {
  ConnectionHandle* cnct = static_cast<ConnectionHandle*>(connectionHandle);
  if (!cnct)
    return SQL_INVALID_HANDLE;
  cnct->clearDiagnostics();
  logMessage(cnct, "SQLDriverConnect: Entering", LOG_LEVEL_TRACE);

  if (isLogLevelEnabled(cnct, LOG_LEVEL_TRACE)) {
    std::stringstream logStream;
    logStream << "SQLDriverConnect parameters: "
              << "connectionHandle = " << handleToString(connectionHandle)
              << ", windowHandle = " << handleToString(windowHandle)
              << ", inConnectionStringLength = " << inConnectionStringLength
              << ", bufferLength = " << bufferLength << ", driverCompletion = " << driverCompletion;
    logMessage(cnct, logStream.str(), LOG_LEVEL_TRACE);
  }

  if (!inConnectionString || inConnectionStringLength == 0) {
    logMessage(cnct, "SQLDriverConnect: Connection string is empty or null", LOG_LEVEL_ERROR);
    cnct->addDiagnostic("08001", "Invalid connection string");
    return SQL_ERROR;
  }

  if (inConnectionStringLength < 0 && inConnectionStringLength != SQL_NTS) {
    cnct->addDiagnostic("HY090", "Invalid connection string length");
    return SQL_ERROR;
  }
  if (bufferLength < 0) {
    cnct->addDiagnostic("HY090", "Invalid output buffer length");
    return SQL_ERROR;
  }
  std::string connectionString((const char*)inConnectionString,
                               inConnectionStringLength == SQL_NTS
                                   ? strlen((const char*)inConnectionString)
                                   : static_cast<size_t>(inConnectionStringLength));
  try {

    // --- Phase 1: first pass to extract DSN name and detect DRIVER keyword ---
    auto attributes = ParseConnectionString(connectionString);
    std::string dsnName;
    bool hasDriverKeyword = false;
    {
      for (const auto& attribute : attributes) {
        std::string key = attribute.first;
        std::string keyLower = key;
        std::transform(keyLower.begin(), keyLower.end(), keyLower.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        if (keyLower == "dsn") {
          dsnName = attribute.second;
        } else if (keyLower == "driver") {
          hasDriverKeyword = true;
        }
      }
    }
    logMessage(cnct,
               "SQLDriverConnect: Extracted DSN='" + dsnName +
                   "', hasDriver=" + (hasDriverKeyword ? "true" : "false"),
               LOG_LEVEL_DEBUG);
    cnct->dataSourceName = dsnName;

    // --- Phase 2: if DSN is specified and no DRIVER keyword, load from ODBC.INI ---
    if (!dsnName.empty() && !hasDriverKeyword) {
      logMessage(cnct,
                 "SQLDriverConnect: Loading DSN '" + dsnName + "' from ODBC.INI as base config",
                 LOG_LEVEL_INFO);
      cnct->LoadDsnFromOdbcIni(dsnName);
    }

    // --- Phase 3: parse connection string again to override DSN values ---
    logMessage(cnct, "SQLDriverConnect: Applying connection string overrides", LOG_LEVEL_TRACE);
    {
      for (const auto& attribute : attributes)
        SetConnectionHandle(cnct, attribute.first, attribute.second);
    }

    if (isLogLevelEnabled(cnct, LOG_LEVEL_INFO)) {
      std::ostringstream oss;
      oss << "SQLDriverConnect: Final config - Server=" << cnct->serverHostName
          << ", Port=" << cnct->serverPort << ", UID=" << cnct->userName
          << ", Database=" << cnct->database
          << ", TableModel=" << (cnct->isTableModel ? "true" : "false")
          << ", LogLevel=" << cnct->logLevel << ", SessionTimeout=" << cnct->sessionTimeoutMs
          << ", BatchSize=" << cnct->batchSize;
      logMessage(cnct, oss.str(), LOG_LEVEL_INFO);
    }

    bool outputTruncated = false;
    if (outConnectionStringLengthPtr) {
      *outConnectionStringLengthPtr = static_cast<SQLSMALLINT>(std::min(
          connectionString.size(), static_cast<size_t>(std::numeric_limits<SQLSMALLINT>::max())));
    }
    if (outConnectionString && bufferLength > 0) {
      const size_t copyLength =
          std::min(connectionString.size(), static_cast<size_t>(bufferLength - 1));
      std::memcpy(outConnectionString, connectionString.data(), copyLength);
      outConnectionString[copyLength] = '\0';
      outputTruncated = copyLength < connectionString.size();
      logMessage(cnct, "SQLDriverConnect: Copied connection string to output buffer",
                 LOG_LEVEL_DEBUG);
    }

    if (cnct->isTableModel && cnct->database.empty()) {
      logMessage(cnct, "SQLDriverConnect: Database not set, using 'information_schema' as default",
                 LOG_LEVEL_INFO);
      cnct->database = "information_schema";
    }

    cnct->ValidateTransport();
    SQLRETURN returnCode;
    if (cnct->useRestful) {
      logMessage(cnct, "SQLDriverConnect: Connecting using RESTful API", LOG_LEVEL_DEBUG);
      returnCode = IoTDB_DriverConnect_Rest(cnct);
    } else {
      logMessage(cnct, "SQLDriverConnect: Connecting using Session API", LOG_LEVEL_DEBUG);
      returnCode = IoTDB_DriverConnect_Session(cnct);
    }

    logMessage(cnct, "SQLDriverConnect: Exiting with return code: " + std::to_string(returnCode),
               LOG_LEVEL_TRACE);
    if (SQL_SUCCEEDED(returnCode) && outputTruncated) {
      cnct->addDiagnostic("01004", "Connection string data was truncated");
      return SQL_SUCCESS_WITH_INFO;
    }
    return returnCode;
  } catch (const std::exception& e) {
    cnct->addDiagnostic("08001", e.what());
    return SQL_ERROR;
  }
}

SQLRETURN SQL_API SQLMoreResults(SQLHSTMT statementHandle) {
  const auto stmt = static_cast<StatementHandle*>(statementHandle);
  ConnectionHandle* cnct = stmt ? stmt->getConnection() : nullptr;

  logMessage(cnct, "SQLMoreResults: Entering", LOG_LEVEL_TRACE);

  if (!statementHandle) {
    logMessage(cnct, "SQLMoreResults: Invalid statement handle, returning SQL_INVALID_HANDLE",
               LOG_LEVEL_ERROR);
    return SQL_INVALID_HANDLE;
  }

  logMessage(cnct, "SQLMoreResults: Only support one result set. Returning SQL_NO_DATA",
             LOG_LEVEL_DEBUG);
  return SQL_NO_DATA;
}

#ifdef WIN32
#include "setup/setup_dialog.h"

extern "C" BOOL APIENTRY ConfigDSN(HWND hwndParent, WORD fRequest, LPCSTR lpszDriver,
                                   LPCSTR lpszAttributes) {
  logMessage(nullptr, "ConfigDSN: Entering", LOG_LEVEL_INFO);
  return ShowDSNDialog(hwndParent, fRequest, lpszDriver, lpszAttributes);
}
#endif
