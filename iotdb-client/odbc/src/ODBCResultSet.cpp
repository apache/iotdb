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
#include "ODBCResultSet.h"

// Project includes
#include "ODBCField.h"
#include "Log.h"
#include "TableSession.h"
#include "TableSessionBuilder.h"
#include "driver.h"
#include "SessionDataSet.h"

// IoTDB Session API includes for streaming support
#include <SessionDataSet.h>

const std::unordered_map<std::string, TSDataType::TSDataType>
    ODBCResultSet::columnTypeToTSDataTypeMap = {
        {"BOOLEAN", TSDataType::BOOLEAN},     {"INT16", TSDataType::INT32},
        {"INT32", TSDataType::INT32},         {"INT64", TSDataType::INT64},
        {"FLOAT", TSDataType::FLOAT},         {"DOUBLE", TSDataType::DOUBLE},
        {"STRING", TSDataType::STRING},       {"TEXT", TSDataType::TEXT},
        {"DATE", TSDataType::DATE},           {"BLOB", TSDataType::BLOB},
        {"TIMESTAMP", TSDataType::TIMESTAMP},
};

ODBCResultSet::ODBCResultSet(StatementHandle* stmtHandle)
    : stmt(stmtHandle), numRows(0), numColumns(0), isMetaData(false) {
  // Initialize member variables - vectors are automatically initialized as empty
}

bool ODBCResultSet::isRestful() const {
  return stmt->getConnection()->useRestful;
}

ODBCResultSet::~ODBCResultSet() {
  // Destructor - shared_ptr will handle cleanup automatically
}

// JSON parsing functionality moved to subclasses via loadData methods
// parseJsonResponseTree implementation moved to ODBCRestResultSet

// JSON parsing functionality moved to subclasses via loadData methods
// parseJsonResponseTable implementation moved to ODBCRestResultSet

// Get column name by column index (0-based)
// @param columnIndex Column index (0-based)
// @return Column name as string, empty string if invalid index
std::string ODBCResultSet::getColumnName(size_t columnIndex) const {
  logMessage(stmt->getConnection(), "ODBCResultSet::getColumnName: Starting", LOG_LEVEL_TRACE);
  if (columnIndex < columnNames.size()) {
    return columnNames[columnIndex];
  }
  logMessage(stmt->getConnection(), "ODBCResultSet::getColumnName: Exiting", LOG_LEVEL_TRACE);
  return "";
}

// Get column type by column index (0-based)
// @param columnIndex Column index (0-based)
// @return Column type as string, empty string if invalid index
SQLSMALLINT ODBCResultSet::getDefaultCTypeForColumn(SQLUSMALLINT columnNumber) const {
  if (columnNumber < 1 || columnNumber > getNumColumns()) {
    return SQL_C_CHAR;
  }
  std::string columnType = getColumnType(columnNumber - 1);
  if (columnType == "INT16")
    return SQL_C_SSHORT;
  auto it = columnTypeToTSDataTypeMap.find(columnType);
  if (it == columnTypeToTSDataTypeMap.end()) {
    return SQL_C_CHAR;
  }
  SQLSMALLINT defaultType = ODBCField::resolveDefaultCType(it->second);
  logMessage(stmt->getConnection(),
             "ODBCResultSet::getDefaultCTypeForColumn: Column " + std::to_string(columnNumber) +
                 ", TSDataType: " + columnType + ", default type: " + CDataTypeName(defaultType),
             LOG_LEVEL_TRACE);
  return defaultType;
}

std::string ODBCResultSet::getColumnType(size_t columnIndex) const {
  logMessage(stmt->getConnection(), "ODBCResultSet::getColumnType: Starting", LOG_LEVEL_TRACE);
  if (columnIndex < columnTypes.size()) {
    return columnTypes[columnIndex];
  }
  logMessage(stmt->getConnection(), "ODBCResultSet::getColumnType: Exiting", LOG_LEVEL_TRACE);
  return "";
}

// Get data value by row and column index (0-based)
// @param rowIndex Row index (0-based)
// @param columnIndex Column index (0-based)
// @return Data value as string, empty string if invalid indices
const ODBCField& ODBCResultSet::getValue(size_t rowIndex, size_t columnIndex) const {
  logMessage(stmt->getConnection(), "ODBCResultSet::getValue: Starting", LOG_LEVEL_TRACE);
  logMessage(stmt->getConnection(),
             "ODBCResultSet::getValue: rowIndex = " + std::to_string(rowIndex) +
                 ", columnIndex = " + std::to_string(columnIndex),
             LOG_LEVEL_TRACE);
  static const ODBCField kNullField;
  if (rowIndex < 0 || rowIndex >= data.size()) {
    logMessage(stmt->getConnection(),
               "ODBCResultSet::getValue: Invalid row index, rowIndex = " +
                   std::to_string(rowIndex) + ", returning empty field",
               LOG_LEVEL_ERROR);
    return kNullField;
  }
  if (columnIndex < 0 || columnIndex >= data[rowIndex].size()) {
    logMessage(
        stmt->getConnection(),
        "ODBCResultSet::getValue: Invalid column index, rowIndex = " + std::to_string(rowIndex) +
            ", columnIndex = " + std::to_string(columnIndex) + ", returning empty field",
        LOG_LEVEL_ERROR);
    return kNullField;
  }

  return data[rowIndex][columnIndex];
}

// Find column index by column name
// @param columnName Column name to find
// @return Column index (0-based), or -1 if not found
int ODBCResultSet::findColumnIndex(const std::string& columnName) const {
  logMessage(stmt->getConnection(), "ODBCResultSet::findColumnIndex: Starting", LOG_LEVEL_TRACE);
  for (size_t i = 0; i < columnNames.size(); ++i) {
    if (columnNames[i] == columnName) {
      return static_cast<int>(i);
    }
  }
  logMessage(stmt->getConnection(), "ODBCResultSet::findColumnIndex: Exiting", LOG_LEVEL_TRACE);
  return -1;
}

// Set data value by row and column index
// @param rowIndex Row index (0-based)
// @param columnIndex Column index (0-based)
// @param value New value to set
// @return true if successful, false if invalid indices
bool ODBCResultSet::setValue(size_t rowIndex, size_t columnIndex, const ODBCField& value) {
  logMessage(stmt->getConnection(), "ODBCResultSet::setValue: Starting", LOG_LEVEL_TRACE);
  if (rowIndex < data.size() && columnIndex < data[rowIndex].size()) {
    data[rowIndex][columnIndex] = value;
    return true;
  }
  logMessage(stmt->getConnection(), "ODBCResultSet::setValue: Exiting", LOG_LEVEL_TRACE);
  return false;
}

// Set column name by column index
// @param columnIndex Column index (0-based)
// @param columnName New column name
// @return true if successful, false if invalid index
bool ODBCResultSet::setColumnName(size_t columnIndex, const std::string& columnName) {
  logMessage(stmt->getConnection(), "ODBCResultSet::setColumnName: Starting", LOG_LEVEL_TRACE);
  if (columnIndex < columnNames.size()) {
    columnNames[columnIndex] = columnName;
    return true;
  }
  logMessage(stmt->getConnection(), "ODBCResultSet::setColumnName: Exiting", LOG_LEVEL_TRACE);
  return false;
}

// Set column type by column index
// @param columnIndex Column index (0-based)
// @param columnType New column type
// @return true if successful, false if invalid index
bool ODBCResultSet::setColumnType(size_t columnIndex, const std::string& columnType) {
  logMessage(stmt->getConnection(), "ODBCResultSet::setColumnType: Starting", LOG_LEVEL_TRACE);
  if (columnIndex < columnTypes.size()) {
    columnTypes[columnIndex] = columnType;
    return true;
  }
  logMessage(stmt->getConnection(), "ODBCResultSet::setColumnType: Exiting", LOG_LEVEL_TRACE);
  return false;
}

// Get raw JSON value for SQLGetData type conversion
// @param rowIndex Row index (0-based)
// @param columnNumber Column number (1-based, as used in SQLGetData)
// @param value Output parameter for the JSON value
// @return true if successful, false otherwise

bool ODBCResultSet::getRawJsonValue(size_t rowIndex, SQLUSMALLINT columnNumber,
                                    nlohmann::json& value) {
  // Default implementation for non-REST result sets
  logMessage(stmt->getConnection(),
             "ODBCResultSet::getRawJsonValue: Not supported for this result set type",
             LOG_LEVEL_ERROR);
  return false;
}

int ODBCResultSet::getNumRows() const {
  return numRows;
}

int ODBCResultSet::getNumColumns() const {
  return numColumns;
}

bool ODBCResultSet::getIsMetaData() const {
  return isMetaData;
}

bool ODBCResultSet::isEmpty() const {
  return data.empty();
}

// Clear all data and reset to initial state
void ODBCResultSet::clear() {
  logMessage(stmt->getConnection(), "ODBCResultSet::clear: Starting", LOG_LEVEL_TRACE);

  // Reset primitive members
  numRows = 0;
  numColumns = 0;
  isMetaData = false;

  // Clear vector contents
  columnNames.clear();
  columnTypes.clear();
  data.clear();

  logMessage(stmt->getConnection(), "ODBCResultSet::clear: Exiting", LOG_LEVEL_TRACE);
}

void ODBCResultSet::outputTable(int maxRows) const {
  (void)maxRows;
  // Result values may contain credentials or application data. Log only shape.
  logMessage(stmt->getConnection(),
             "ODBCResultSet: " + std::to_string(numRows) + " rows, " + std::to_string(numColumns) +
                 " columns",
             LOG_LEVEL_DEBUG);
}

// Check if data type conversion is valid for a column
bool ODBCResultSet::isValidConversion(SQLUSMALLINT columnNumber, SQLSMALLINT targetType) {
  std::string columnType = getColumnType(columnNumber - 1);
  if (columnTypeToTSDataTypeMap.find(columnType) == columnTypeToTSDataTypeMap.end()) {
    logMessage(stmt->getConnection(),
               "ODBCResultSet::isValidConversion: Invalid column type: " + columnType,
               LOG_LEVEL_ERROR);
    return false;
  }
  TSDataType::TSDataType tsDataType = columnTypeToTSDataTypeMap.at(columnType);
  bool canConvert = ODBCField::canConvert(tsDataType, targetType);
  if (!canConvert) {
    logMessage(stmt->getConnection(),
               "ODBCResultSet::isValidConversion: Invalid data type conversion for column " +
                   std::to_string(columnNumber) + ", source type : " + columnType +
                   ", target type: " + CDataTypeName(targetType),
               LOG_LEVEL_ERROR);
    return false;
  }
  return true;
}

// Bind a column to application variables for data retrieval
SQLRETURN ODBCResultSet::bindColumn(SQLUSMALLINT columnNumber, SQLSMALLINT targetType,
                                    SQLPOINTER targetValuePtr, SQLLEN bufferLength,
                                    SQLLEN* strLen_or_IndPtr) {
  ConnectionHandle* cnct = stmt->getConnection();
  logMessage(cnct, "ODBCResultSet::bindColumn: Entering", LOG_LEVEL_TRACE);
  if (isLogLevelEnabled(cnct, LOG_LEVEL_TRACE)) {
    std::stringstream logStream;
    logStream << "Parameters: "
              << "columnNumber = " << columnNumber << ", targetType = " << targetType
              << ", targetValue = " << targetValuePtr << ", bufferLength = " << bufferLength
              << ", strLen_or_Ind = " << strLen_or_IndPtr;

    logMessage(cnct, logStream.str(), LOG_LEVEL_TRACE);
  }

  if (columnNumber == 0) {
    // Bookmark column not supported
    logMessage(cnct, "ODBCResultSet::bindColumn: Bookmark column (0) not supported",
               LOG_LEVEL_WARN);
    stmt->addDiagnostic("HYC00", "Bookmarks are not supported");
    return SQL_ERROR;
  }

  if (bufferLength < 0) {
    logMessage(cnct,
               "ODBCResultSet::bindColumn: Invalid buffer length: " + std::to_string(bufferLength),
               LOG_LEVEL_ERROR);
    stmt->addDiagnostic("HY090", "Invalid string or buffer length");
    return SQL_ERROR;
  }

  if (columnNumber > getNumColumns()) {
    logMessage(cnct,
               "ODBCResultSet::bindColumn: Invalid column index: " + std::to_string(columnNumber) +
                   ", max columns: " + std::to_string(getNumColumns()),
               LOG_LEVEL_ERROR);
    stmt->addDiagnostic("07009", "Invalid descriptor index");
    return SQL_ERROR;
  }

  if (!targetValuePtr && !strLen_or_IndPtr) {
    if (bindColInfo.size() > columnNumber)
      bindColInfo[columnNumber] = BindColInfo();
    return SQL_SUCCESS;
  }

  SQLSMALLINT actualType =
      (targetType == SQL_C_DEFAULT) ? getDefaultCTypeForColumn(columnNumber) : targetType;
  logMessage(cnct, "ODBCResultSet::bindColumn: Actual type: " + CDataTypeName(actualType),
             LOG_LEVEL_TRACE);

  if (!isValidConversion(columnNumber, actualType)) {
    logMessage(cnct,
               "ODBCResultSet::bindColumn: Invalid data type conversion for column " +
                   std::to_string(columnNumber) + ", target type: " + std::to_string(targetType),
               LOG_LEVEL_ERROR);
    stmt->addDiagnostic("07006", "Restricted data type attribute violation");
    return SQL_ERROR;
  }

  // Ensure bindColInfo vector is large enough
  if (bindColInfo.size() <= columnNumber) {
    bindColInfo.resize(columnNumber + 1);
  }

  BindColInfo* binding = &bindColInfo[columnNumber];
  binding->targetType = actualType;
  binding->targetValuePtr = targetValuePtr;
  binding->bufferLength = bufferLength;
  binding->strLen_or_IndPtr = strLen_or_IndPtr;
  binding->isBound = true;

  if (targetValuePtr && actualType == SQL_C_CHAR && bufferLength > 0) {
    // Initialize to an empty string
    static_cast<char*>(targetValuePtr)[0] = '\0';
    if (strLen_or_IndPtr) {
      *strLen_or_IndPtr = SQL_NTS;
    }
  }
  logMessage(cnct, "ODBCResultSet::bindColumn: Exiting", LOG_LEVEL_TRACE);
  return SQL_SUCCESS;
}

// Unbind columns
void ODBCResultSet::unbindColumns() {
  logMessage(stmt->getConnection(), "ODBCResultSet::unbindColumns: Entering", LOG_LEVEL_TRACE);
  for (auto& binding : bindColInfo) {
    binding.isBound = false;
    binding.targetValuePtr = nullptr;
    binding.bufferLength = 0;
    binding.strLen_or_IndPtr = nullptr;
    binding.isBound = false;
  }
  logMessage(stmt->getConnection(), "ODBCResultSet::unbindColumns: Exiting", LOG_LEVEL_TRACE);
}

// Get binding information for a column
BindColInfo* ODBCResultSet::getBindColInfo(SQLUSMALLINT columnNumber) {
  if (columnNumber >= bindColInfo.size() || columnNumber == 0) {
    return nullptr;
  }
  return &bindColInfo[columnNumber];
}

// Check if a column is bound
bool ODBCResultSet::isColumnBound(SQLUSMALLINT columnNumber) {
  BindColInfo* binding = getBindColInfo(columnNumber);
  return binding && binding->isBound;
}
