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
#ifndef ODBCRESULTSET_H
#define ODBCRESULTSET_H

#include "Pch.h"
#include "ODBCField.h"

class StatementHandle; // Forward declaration
class SessionDataSet;  // Forward declaration

struct BindColInfo {
  SQLSMALLINT targetType;    // C data type (SQL_C_CHAR, etc.)
  SQLPOINTER targetValuePtr; // Data buffer pointer
  SQLLEN bufferLength;       // Buffer length
  SQLLEN* strLen_or_IndPtr;  // Length/indicator pointer
  bool isBound;              // Whether bound
  BindColInfo()
      : targetType(SQL_C_DEFAULT), targetValuePtr(nullptr), bufferLength(0),
        strLen_or_IndPtr(nullptr), isBound(false) {}
};

class ODBCResultSet {
public:
  ODBCResultSet(StatementHandle* stmtHandle);
  virtual ~ODBCResultSet();

  // Unified data loading interface - to be implemented by subclasses
  virtual void loadData(const nlohmann::json& jsonData) = 0;
  virtual void loadData(std::unique_ptr<SessionDataSet> sessionDataSet) = 0;
  // Query methods
  // Get column name by column index (0-based)
  // @param columnIndex Column index (0-based)
  // @return Column name as string, empty string if invalid index
  std::string getColumnName(size_t columnIndex) const;

  // Get column type by column index (0-based)
  // @param columnIndex Column index (0-based)
  // @return Column type as string, empty string if invalid index
  std::string getColumnType(size_t columnIndex) const;

  // Get default C type for SQL_C_DEFAULT for a column (1-based).
  SQLSMALLINT getDefaultCTypeForColumn(SQLUSMALLINT columnNumber) const;

  // Get data value by row and column index (0-based)
  // @param rowIndex Row index (0-based)
  // @param columnIndex Column index (0-based)
  // @return Data value as string, empty string if invalid indices
  const ODBCField& getValue(size_t rowIndex, size_t columnIndex) const;

  // Find column index by column name
  // @param columnName Column name to find
  // @return Column index (0-based), or -1 if not found
  int findColumnIndex(const std::string& columnName) const;

  // Set data value by row and column index
  // @param rowIndex Row index (0-based)
  // @param columnIndex Column index (0-based)
  // @param value New value to set
  // @return true if successful, false if invalid indices
  bool setValue(size_t rowIndex, size_t columnIndex, const ODBCField& value);

  // Set column name by column index
  // @param columnIndex Column index (0-based)
  // @param columnName New column name
  // @return true if successful, false if invalid index
  bool setColumnName(size_t columnIndex, const std::string& columnName);

  // Set column type by column index
  // @param columnIndex Column index (0-based)
  // @param columnType New column type
  // @return true if successful, false if invalid index
  bool setColumnType(size_t columnIndex, const std::string& columnType);

  // Remove rows that match the predicate function
  // @param predicate Function that returns true for rows to be removed
  // @return Number of rows removed
  template <typename Predicate> size_t removeRowsIf(Predicate predicate) {
    size_t originalSize = data.size();
    data.erase(std::remove_if(data.begin(), data.end(), predicate), data.end());
    size_t removedCount = originalSize - data.size();
    numRows = data.size();
    return removedCount;
  }

  int getNumRows() const;
  int getNumColumns() const;
  bool getIsMetaData() const;
  bool isEmpty() const;

  // Clear all data and reset to initial state
  void clear();

  // Check if data type conversion is valid for a column
  // @param columnNumber Column number to check (1-based)
  // @param targetType Target C data type
  // @return true if conversion is valid, false otherwise
  bool isValidConversion(SQLUSMALLINT columnNumber, SQLSMALLINT targetType);

  // Get raw JSON value for SQLGetData type conversion
  // @param rowIndex Row index (0-based)
  // @param columnNumber Column number (1-based, as used in SQLGetData)
  // @param value Output parameter for the JSON value
  // @return true if successful, false otherwise
  virtual bool getRawJsonValue(size_t rowIndex, SQLUSMALLINT columnNumber, nlohmann::json& value);

  // Bind a column to application variables for data retrieval
  // @param columnNumber Column number to bind (1-based)
  // @param targetType C data type for the column data
  // @param targetValuePtr Pointer to the buffer for column data
  // @param bufferLength Length of the buffer
  // @param strLen_or_IndPtr Pointer to length/indicator variable
  // @return SQL_SUCCESS if successful, SQL_ERROR otherwise
  SQLRETURN bindColumn(SQLUSMALLINT columnNumber, SQLSMALLINT targetType, SQLPOINTER targetValuePtr,
                       SQLLEN bufferLength, SQLLEN* strLen_or_IndPtr);

  // Unbind columns
  void unbindColumns();

  // Get binding information for a column
  // @param columnNumber Column number (1-based)
  // @return Pointer to BindColInfo, or nullptr if not bound
  BindColInfo* getBindColInfo(SQLUSMALLINT columnNumber);

  // Check if a column is bound
  // @param columnNumber Column number (1-based)
  // @return true if bound, false otherwise
  bool isColumnBound(SQLUSMALLINT columnNumber);

  bool isRestful() const;

  void outputTable(int maxRows = 20) const;

  // Member variables, consistent with corresponding members in StatementHandle
  int numRows;
  int numColumns;
  bool
      isMetaData; // Whether metadata, affects json content format. Only valid for tree model, meaningless for table model

  // Data storage using vector format
  std::vector<std::string> columnNames;     // Column names
  std::vector<std::string> columnTypes;     // Column types
  std::vector<std::vector<ODBCField>> data; // Row data, each row is a vector of ODBCFields

  // Column binding information
  std::vector<BindColInfo> bindColInfo;

protected:
  StatementHandle* stmt; // Statement handle reference
  static const std::unordered_map<std::string, TSDataType::TSDataType> columnTypeToTSDataTypeMap;
};

#endif //ODBCRESULTSET_H
