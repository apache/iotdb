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
#define WIN32_LEAN_AND_MEAN
#include <windows.h>

#include <sql.h>
#include <sqlext.h>
#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <ctime>

#ifndef SQL_DIAG_COLUMN_SIZE
#define SQL_DIAG_COLUMN_SIZE 33L
#endif

// Error handling function
void CheckOdbcError(SQLRETURN retCode, SQLSMALLINT handleType, SQLHANDLE handle,
                    const char* functionName) {
  if (retCode == SQL_SUCCESS || retCode == SQL_SUCCESS_WITH_INFO) {
    return;
  }

  SQLCHAR sqlState[6];
  SQLCHAR message[SQL_MAX_MESSAGE_LENGTH];
  SQLINTEGER nativeError;
  SQLSMALLINT textLength;
  SQLRETURN errRet;
  errRet = SQLGetDiagRec(handleType, handle, 1, sqlState, &nativeError, message, sizeof(message),
                         &textLength);

  std::cerr << "ODBC Error in " << functionName << ":\n";
  std::cerr << "  SQL State: " << sqlState << "\n";
  std::cerr << "  Native Error: " << nativeError << "\n";
  std::cerr << "  Message: " << message << "\n";
  std::cerr << "  SQLGetDiagRec Return: " << errRet << "\n";

  if (retCode == SQL_ERROR || retCode == SQL_INVALID_HANDLE) {
    exit(1);
  }
}

// Simplified table output - display basic data only
void PrintSimpleTable(const std::vector<std::string>& headers,
                      const std::vector<std::vector<std::string>>& rows) {
  // Print header
  for (size_t i = 0; i < headers.size(); i++) {
    std::cout << headers[i];
    if (i < headers.size() - 1)
      std::cout << "\t";
  }
  std::cout << std::endl;

  // Print separator
  for (size_t i = 0; i < headers.size(); i++) {
    std::cout << "----------------";
    if (i < headers.size() - 1)
      std::cout << "\t";
  }
  std::cout << std::endl;

  // Print data rows
  for (const auto& row : rows) {
    for (size_t i = 0; i < row.size(); i++) {
      std::cout << row[i];
      if (i < row.size() - 1)
        std::cout << "\t";
    }
    std::cout << std::endl;
  }
  std::cout << std::endl;
}

/// Execute a SELECT query and display fulltable results in table format
void Query(SQLHDBC hDbc) {
  SQLHSTMT hStmt = SQL_NULL_HSTMT;
  SQLRETURN ret = SQL_SUCCESS;

  try {
    // Allocate statement handle
    ret = SQLAllocHandle(SQL_HANDLE_STMT, hDbc, &hStmt);
    if (!SQL_SUCCEEDED(ret)) {
      CheckOdbcError(ret, SQL_HANDLE_DBC, hDbc, "SQLAllocHandle(SQL_HANDLE_STMT)");
      return;
    }

    // Execute query
    const std::string sqlQuery = "select * from fulltable";
    std::cout << "Execute query: " << sqlQuery << std::endl;

    ret = SQLExecDirect(hStmt, reinterpret_cast<SQLCHAR*>(const_cast<char*>(sqlQuery.c_str())),
                        SQL_NTS);
    if (!SQL_SUCCEEDED(ret)) {
      if (ret != SQL_NO_DATA) {
        CheckOdbcError(ret, SQL_HANDLE_STMT, hStmt, "SQLExecDirect(SELECT)");
      }
      SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
      return;
    }

    // Get column count
    SQLSMALLINT colCount = 0;
    ret = SQLNumResultCols(hStmt, &colCount);
    if (!SQL_SUCCEEDED(ret)) {
      CheckOdbcError(ret, SQL_HANDLE_STMT, hStmt, "SQLNumResultCols");
      SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
      return;
    }

    std::cout << "Column count = " << colCount << std::endl;

    // Return immediately if no columns
    if (colCount <= 0) {
      SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
      return;
    }

    // Get column names and type information
    std::vector<std::string> columnNames;
    std::vector<SQLSMALLINT> columnTypes(colCount);
    std::vector<SQLULEN> columnSizes(colCount);
    std::vector<SQLSMALLINT> decimalDigits(colCount);
    std::vector<SQLSMALLINT> nullable(colCount);

    // Get basic column information
    for (SQLSMALLINT i = 1; i <= colCount; i++) {
      SQLSMALLINT nameLength = 0;
      ret = SQLDescribeCol(hStmt, i, NULL, 0, &nameLength, NULL, NULL, NULL, NULL);
      if (!SQL_SUCCEEDED(ret)) {
        CheckOdbcError(ret, SQL_HANDLE_STMT, hStmt, "SQLDescribeCol (get length)");
        SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
        return;
      }

      std::vector<SQLCHAR> colNameBuffer(nameLength + 1);
      SQLSMALLINT actualNameLength = 0;

      ret = SQLDescribeCol(hStmt, i, colNameBuffer.data(), nameLength + 1, &actualNameLength, NULL,
                           NULL, NULL, NULL);
      if (!SQL_SUCCEEDED(ret)) {
        CheckOdbcError(ret, SQL_HANDLE_STMT, hStmt, "SQLDescribeCol (get name)");
        SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
        return;
      }

      std::string fullName(reinterpret_cast<char*>(colNameBuffer.data()));

      size_t pos = fullName.find_last_of('.');
      if (pos != std::string::npos) {
        columnNames.push_back(fullName.substr(pos + 1));
      } else {
        columnNames.push_back(fullName);
      }

      ret = SQLDescribeCol(hStmt, i, NULL, 0, NULL, &columnTypes[i - 1], &columnSizes[i - 1],
                           &decimalDigits[i - 1], &nullable[i - 1]);
      if (!SQL_SUCCEEDED(ret)) {
        CheckOdbcError(ret, SQL_HANDLE_STMT, hStmt, "SQLDescribeCol (get type info)");
        SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
        return;
      }
    }

    std::vector<std::vector<std::string>> tableRows;

    int rowCount = 0;
    // Get data front every row
    while (true) {
      ret = SQLFetch(hStmt);
      if (ret == SQL_NO_DATA) {
        break;
      }

      if (!SQL_SUCCEEDED(ret)) {
        CheckOdbcError(ret, SQL_HANDLE_STMT, hStmt, "SQLFetch");
        break;
      }

      std::vector<std::string> row;

      for (SQLSMALLINT i = 1; i <= colCount; i++) {
        SQLLEN indicator = 0;
        std::string valueStr;

        SQLSMALLINT cType;
        size_t bufferSize;
        bool isCharacterType = false;
        const int maxBufferSize = 32768;

        switch (columnTypes[i - 1]) {
        case SQL_CHAR:
        case SQL_VARCHAR:
        case SQL_LONGVARCHAR:
        case SQL_WCHAR:
        case SQL_WVARCHAR:
        case SQL_WLONGVARCHAR:
          cType = SQL_C_CHAR;
          if (columnSizes[i - 1] > 0) {
            bufferSize = min(maxBufferSize, static_cast<size_t>(columnSizes[i - 1]) * 4 + 1);
          } else {
            bufferSize = maxBufferSize;
          }
          isCharacterType = true;
          break;

        case SQL_DECIMAL:
        case SQL_NUMERIC:
          cType = SQL_C_CHAR;
          if (columnSizes[i - 1] > 0) {
            bufferSize = min(maxBufferSize, static_cast<size_t>(columnSizes[i - 1]) * 4 + 1);
          } else {
            bufferSize = maxBufferSize;
          }
          isCharacterType = true;
          break;

        case SQL_INTEGER:
        case SQL_SMALLINT:
        case SQL_TINYINT:
        case SQL_BIGINT:
          cType = SQL_C_SBIGINT;
          bufferSize = sizeof(SQLBIGINT);
          break;

        case SQL_REAL:
        case SQL_FLOAT:
        case SQL_DOUBLE:
          cType = SQL_C_DOUBLE;
          bufferSize = sizeof(double);
          break;

        case SQL_BIT:
          cType = SQL_C_BIT;
          bufferSize = sizeof(SQLCHAR);
          break;

        case SQL_DATE:
        case SQL_TYPE_DATE:
          cType = SQL_C_DATE;
          bufferSize = sizeof(SQL_DATE_STRUCT);
          break;

        case SQL_TIME:
        case SQL_TYPE_TIME:
          cType = SQL_C_TIME;
          bufferSize = sizeof(SQL_TIME_STRUCT);
          break;

        case SQL_TIMESTAMP:
        case SQL_TYPE_TIMESTAMP:
          cType = SQL_C_TIMESTAMP;
          bufferSize = sizeof(SQL_TIMESTAMP_STRUCT);
          break;

        default:
          cType = SQL_C_CHAR;
          bufferSize = 256;
          isCharacterType = true;
          break;
        }

        std::vector<BYTE> buffer(bufferSize);

        ret = SQLGetData(hStmt, i, cType, buffer.data(), bufferSize, &indicator);

        if (indicator == SQL_NULL_DATA) {
          valueStr = "NULL";
        } else if (ret != SQL_SUCCESS) {
          valueStr = "ERR_CONV";
        } else {
          if (cType == SQL_C_CHAR) {
            valueStr = reinterpret_cast<char*>(buffer.data());
          } else if (cType == SQL_C_SBIGINT) {
            SQLBIGINT intVal = *reinterpret_cast<SQLBIGINT*>(buffer.data());
            valueStr = std::to_string(intVal);
          } else if (cType == SQL_C_DOUBLE) {
            double doubleVal = *reinterpret_cast<double*>(buffer.data());
            valueStr = std::to_string(doubleVal);
          } else if (cType == SQL_C_BIT) {
            valueStr = (*buffer.data() != 0) ? "TRUE" : "FALSE";
          } else if (cType == SQL_C_DATE) {
            SQL_DATE_STRUCT* date = reinterpret_cast<SQL_DATE_STRUCT*>(buffer.data());
            char dateStr[20];
            snprintf(dateStr, sizeof(dateStr), "%04d-%02d-%02d", date->year, date->month,
                     date->day);
            valueStr = dateStr;
          } else if (cType == SQL_C_TIME) {
            SQL_TIME_STRUCT* time = reinterpret_cast<SQL_TIME_STRUCT*>(buffer.data());
            char timeStr[15];
            snprintf(timeStr, sizeof(timeStr), "%02d:%02d:%02d", time->hour, time->minute,
                     time->second);
            valueStr = timeStr;
          } else if (cType == SQL_C_TIMESTAMP) {
            SQL_TIMESTAMP_STRUCT* ts = reinterpret_cast<SQL_TIMESTAMP_STRUCT*>(buffer.data());
            char tsStr[30];
            snprintf(tsStr, sizeof(tsStr), "%04d-%02d-%02d %02d:%02d:%02d.%06d", ts->year,
                     ts->month, ts->day, ts->hour, ts->minute, ts->second, ts->fraction / 1000);
            valueStr = tsStr;
          } else {
            valueStr = "UNKNOWN_TYPE";
          }

          if (isCharacterType && ret == SQL_SUCCESS_WITH_INFO) {
            SQLLEN actualSize = 0;
            SQLGetDiagField(SQL_HANDLE_STMT, hStmt, 0, SQL_DIAG_COLUMN_SIZE, &actualSize,
                            SQL_IS_INTEGER, NULL);

            if (indicator > 0 && static_cast<size_t>(indicator) > bufferSize - 1) {
              valueStr += "...";
            }
          }
        }

        row.push_back(valueStr);
      }

      tableRows.push_back(row);
    }

    if (!tableRows.empty()) {
      PrintSimpleTable(columnNames, tableRows);
    }

    SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
  } catch (const std::exception& ex) {
    std::cerr << "Exception: " << ex.what() << std::endl;
    if (hStmt != SQL_NULL_HSTMT) {
      SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
    }
    throw;
  } catch (...) {
    std::cerr << "Unknown exception occurred" << std::endl;
    if (hStmt != SQL_NULL_HSTMT) {
      SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
    }
    throw;
  }
}

/// Execute a non-query SQL statement (e.g. CREATE DATABASE, CREATE TABLE, INSERT)
void Execute(SQLHDBC hDbc, const std::string& command) {
  SQLHSTMT hStmt = SQL_NULL_HSTMT;
  SQLRETURN ret;

  try {
    // Allocate statement handle
    ret = SQLAllocHandle(SQL_HANDLE_STMT, hDbc, &hStmt);
    CheckOdbcError(ret, SQL_HANDLE_DBC, hDbc, "SQLAllocHandle(SQL_HANDLE_STMT)");

    // Execute command
    ret = SQLExecDirect(hStmt, (SQLCHAR*)command.c_str(), SQL_NTS);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
      CheckOdbcError(ret, SQL_HANDLE_STMT, hStmt, "SQLExecDirect");
    }

    // Free statement handle
    SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
  } catch (...) {
    if (hStmt != SQL_NULL_HSTMT) {
      SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
    }
    throw;
  }
}

int main() {
  SQLHENV hEnv = SQL_NULL_HENV;
  SQLHDBC hDbc = SQL_NULL_HDBC;
  SQLRETURN ret;

  try {
    std::cout << "Start" << std::endl;

    // 1. Initialize ODBC environment
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &hEnv);
    CheckOdbcError(ret, SQL_HANDLE_ENV, hEnv, "SQLAllocHandle(SQL_HANDLE_ENV)");

    ret = SQLSetEnvAttr(hEnv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    CheckOdbcError(ret, SQL_HANDLE_ENV, hEnv, "SQLSetEnvAttr");

    // 2. Establish connection
    ret = SQLAllocHandle(SQL_HANDLE_DBC, hEnv, &hDbc);
    CheckOdbcError(ret, SQL_HANDLE_ENV, hEnv, "SQLAllocHandle(SQL_HANDLE_DBC)");

    // Connection string
    std::string dsn = "Apache IoTDB DSN";
    std::string user = "root";
    std::string password = "root";
    std::string server = "127.0.0.1";
    std::string database = "test";

    std::string connectionString = "DSN=" + dsn + ";Server=" + server + ";UID=" + user +
                                   ";PWD=" + password + ";Database=" + database + ";loglevel=4";
    std::cout << "Using connection string: " << connectionString << std::endl;

    SQLCHAR outConnStr[1024];
    SQLSMALLINT outConnStrLen;

    ret = SQLDriverConnect(hDbc, NULL, (SQLCHAR*)connectionString.c_str(), SQL_NTS, outConnStr,
                           sizeof(outConnStr), &outConnStrLen, SQL_DRIVER_COMPLETE);

    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
      std::cerr << "Login failed" << std::endl;
      CheckOdbcError(ret, SQL_HANDLE_DBC, hDbc, "SQLDriverConnect");
      return 1;
    }

    // Get driver name
    SQLCHAR driverName[256];
    SQLSMALLINT nameLength;
    ret = SQLGetInfo(hDbc, SQL_DRIVER_NAME, driverName, sizeof(driverName), &nameLength);
    CheckOdbcError(ret, SQL_HANDLE_DBC, hDbc, "SQLGetInfo");

    std::cout << "Successfully opened connection. database name = " << driverName << std::endl;

    // 3. Execute operations
    Execute(hDbc, "CREATE DATABASE IF NOT EXISTS test");
    Execute(hDbc, "use test");
    std::cout << "use test Execute complete. Begin to setup fulltable." << std::endl;

    // Create fulltable and insert test data
    Execute(hDbc,
            "CREATE TABLE IF NOT EXISTS fullTable (time TIMESTAMP TIME, bool_col BOOLEAN FIELD, "
            "int32_col INT32 FIELD, int64_col INT64 FIELD, float_col FLOAT FIELD, double_col "
            "DOUBLE FIELD, text_col TEXT FIELD, string_col STRING FIELD, blob_col BLOB FIELD, "
            "timestamp_col TIMESTAMP FIELD, date_col DATE FIELD) WITH (TTL=315360000000)");
    const char* insertStatements[] = {
        "INSERT INTO fulltable VALUES (1735689600000, true, 100, 10000000000, 36.5, 128.689, "
        "'Device running normally', 'DeviceA-Room1', '0x506C616E7444617461', 1735689600000, "
        "'2026-01-04')",
        "INSERT INTO fulltable VALUES (1735689660000, false, 101, 10000000001, 36.6, 128.789, "
        "'Device running normally', 'DeviceA-Room1', '0x506C616E7444617461', 1735689660000, "
        "'2026-01-04')",
        "INSERT INTO fulltable VALUES (1735689720000, true, 102, 10000000002, 36.7, 128.889, "
        "'Device running normally', 'DeviceA-Room1', '0x506C616E7444617461', 1735689720000, "
        "'2026-01-04')",
        "INSERT INTO fulltable VALUES (1735689780000, false, 103, 10000000003, 36.8, 128.989, "
        "'Device high temperature alert', 'DeviceA-Room1', '0x506C616E7444617462', 1735689780000, "
        "'2026-01-04')",
        "INSERT INTO fulltable VALUES (1735689840000, true, 104, 10000000004, 36.9, 129.089, "
        "'Device status restored', 'DeviceA-Room1', '0x506C616E7444617461', 1735689840000, "
        "'2026-01-04')",
        "INSERT INTO fulltable VALUES (1735689900000, false, 105, 10000000005, 37.0, 129.189, "
        "'Device running normally', 'DeviceB-Room2', '0x506C616E7444617463', 1735689900000, "
        "'2026-01-04')",
        "INSERT INTO fulltable VALUES (1735689960000, true, 106, 10000000006, 37.1, 129.289, "
        "'Device running normally', 'DeviceB-Room2', '0x506C616E7444617463', 1735689960000, "
        "'2026-01-04')",
        "INSERT INTO fulltable VALUES (1735690020000, false, 107, 10000000007, 37.2, 129.389, "
        "'Device low humidity alert', 'DeviceB-Room2', '0x506C616E7444617464', 1735690020000, "
        "'2026-01-04')",
        "INSERT INTO fulltable VALUES (1735690080000, true, 108, 10000000008, 37.3, 129.489, "
        "'Device status restored', 'DeviceB-Room2', '0x506C616E7444617463', 1735690080000, "
        "'2026-01-04')",
        "INSERT INTO fulltable VALUES (1735690140000, false, 109, 10000000009, 37.4, 129.589, "
        "'Device running normally', 'DeviceC-Room3', '0x506C616E7444617465', 1735690140000, "
        "'2026-01-04')",
        "INSERT INTO fulltable VALUES (1735690200000, true, 110, 10000000010, 37.5, 129.689, "
        "'Device running normally', 'DeviceC-Room3', '0x506C616E7444617465', 1735690200000, "
        "'2026-01-04')",
        "INSERT INTO fulltable VALUES (1735690260000, false, 111, 10000000011, 37.6, 129.789, "
        "'Device unstable voltage alert', 'DeviceC-Room3', '0x506C616E7444617466', 1735690260000, "
        "'2026-01-04')",
        "INSERT INTO fulltable VALUES (1735690320000, true, 112, 10000000012, 37.7, 129.889, "
        "'Device status restored', 'DeviceC-Room3', '0x506C616E7444617465', 1735690320000, "
        "'2026-01-04')",
        "INSERT INTO fulltable VALUES (1735690380000, false, 113, 10000000013, 37.8, 129.989, "
        "'Device running normally', 'DeviceD-Room4', '0x506C616E7444617467', 1735690380000, "
        "'2026-01-04')",
        "INSERT INTO fulltable VALUES (1735690440000, true, 114, 10000000014, 37.9, 130.089, "
        "'Device running normally', 'DeviceD-Room4', '0x506C616E7444617467', 1735690440000, "
        "'2026-01-04')",
        "INSERT INTO fulltable VALUES (1735690500000, false, 115, 10000000015, 38.0, 130.189, "
        "'Device running normally', 'DeviceD-Room4', '0x506C616E7444617467', 1735690500000, "
        "'2026-01-04')",
        "INSERT INTO fulltable VALUES (1735690560000, true, 116, 10000000016, 38.1, 130.289, "
        "'Device signal interrupted alert', 'DeviceD-Room4', '0x506C616E7444617468', "
        "1735690560000, '2026-01-04')",
        "INSERT INTO fulltable VALUES (1735690620000, false, 117, 10000000017, 38.2, 130.389, "
        "'Device running normally', 'DeviceE-Room5', '0x506C616E7444617469', 1735690620000, "
        "'2026-01-04')",
        "INSERT INTO fulltable VALUES (1735690680000, true, 118, 10000000018, 38.3, 130.489, "
        "'Device running normally', 'DeviceE-Room5', '0x506C616E7444617469', 1735690680000, "
        "'2026-01-04')",
        "INSERT INTO fulltable VALUES (1735690740000, false, 119, 10000000019, 38.4, 130.589, "
        "'Device running normally', 'DeviceE-Room5', '0x506C616E7444617469', 1735690740000, "
        "'2026-01-04')",
        "INSERT INTO fulltable VALUES (1735690790000, false, 119, 10000000019, 38.4, 130.589, "
        "'Device running normally', 'DeviceE-Room5', '0x506C616E7444617469', 1735690740000, "
        "'2026-01-04')"};
    for (const char* sql : insertStatements) {
      Execute(hDbc, sql);
    }
    std::cout << "fulltable setup complete. Begin to query." << std::endl;
    Query(hDbc);
    std::cout << "Query ok" << std::endl;

    // 4. Clean up resources
    SQLDisconnect(hDbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hDbc);
    SQLFreeHandle(SQL_HANDLE_ENV, hEnv);

    return 0;
  } catch (...) {
    // Exception cleanup
    if (hDbc != SQL_NULL_HDBC) {
      SQLDisconnect(hDbc);
      SQLFreeHandle(SQL_HANDLE_DBC, hDbc);
    }
    if (hEnv != SQL_NULL_HENV) {
      SQLFreeHandle(SQL_HANDLE_ENV, hEnv);
    }

    std::cerr << "Unexpected error!" << std::endl;
    return 1;
  }
}
