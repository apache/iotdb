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
#include "ConnectionHandle.h"
#include "ConnectionString.h"
#include "ODBCField.h"
#include "StatementUtils.h"
#include "driver.h"
#include <iostream>
#include <fstream>
#include <iterator>

static void checkAt(bool ok, int line) {
  if (!ok)
    throw std::runtime_error("ODBC unit assertion failed at line " + std::to_string(line));
}
#define check(condition) checkAt((condition), __LINE__)

static void checkDescriptorBindings() {
  ConnectionHandle connection(nullptr);
  StatementHandle statement(&connection);
  statement.AllocateSessionResultSet();
  statement.resultSetPtr->columnNames = {"v"};
  statement.resultSetPtr->columnTypes = {"INT32"};
  statement.resultSetPtr->data = {
      {ODBCField(int32_t(1))}, {ODBCField(int32_t(2))}, {ODBCField(int32_t(3))}};
  statement.resultSetPtr->numColumns = 1;
  statement.resultSetPtr->numRows = 3;
  SQLINTEGER values[4] = {-1, -1, -1, -1};
  SQLLEN lengths[2]{};
  SQLULEN fetched = 0;
  SQLUSMALLINT status[2]{};
  check(SQLSetDescField(statement.impRowDesc, 0, SQL_DESC_ROWS_PROCESSED_PTR, &fetched, 0) ==
        SQL_SUCCESS);
  check(SQLSetDescField(statement.impRowDesc, 0, SQL_DESC_ARRAY_STATUS_PTR, status, 0) ==
        SQL_SUCCESS);
  check(SQLSetDescField(statement.appRowDesc, 0, SQL_DESC_ARRAY_SIZE,
                        reinterpret_cast<SQLPOINTER>(2), 0) == SQL_SUCCESS);
  SQLULEN arraySize = 0;
  check(SQLGetStmtAttr(&statement, SQL_ATTR_ROW_ARRAY_SIZE, &arraySize, 0, nullptr) ==
            SQL_SUCCESS &&
        arraySize == 2);
  // BufferLength is ignored for fixed-width C types, including SQL_C_DEFAULT.
  check(SQLBindCol(&statement, 1, SQL_C_DEFAULT, values, sizeof(values), lengths) == SQL_SUCCESS);
  check(SQLFetch(&statement) == SQL_SUCCESS && fetched == 2 && values[0] == 1 && values[1] == 2 &&
        values[2] == -1 && status[1] == SQL_ROW_SUCCESS);
  check(SQLFreeStmt(&statement, SQL_UNBIND) == SQL_SUCCESS);
  check(SQLFetch(&statement) == SQL_SUCCESS && fetched == 1 && values[0] == 1 && values[1] == 2);

  statement.curRow = -1;
  check(SQLBindCol(&statement, 1, SQL_C_SLONG, values, 0, lengths) == SQL_SUCCESS);
  check(SQLSetDescField(statement.appRowDesc, 0, SQL_DESC_COUNT, nullptr, 0) == SQL_SUCCESS);
  values[0] = -1;
  check(SQLFetch(&statement) == SQL_SUCCESS && values[0] == -1);
  statement.curRow = -1;
  SQLLEN separateIndicators[2] = {-99, -99};
  check(SQLSetDescRec(statement.appRowDesc, 1, SQL_C_SLONG, 0, 0, 0, 0, values, lengths,
                      separateIndicators) == SQL_SUCCESS);
  check(SQLFetch(&statement) == SQL_SUCCESS && values[1] == 2 && lengths[1] == sizeof(SQLINTEGER) &&
        separateIndicators[1] == 0);
  statement.curRow = -1;
  statement.resultSetPtr->data[0][0] = ODBCField(std::string("not an integer"));
  check(SQLFetch(&statement) == SQL_SUCCESS_WITH_INFO && fetched == 2 &&
        status[0] == SQL_ROW_ERROR && status[1] == SQL_ROW_SUCCESS && values[1] == 2);
  check(SQLBindCol(&statement, 65535, SQL_C_CHAR, values, sizeof(values), nullptr) == SQL_ERROR);
  check(SQLBindParameter(&statement, 65535, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 10, 0,
                         values, 0, nullptr) == SQL_ERROR);

  SQLHANDLE descriptor = nullptr;
  check(SQLAllocHandle(SQL_HANDLE_DESC, &connection, &descriptor) == SQL_SUCCESS);
  StatementHandle other(&connection);
  check(SQLSetStmtAttr(&statement, SQL_ATTR_APP_ROW_DESC, descriptor, 0) == SQL_SUCCESS);
  check(SQLSetStmtAttr(&other, SQL_ATTR_APP_PARAM_DESC, descriptor, 0) == SQL_SUCCESS);
  check(SQLFreeHandle(SQL_HANDLE_DESC, descriptor) == SQL_SUCCESS);
  check(statement.appRowDesc == statement.implicitAppRowDesc &&
        other.appParamDesc == other.implicitAppParamDesc);
  check(SQLSetStmtAttr(&statement, SQL_ATTR_APP_ROW_DESC, other.appRowDesc, 0) == SQL_ERROR);
  check(SQLSetStmtAttr(&statement, SQL_ATTR_APP_PARAM_DESC, statement.impParamDesc, 0) ==
        SQL_ERROR);

  SQLCHAR sql[] = "insert into t values (?)";
  check(SQLPrepare(&statement, sql, SQL_NTS) == SQL_SUCCESS);
  SQLLEN indicator = SQL_DATA_AT_EXEC;
  check(SQLBindParameter(&statement, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 10, 0, values,
                         sizeof(values), &indicator) == SQL_SUCCESS);
  check(SQLExecute(&statement) == SQL_NEED_DATA);
  check(SQLCancel(&statement) == SQL_SUCCESS);
  check(SQLSetDescField(statement.appParamDesc, 0, SQL_DESC_COUNT, nullptr, 0) == SQL_SUCCESS);
  check(SQLExecute(&statement) == SQL_ERROR);
  std::string state, message;
  int native = 0;
  check(statement.getDiagnostic(1, state, message, native) && state == "07002");
  indicator = -10;
  check(SQLBindParameter(&statement, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 10, 0, values,
                         sizeof(values), &indicator) == SQL_SUCCESS);
  check(SQLExecute(&statement) == SQL_ERROR);
  check(statement.getDiagnostic(1, state, message, native) && state == "HY090");
}

static bool invalid(const ConnectionHandle& c) {
  try {
    c.ValidateTransport();
    return false;
  } catch (const std::invalid_argument&) {
    return true;
  }
}
int main() {
  try {
    checkDescriptorBindings();
    ConnectionHandle c(nullptr);
    check(!invalid(c));
    SetConnectionHandle(&c, "SSL", "true");
    check(invalid(c));
    SetConnectionHandle(&c, "sSlCa", "/certs/ca.pem");
    check(!invalid(c));
    SetConnectionHandle(&c, "SSLCERT", "/certs/client.pem");
    check(invalid(c));
    SetConnectionHandle(&c, "SSLKEY", "/certs/client.key");
    check(!invalid(c));
    SetConnectionHandle(&c, "RESTFUL", "1");
    check(invalid(c));
    SetConnectionHandle(&c, "RESTFUL", "0");
    SetConnectionHandle(&c, "PORT", "70000");
    check(invalid(c));
    SetConnectionHandle(&c, "PORT", "6667");
    SetConnectionHandle(&c, "SSL", "garbage");
    check(invalid(c));
    SetConnectionHandle(&c, "SSL", "0");
    SetConnectionHandle(&c, "RESTFUL", "garbage");
    check(invalid(c));
    SetConnectionHandle(&c, "RESTFUL", "0");
    check(IsQueryStatement("  SELECT * FROM t"));
    check(IsQueryStatement("show version"));
    check(IsQueryStatement("-- comment\n /* comment */ SELECT/* hint */1"));
    check(!IsQueryStatement("/* comment only */"));
    check(!IsQueryStatement("selection"));
    check(!IsQueryStatement("insert into t values (1)"));

    SQLHANDLE env = nullptr;
    check(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, nullptr) == SQL_ERROR);
    check(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env) == SQL_SUCCESS && env);
    SQLHANDLE dbc = nullptr;
    check(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc) == SQL_SUCCESS && dbc);
    check(SQLFreeHandle(SQL_HANDLE_ENV, dbc) == SQL_INVALID_HANDLE);
    check(SQLFreeHandle(SQL_HANDLE_DBC, dbc) == SQL_SUCCESS);
    check(SQLFreeHandle(SQL_HANDLE_ENV, env) == SQL_SUCCESS);

    SQLUINTEGER functions = 0;
    SQLSMALLINT infoLength = 0;
    check(SQLGetInfo(&c, SQL_NUMERIC_FUNCTIONS, &functions, sizeof(functions), &infoLength) ==
          SQL_SUCCESS);
    check(functions == 0 && infoLength == sizeof(SQLUINTEGER));
    functions = 0;
    check(SQLGetInfo(&c, SQL_TIMEDATE_FUNCTIONS, &functions, sizeof(functions), &infoLength) ==
          SQL_SUCCESS);
    check(functions == 0);
    SQLUSMALLINT supported = SQL_FALSE;
    check(SQLGetFunctions(&c, SQL_API_SQLNATIVESQL, &supported) == SQL_SUCCESS &&
          supported == SQL_TRUE);
    check(SQLGetFunctions(&c, SQL_API_SQLBINDPARAM, &supported) == SQL_SUCCESS &&
          supported == SQL_TRUE);
    check(SQLGetFunctions(&c, SQL_API_SQLBINDPARAMETER, &supported) == SQL_SUCCESS &&
          supported == SQL_TRUE);
    check(SQLGetFunctions(&c, SQL_API_SQLDRIVERS, &supported) == SQL_SUCCESS &&
          supported == SQL_TRUE);
#ifdef SQL_API_SQLCOMPLETEASYNC
    check(SQLGetFunctions(&c, SQL_API_SQLCOMPLETEASYNC, &supported) == SQL_SUCCESS &&
          supported == SQL_FALSE);
#endif
#ifdef SQL_API_SQLCANCELHANDLE
    check(SQLGetFunctions(&c, SQL_API_SQLCANCELHANDLE, &supported) == SQL_SUCCESS &&
          supported == SQL_TRUE);
#endif
    check(ParameterMarkerPositions("select '?', ? -- ?\n, \"?\", ? /* ? */").size() == 2);
    check(ParameterMarkerPositions("select `?`, `a``?`, ?, 'it''s ?'").size() == 1);
    SQLCHAR nativeSql[8]{};
    SQLINTEGER nativeSqlLength = 0;
    SQLCHAR inputSql[] = "SELECT 1";
    check(SQLNativeSql(&c, inputSql, SQL_NTS, nativeSql, sizeof(nativeSql), &nativeSqlLength) ==
          SQL_SUCCESS_WITH_INFO);
    check(nativeSqlLength == 8 && std::string(reinterpret_cast<char*>(nativeSql)) == "SELECT ");
    SQLCHAR stateBuffer[6]{};
    SQLCHAR emptyMessageBuffer[1]{};
    check(SQLGetDiagRec(SQL_HANDLE_DBC, &c, 1, stateBuffer, nullptr, emptyMessageBuffer, 0,
                        nullptr) == SQL_SUCCESS_WITH_INFO);
    check(SQLGetDiagRec(SQL_HANDLE_DBC, &c, 1, stateBuffer, nullptr, nullptr, 0, nullptr) ==
          SQL_SUCCESS);
    check(std::string(reinterpret_cast<char*>(stateBuffer)) == "01004");
    SQLCHAR driverName[5]{};
    check(SQLGetInfo(&c, SQL_DRIVER_NAME, driverName, sizeof(driverName), &infoLength) ==
          SQL_SUCCESS_WITH_INFO);
    check(infoLength > 4);
    SQLUINTEGER optionValue = 99;
    check(SQLGetConnectOption(&c, SQL_AUTOCOMMIT, &optionValue) == SQL_SUCCESS &&
          optionValue == SQL_AUTOCOMMIT_ON);
    check(SQLSetConnectOption(&c, SQL_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF) == SQL_ERROR);
    check(SQLSetConnectOption(&c, SQL_LOGIN_TIMEOUT, 7) == SQL_SUCCESS);
    check(SQLGetConnectOption(&c, SQL_LOGIN_TIMEOUT, &optionValue) == SQL_SUCCESS &&
          optionValue == 7);
    StatementHandle statement(&c);
    check(SQLCancelHandle(SQL_HANDLE_STMT, &statement) == SQL_SUCCESS);
    check(SQLCancelHandle(SQL_HANDLE_DBC, &c) == SQL_SUCCESS);
    int optionNative = 0;
    std::string statementState, statementMessage;
    SQLHANDLE implicitArd = nullptr;
    SQLHANDLE implicitApd = nullptr;
    check(SQLGetStmtAttr(&statement, SQL_ATTR_APP_ROW_DESC, &implicitArd, 0, nullptr) ==
              SQL_SUCCESS &&
          implicitArd != nullptr);
    check(SQLGetStmtAttr(&statement, SQL_ATTR_APP_PARAM_DESC, &implicitApd, 0, nullptr) ==
              SQL_SUCCESS &&
          implicitApd != nullptr && implicitApd != implicitArd);
    check(SQLFreeHandle(SQL_HANDLE_DESC, implicitArd) == SQL_ERROR);
    SQLHANDLE descriptor = nullptr;
    SQLHANDLE descriptorCopy = nullptr;
    check(SQLAllocHandle(SQL_HANDLE_DESC, &c, &descriptor) == SQL_SUCCESS);
    check(SQLAllocHandle(SQL_HANDLE_DESC, &c, &descriptorCopy) == SQL_SUCCESS);
    SQLLEN descriptorLength = 32;
    SQLCHAR descriptorBuffer[32]{};
    check(SQLSetDescRec(descriptor, 1, SQL_C_CHAR, 0, descriptorLength, 0, 0, descriptorBuffer,
                        &descriptorLength, nullptr) == SQL_SUCCESS);
    SQLSMALLINT descriptorCount = 0;
    check(SQLGetDescField(descriptor, 0, SQL_DESC_COUNT, &descriptorCount, 0, nullptr) ==
              SQL_SUCCESS &&
          descriptorCount == 1);
    check(SQLCopyDesc(descriptor, descriptorCopy) == SQL_SUCCESS);
    check(SQLSetDescRec(descriptorCopy, 1, SQL_DATETIME, SQL_CODE_TIMESTAMP,
                        sizeof(SQL_TIMESTAMP_STRUCT), 0, 0, nullptr, nullptr,
                        nullptr) == SQL_SUCCESS);
    SQLSMALLINT conciseType = 0;
    check(SQLGetDescField(descriptorCopy, 1, SQL_DESC_CONCISE_TYPE, &conciseType, 0, nullptr) ==
              SQL_SUCCESS &&
          conciseType == SQL_C_TYPE_TIMESTAMP);
    check(SQLSetStmtAttr(&statement, SQL_ATTR_APP_ROW_DESC, descriptor, 0) == SQL_SUCCESS);
    check(SQLSetStmtAttr(&statement, SQL_ATTR_APP_ROW_DESC, nullptr, 0) == SQL_SUCCESS);
    check(SQLSetStmtAttr(&statement, SQL_ATTR_ROW_ARRAY_SIZE, reinterpret_cast<SQLPOINTER>(1), 0) ==
          SQL_SUCCESS);
    check(SQLSetStmtAttr(&statement, SQL_ATTR_ROW_ARRAY_SIZE, reinterpret_cast<SQLPOINTER>(2), 0) ==
          SQL_SUCCESS);
    SQLULEN rowArraySize = 0;
    check(SQLGetStmtAttr(&statement, SQL_ATTR_ROW_ARRAY_SIZE, &rowArraySize, 0, nullptr) ==
              SQL_SUCCESS &&
          rowArraySize == 2);
    statement.AllocateSessionResultSet();
    statement.resultSetPtr->columnNames = {"v"};
    statement.resultSetPtr->columnTypes = {"INT32"};
    statement.resultSetPtr->data = {
        {ODBCField(int32_t(1))}, {ODBCField(int32_t(2))}, {ODBCField(int32_t(3))}};
    statement.resultSetPtr->numColumns = 1;
    statement.resultSetPtr->numRows = 3;
    SQLINTEGER rowValues[2]{};
    SQLLEN rowLengths[2]{};
    SQLULEN rowsFetched = 0;
    SQLUSMALLINT rowStatus[2]{};
    check(SQLBindCol(&statement, 1, SQL_C_SLONG, rowValues, sizeof(SQLINTEGER), rowLengths) ==
          SQL_SUCCESS);
    check(SQLSetStmtAttr(&statement, SQL_ATTR_ROWS_FETCHED_PTR, &rowsFetched, 0) == SQL_SUCCESS);
    check(SQLSetStmtAttr(&statement, SQL_ATTR_ROW_STATUS_PTR, rowStatus, 0) == SQL_SUCCESS);
    check(SQLFetch(&statement) == SQL_SUCCESS && rowsFetched == 2 && rowValues[0] == 1 &&
          rowValues[1] == 2 && rowStatus[0] == SQL_ROW_SUCCESS && rowStatus[1] == SQL_ROW_SUCCESS);
    check(SQLCloseCursor(&statement) == SQL_SUCCESS);
    check(SQLSetStmtAttr(&statement, SQL_ATTR_ROW_ARRAY_SIZE, reinterpret_cast<SQLPOINTER>(1), 0) ==
          SQL_SUCCESS);
    check(SQLSetStmtAttr(&statement, SQL_ATTR_PARAMSET_SIZE, reinterpret_cast<SQLPOINTER>(2), 0) ==
          SQL_SUCCESS);
    SQLULEN parameterSetSize = 0;
    check(SQLGetStmtAttr(&statement, SQL_ATTR_PARAMSET_SIZE, &parameterSetSize, 0, nullptr) ==
              SQL_SUCCESS &&
          parameterSetSize == 2);
    check(SQLSetStmtAttr(&statement, SQL_ATTR_PARAMSET_SIZE, reinterpret_cast<SQLPOINTER>(1), 0) ==
          SQL_SUCCESS);
    SQLCHAR preparedSql[] = "insert into t values (?, '?', ?)";
    check(SQLPrepare(&statement, preparedSql, SQL_NTS) == SQL_SUCCESS);
    SQLSMALLINT parameterCount = 0;
    check(SQLNumParams(&statement, &parameterCount) == SQL_SUCCESS && parameterCount == 2);
    SQLLEN dataAtExecution = SQL_DATA_AT_EXEC;
    char parameterToken = 0;
    check(SQLBindParameter(&statement, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 16, 0,
                           &parameterToken, 16, &dataAtExecution) == SQL_SUCCESS);
    SQLINTEGER integerParameter = 7;
    check(SQLBindParameter(&statement, 2, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 10, 0,
                           &integerParameter, 0, nullptr) == SQL_SUCCESS);
    check(SQLExecute(&statement) == SQL_NEED_DATA);
    SQLPOINTER requestedParameter = nullptr;
    check(SQLParamData(&statement, &requestedParameter) == SQL_NEED_DATA &&
          requestedParameter == &parameterToken);
    check(SQLPutData(&statement, const_cast<char*>("streamed"), SQL_NTS) == SQL_SUCCESS);
    check(SQLCancel(&statement) == SQL_SUCCESS);
    check(SQLParamData(&statement, &requestedParameter) == SQL_ERROR);

    SQLLEN arrayDataAtExecution[2] = {SQL_DATA_AT_EXEC, SQL_DATA_AT_EXEC};
    char parameterTokens[32]{};
    SQLINTEGER integerParameters[2] = {7, 8};
    check(SQLSetStmtAttr(&statement, SQL_ATTR_PARAMSET_SIZE, reinterpret_cast<SQLPOINTER>(2), 0) ==
          SQL_SUCCESS);
    check(SQLBindParameter(&statement, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 16, 0,
                           parameterTokens, 16, arrayDataAtExecution) == SQL_SUCCESS);
    check(SQLBindParameter(&statement, 2, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 10, 0,
                           integerParameters, 0, nullptr) == SQL_SUCCESS);
    check(SQLExecute(&statement) == SQL_NEED_DATA);
    check(SQLParamData(&statement, &requestedParameter) == SQL_NEED_DATA &&
          requestedParameter == parameterTokens);
    check(SQLPutData(&statement, const_cast<char*>("first"), SQL_NTS) == SQL_SUCCESS);
    check(SQLParamData(&statement, &requestedParameter) == SQL_NEED_DATA &&
          requestedParameter == parameterTokens + 16);
    check(SQLPutData(&statement, const_cast<char*>("second"), SQL_NTS) == SQL_SUCCESS);
    check(statement.parameterBindings[0].streamedData[0] == "first" &&
          statement.parameterBindings[0].streamedData[1] == "second");
    check(SQLCancel(&statement) == SQL_SUCCESS);
    check(SQLFreeStmt(&statement, SQL_RESET_PARAMS) == SQL_SUCCESS);
    check(SQLSpecialColumns(&statement, SQL_BEST_ROWID, nullptr, 0, nullptr, 0, nullptr, 0,
                            SQL_SCOPE_SESSION, SQL_NULLABLE) == SQL_SUCCESS);
    SQLSMALLINT catalogColumns = 0;
    check(SQLNumResultCols(&statement, &catalogColumns) == SQL_SUCCESS && catalogColumns == 8);
    check(SQLStatistics(&statement, nullptr, 0, nullptr, 0, nullptr, 0, SQL_INDEX_ALL, SQL_QUICK) ==
          SQL_SUCCESS);
    check(SQLNumResultCols(&statement, &catalogColumns) == SQL_SUCCESS && catalogColumns == 13);
    SQLCHAR typeName[16]{};
    SQLSMALLINT typeNameLength = 0;
    check(SQLColAttribute(&statement, 2, SQL_DESC_TYPE_NAME, nullptr, 0, &typeNameLength,
                          nullptr) == SQL_SUCCESS &&
          typeNameLength == 4);
    check(SQLColAttribute(&statement, 2, SQL_DESC_TYPE_NAME, typeName, sizeof(typeName), nullptr,
                          nullptr) == SQL_SUCCESS &&
          std::string(reinterpret_cast<char*>(typeName)) == "TEXT");
    check(SQLFreeStmt(&statement, SQL_UNBIND) == SQL_SUCCESS);
    check(SQLGetTypeInfo(&statement, SQL_ALL_TYPES) == SQL_SUCCESS);
    SQLSMALLINT catalogType = 0;
    check(SQLDescribeCol(&statement, 2, nullptr, 0, nullptr, &catalogType, nullptr, nullptr,
                         nullptr) == SQL_SUCCESS &&
          catalogType == SQL_SMALLINT);
    check(SQLFetch(&statement) == SQL_SUCCESS);
    SQLCHAR firstType[32]{};
    SQLLEN firstTypeLength = 0;
    check(SQLGetData(&statement, 1, SQL_C_CHAR, firstType, sizeof(firstType), &firstTypeLength) ==
          SQL_SUCCESS);
    check(SQLGetTypeInfo(&statement, SQL_ALL_TYPES) == SQL_SUCCESS);
    check(SQLFetch(&statement) == SQL_SUCCESS);
    SQLCHAR refreshedType[32]{};
    check(SQLGetData(&statement, 1, SQL_C_CHAR, refreshedType, sizeof(refreshedType),
                     &firstTypeLength) == SQL_SUCCESS &&
          std::string(reinterpret_cast<char*>(firstType)) ==
              std::string(reinterpret_cast<char*>(refreshedType)));
    check(SQLSetStmtAttr(&statement, SQL_ATTR_CURSOR_TYPE,
                         reinterpret_cast<SQLPOINTER>(SQL_CURSOR_STATIC), 0) == SQL_ERROR);
    check(SQLCloseCursor(&statement) == SQL_SUCCESS);
    check(SQLCloseCursor(&statement) == SQL_ERROR);
    check(statement.getDiagnostic(1, statementState, statementMessage, optionNative) &&
          statementState == "24000");
    check(SQLEndTran(SQL_HANDLE_DBC, &c, SQL_ROLLBACK) == SQL_ERROR);
    check(c.getDiagnostic(1, statementState, statementMessage, optionNative) &&
          statementState == "HYC00");
    check(SQLFreeHandle(SQL_HANDLE_DESC, descriptorCopy) == SQL_SUCCESS);
    check(SQLFreeHandle(SQL_HANDLE_DESC, descriptor) == SQL_SUCCESS);
    auto parsed = ParseConnectionString(
        " Driver={Apache IoTDB ODBC Driver};PWD={a;b}}c};SSLCA={C:\\certs\\ca.pem};SSL=1;");
    check(parsed.size() == 4 && parsed[1].second == "a;b}c");
    try {
      ParseConnectionString("SSLCA={broken");
      check(false);
    } catch (const std::invalid_argument&) {
    }
    SQLCHAR output[64]{};
    SQLLEN size = 0;
    ODBCField date(IoTDBDate(2024, 2, 29));
    check(date.toChar(output, sizeof(output), &size));
    check(std::string(reinterpret_cast<char*>(output)) == "2024-02-29");
    SQL_DATE_STRUCT d{};
    check(date.toDate(&d) && d.year == 2024 && d.month == 2 && d.day == 29);
    ODBCField null = ODBCField::null();
    check(null.toChar(output, sizeof(output), &size) && size == SQL_NULL_DATA);
    ODBCField unicode(std::string("\xe4\xb8\xad\xe6\x96\x87"));
    SQLWCHAR wide[16]{};
    check(unicode.toWChar(wide, sizeof(wide), &size));
    check(size == 4 && wide[0] == 0x4e2d && wide[1] == 0x6587 && wide[2] == 0);
    Field timestamp;
    timestamp.dataType = TSDataType::TIMESTAMP;
    timestamp.longV = int64_t(1234);
    SQL_TIMESTAMP_STRUCT ts{};
    check(ODBCField(timestamp).toTimestamp(&ts));
    check(ts.year == 1970 && ts.second == 1 && ts.fraction == 234000000);
    timestamp.longV = int64_t(-1);
    check(ODBCField(timestamp).toTimestamp(&ts));
    check(ts.year == 1969 && ts.month == 12 && ts.day == 31 && ts.hour == 23 && ts.minute == 59 &&
          ts.second == 59 && ts.fraction == 999000000);
    // This fails before network I/O and exercises the public diagnostic path.
    ConnectionHandle bad(nullptr);
    bad.addDiagnostic("HY000", "Stale error from a previous connection attempt");
    std::string connection = "LOGLEVEL=4;SSL=1;SSLCA=x;RESTFUL=1;PWD=do-not-log";
    check(SQLDriverConnect(&bad, nullptr, reinterpret_cast<SQLCHAR*>(&connection[0]),
                           static_cast<SQLSMALLINT>(connection.size()), nullptr, 0, nullptr,
                           SQL_DRIVER_NOPROMPT) == SQL_ERROR);
    std::string state, message;
    int native = 0;
    check(bad.getDiagnostic(1, state, message, native) && state == "08001");
    SQLCHAR sqlState[6]{}, errorMessage[512]{};
    SQLINTEGER code = 0;
    SQLSMALLINT errorLength = 0;
    check(SQLError(nullptr, &bad, nullptr, sqlState, &code, errorMessage, sizeof(errorMessage),
                   &errorLength) == SQL_SUCCESS);
    check(SQLError(nullptr, &bad, nullptr, sqlState, &code, errorMessage, sizeof(errorMessage),
                   &errorLength) == SQL_NO_DATA);
#ifndef WIN32
    ConnectionHandle dsn(nullptr);
    std::string dsnString = "DSN=ODBC_TLS_TEST;SSLCA={override;ca.pem};RESTFUL=1;";
    check(SQLDriverConnect(&dsn, nullptr, reinterpret_cast<SQLCHAR*>(&dsnString[0]), SQL_NTS,
                           nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT) == SQL_ERROR);
    check(dsn.sslConfig.useSsl && dsn.sslConfig.trustCertFilePath == "override;ca.pem" &&
          dsn.sslConfig.clientCertificateFilePath == "dsn-client.pem");
#endif
    std::ifstream log("apache_iotdb_odbc.log");
    std::string logText((std::istreambuf_iterator<char>(log)), std::istreambuf_iterator<char>());
    check(logText.find("do-not-log") == std::string::npos);
    std::cout << "ODBC configuration, conversion and diagnostic tests passed\n";
  } catch (const std::exception& e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
