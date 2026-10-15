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

static bool hasState(ODBCHandle& handle, const std::string& expected) {
  std::string state, message;
  int native = 0;
  return handle.getDiagnostic(1, state, message, native) && state == expected;
}

static void checkReviewedMetadata() {
  ConnectionHandle connection(nullptr);
  StatementHandle statement(&connection);
  statement.AllocateSessionResultSet();
  statement.resultSetPtr->columnNames = {"long_label", "flag", "large"};
  statement.resultSetPtr->columnTypes = {"STRING", "BOOLEAN", "INT64"};
  statement.resultSetPtr->numColumns = 3;
  SQLULEN size = 0;
  check(SQLDescribeCol(&statement, 1, nullptr, 0, nullptr, nullptr, &size, nullptr, nullptr) ==
            SQL_SUCCESS &&
        size == 2147483647);
  const SQLSMALLINT types[] = {SQL_WVARCHAR, SQL_BIT, SQL_BIGINT};
  for (SQLUSMALLINT col = 1; col <= 3; ++col) {
    SQLLEN type = 0x12345678;
    check(SQLColAttribute(&statement, col, SQL_DESC_CONCISE_TYPE, nullptr, 0, nullptr, &type) ==
              SQL_SUCCESS &&
          type == types[col - 1]);
    check(SQLColAttribute(&statement, col, SQL_DESC_TYPE, nullptr, 0, nullptr, &type) ==
              SQL_SUCCESS &&
          type == types[col - 1]);
  }
  SQLSMALLINT length = 0;
  check(SQLColAttribute(&statement, 1, SQL_DESC_LABEL, nullptr, 0, &length, nullptr) ==
            SQL_SUCCESS &&
        length == 10);
  char sentinel[8];
  std::memset(sentinel, 'X', sizeof(sentinel));
  check(SQLColAttribute(&statement, 1, SQL_DESC_LABEL, sentinel, -1, &length, nullptr) ==
            SQL_ERROR &&
        hasState(statement, "HY090"));
  for (char byte : sentinel)
    check(byte == 'X');
  check(SQLDescribeCol(&statement, 1, reinterpret_cast<SQLCHAR*>(sentinel), -1, &length, nullptr,
                       nullptr, nullptr, nullptr) == SQL_ERROR &&
        hasState(statement, "HY090"));
  for (char byte : sentinel)
    check(byte == 'X');
  SQLSMALLINT type = 0;
  check(SQLDescribeCol(&statement, 1, reinterpret_cast<SQLCHAR*>(sentinel), 4, &length, &type,
                       &size, nullptr, nullptr) == SQL_SUCCESS_WITH_INFO &&
        std::string(sentinel) == "lon" && length == 10 && type == SQL_WVARCHAR &&
        size == 2147483647 && hasState(statement, "01004"));
  check(SQLColAttribute(&statement, 1, SQL_DESC_LABEL, sentinel, 1, &length, nullptr) ==
            SQL_SUCCESS_WITH_INFO &&
        sentinel[0] == 0 && length == 10 && hasState(statement, "01004"));
  check(SQLColAttribute(&statement, 1, SQL_DESC_NAME, sentinel, 0, &length, nullptr) ==
            SQL_SUCCESS_WITH_INFO &&
        hasState(statement, "01004"));
  SQLWCHAR wide[3]{};
  check(SQLDescribeColW(&statement, 1, wide, 3, &length, nullptr, &size, nullptr, nullptr) ==
            SQL_SUCCESS_WITH_INFO &&
        wide[0] == 'l' && wide[1] == 'o' && wide[2] == 0 && size == 2147483647 && length == 10 &&
        hasState(statement, "01004"));
  check(SQLColAttributeW(&statement, 1, SQL_DESC_LABEL, nullptr, 0, &length, nullptr) ==
            SQL_SUCCESS &&
        length == 10 * sizeof(SQLWCHAR));
  char quote[4]{};
  connection.isTableModel = false;
  check(SQLGetInfo(&connection, SQL_IDENTIFIER_QUOTE_CHAR, quote, sizeof(quote), &length) ==
            SQL_SUCCESS &&
        std::string(quote) == "`" && length == 1);
  connection.isTableModel = true;
  check(SQLGetInfo(&connection, SQL_IDENTIFIER_QUOTE_CHAR, quote, sizeof(quote), &length) ==
            SQL_SUCCESS &&
        std::string(quote) == "\"");
}

static uint64_t numericMagnitude(const SQL_NUMERIC_STRUCT& value) {
  uint64_t result = 0;
  for (int i = 7; i >= 0; --i)
    result = (result << 8) | value.val[i];
  for (int i = 8; i < SQL_MAX_NUMERIC_LEN; ++i)
    check(value.val[i] == 0);
  return result;
}

static void checkReviewedNumeric() {
  SQL_NUMERIC_STRUCT value{};
  check(ODBCField(std::numeric_limits<int64_t>::min()).toNumeric(&value) && value.sign == 0 &&
        numericMagnitude(value) == (uint64_t(1) << 63));
  check(ODBCField(std::numeric_limits<int64_t>::max()).toNumeric(&value) && value.sign == 1 &&
        numericMagnitude(value) == static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
  check(ODBCField(std::numeric_limits<int32_t>::min()).toNumeric(&value) && value.sign == 0 &&
        numericMagnitude(value) == (uint64_t(1) << 31));
  ConnectionHandle connection(nullptr);
  StatementHandle statement(&connection);
  statement.AllocateSessionResultSet();
  statement.resultSetPtr->columnNames = {"n"};
  statement.resultSetPtr->columnTypes = {"INT64"};
  statement.resultSetPtr->numColumns = 1;
  statement.resultSetPtr->numRows = 1;
  statement.resultSetPtr->data = {{ODBCField(int64_t(123))}};
  statement.curRow = 0;
  SQLLEN length = 0;
  check(SQLGetData(&statement, 1, SQL_C_NUMERIC, &value, sizeof(value), &length) == SQL_SUCCESS &&
        value.scale == 0 && value.precision == 38 && numericMagnitude(value) == 123 &&
        length == sizeof(value));
  check(SQLBindCol(&statement, 1, SQL_C_NUMERIC, &value, sizeof(value), &length) == SQL_SUCCESS);
  check(SQLSetDescField(statement.appRowDesc, 1, SQL_DESC_PRECISION,
                        reinterpret_cast<SQLPOINTER>(5), 0) == SQL_SUCCESS);
  check(SQLSetDescField(statement.appRowDesc, 1, SQL_DESC_SCALE, reinterpret_cast<SQLPOINTER>(2),
                        0) == SQL_SUCCESS);
  check(SQLGetData(&statement, 1, SQL_ARD_TYPE, &value, sizeof(value), &length) == SQL_SUCCESS &&
        value.scale == 2 && value.precision == 5 && numericMagnitude(value) == 12300);
  check(SQLGetData(&statement, 1, SQL_C_NUMERIC, &value, sizeof(value), &length) == SQL_SUCCESS &&
        value.scale == 0 && numericMagnitude(value) == 123);
  statement.curRow = -1;
  check(SQLSetDescField(statement.appRowDesc, 1, SQL_DESC_DATA_PTR, &value, 0) == SQL_SUCCESS);
  check(SQLFetch(&statement) == SQL_SUCCESS && value.scale == 2 &&
        numericMagnitude(value) == 12300);
  check(SQLSetDescField(statement.appRowDesc, 1, SQL_DESC_SCALE, reinterpret_cast<SQLPOINTER>(-1),
                        0) == SQL_SUCCESS);
  check(SQLGetData(&statement, 1, SQL_ARD_TYPE, &value, sizeof(value), &length) ==
            SQL_SUCCESS_WITH_INFO &&
        value.scale == -1 && numericMagnitude(value) == 12 && hasState(statement, "01S07"));
  check(SQLSetDescField(statement.appRowDesc, 1, SQL_DESC_PRECISION,
                        reinterpret_cast<SQLPOINTER>(2), 0) == SQL_SUCCESS);
  check(SQLSetDescField(statement.appRowDesc, 1, SQL_DESC_SCALE, nullptr, 0) == SQL_SUCCESS);
  check(SQLGetData(&statement, 1, SQL_ARD_TYPE, &value, sizeof(value), &length) == SQL_ERROR &&
        hasState(statement, "22003"));
  check(SQLFreeStmt(&statement, SQL_UNBIND) == SQL_SUCCESS);
  for (const auto& approximate :
       {ODBCField(1.5), ODBCField(float(-1.5)), ODBCField(std::numeric_limits<double>::infinity()),
        ODBCField(std::numeric_limits<double>::quiet_NaN())}) {
    statement.resultSetPtr->data = {{approximate}};
    std::memset(&value, 0x5a, sizeof(value));
    auto unchanged = value;
    check(SQLGetData(&statement, 1, SQL_C_NUMERIC, &value, sizeof(value), &length) == SQL_ERROR &&
          hasState(statement, "07006") && std::memcmp(&value, &unchanged, sizeof(value)) == 0);
    check(SQLSetDescField(statement.appRowDesc, 1, SQL_DESC_CONCISE_TYPE,
                          reinterpret_cast<SQLPOINTER>(SQL_C_NUMERIC), 0) == SQL_SUCCESS);
    check(SQLSetDescField(statement.appRowDesc, 1, SQL_DESC_SCALE, reinterpret_cast<SQLPOINTER>(1),
                          0) == SQL_SUCCESS);
    check(SQLGetData(&statement, 1, SQL_ARD_TYPE, &value, sizeof(value), &length) == SQL_ERROR &&
          hasState(statement, "07006") && std::memcmp(&value, &unchanged, sizeof(value)) == 0);
  }
  check(!ODBCField::canConvert(TSDataType::DOUBLE, SQL_C_NUMERIC));
  check(!ODBCField::canConvert(TSDataType::FLOAT, SQL_C_NUMERIC));
}

static void checkReviewedHandleLifetime() {
  SQLHANDLE env = nullptr, dbc = nullptr, first = nullptr, second = nullptr, descriptor = nullptr;
  check(SQLAllocHandle(SQL_HANDLE_ENV, nullptr, &env) == SQL_SUCCESS);
  check(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc) == SQL_SUCCESS);
  check(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &first) == SQL_SUCCESS);
  check(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &second) == SQL_SUCCESS);
  check(SQLAllocHandle(SQL_HANDLE_DESC, dbc, &descriptor) == SQL_SUCCESS);
  check(SQLSetStmtAttr(first, SQL_ATTR_APP_ROW_DESC, descriptor, 0) == SQL_SUCCESS);
  check(SQLSetStmtAttr(second, SQL_ATTR_APP_PARAM_DESC, descriptor, 0) == SQL_SUCCESS);
  auto implicit = static_cast<StatementHandle*>(first)->impRowDesc;
  auto* connection = static_cast<ConnectionHandle*>(dbc);
  check(connection->statements.size() == 2 && connection->descriptors.size() == 9);
  std::weak_ptr<ODBCResultSet> result;
  static_cast<StatementHandle*>(first)->AllocateSessionResultSet();
  result = static_cast<StatementHandle*>(first)->resultSetPtr;
  check(SQLDisconnect(dbc) == SQL_SUCCESS && result.expired());
  check(connection->statements.empty() && connection->descriptors.empty());
  check(!ODBCHandle::isLiveHandle(implicit, SQL_HANDLE_DESC));
  check(SQLFreeHandle(SQL_HANDLE_DBC, dbc) == SQL_SUCCESS);
  check(SQLFreeHandle(SQL_HANDLE_STMT, first) == SQL_INVALID_HANDLE);
  check(SQLFreeHandle(SQL_HANDLE_STMT, second) == SQL_INVALID_HANDLE);
  check(SQLFreeHandle(SQL_HANDLE_DESC, descriptor) == SQL_INVALID_HANDLE);
  check(SQLFreeHandle(SQL_HANDLE_DESC, implicit) == SQL_INVALID_HANDLE);
  check(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc) == SQL_SUCCESS);
  check(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &first) == SQL_SUCCESS);
  check(SQLAllocHandle(SQL_HANDLE_DESC, dbc, &descriptor) == SQL_SUCCESS);
  check(SQLFreeHandle(SQL_HANDLE_DBC, dbc) == SQL_SUCCESS);
  check(SQLFreeHandle(SQL_HANDLE_STMT, first) == SQL_INVALID_HANDLE);
  check(SQLFreeHandle(SQL_HANDLE_DESC, descriptor) == SQL_INVALID_HANDLE);
  check(SQLFreeHandle(SQL_HANDLE_ENV, env) == SQL_SUCCESS);
}

int main() {
  try {
    checkReviewedMetadata();
    checkReviewedNumeric();
    checkReviewedHandleLifetime();
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
