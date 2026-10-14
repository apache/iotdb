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
#ifdef _WIN32
#include <windows.h>
#endif
#include <sql.h>
#include <sqlext.h>
#include <cstdlib>
#include <iostream>
#include <string>
#include <codecvt>
#include <locale>
#include <stdexcept>
#include <algorithm>
#include <sstream>
#include <vector>

#ifdef _WIN32
// Use a disposable user DSN so the test exercises the Windows Driver Manager
// without changing machine-wide driver registration or redirecting HKLM.
class UserDsnSandbox {
  std::string path_;
  std::string name_;

public:
  explicit UserDsnSandbox(std::string dll) {
    std::replace(dll.begin(), dll.end(), '/', '\\');
    name_ =
        "IoTDB-" + std::to_string(GetCurrentProcessId()) + "-" + std::to_string(GetTickCount64());
    path_ = "Software\\ODBC\\ODBC.INI\\" + name_;
    auto write = [&](const std::string& path, const char* name, const std::string& value) {
      HKEY key = nullptr;
      LONG rc = RegCreateKeyExA(HKEY_CURRENT_USER, path.c_str(), 0, nullptr, REG_OPTION_VOLATILE,
                                KEY_ALL_ACCESS, nullptr, &key, nullptr);
      if (rc == ERROR_SUCCESS) {
        rc = RegSetValueExA(key, name, 0, REG_SZ, reinterpret_cast<const BYTE*>(value.c_str()),
                            static_cast<DWORD>(value.size() + 1));
        RegCloseKey(key);
      }
      return rc == ERROR_SUCCESS;
    };
    if (!write(path_, "Driver", dll) || !write("Software\\ODBC\\ODBC.INI\\ODBC Data Sources",
                                               name_.c_str(), "Apache IoTDB ODBC Driver")) {
      RegDeleteTreeA(HKEY_CURRENT_USER, path_.c_str());
      throw std::runtime_error("Cannot configure temporary user DSN");
    }
  }
  const std::string& name() const {
    return name_;
  }
  ~UserDsnSandbox() {
    RegDeleteTreeA(HKEY_CURRENT_USER, path_.c_str());
    HKEY sources = nullptr;
    if (RegOpenKeyExA(HKEY_CURRENT_USER, "Software\\ODBC\\ODBC.INI\\ODBC Data Sources", 0,
                      KEY_SET_VALUE, &sources) == ERROR_SUCCESS) {
      RegDeleteValueA(sources, name_.c_str());
      RegCloseKey(sources);
    }
  }
};
#endif

static std::string utf8Environment(const char* name) {
#ifdef _WIN32
  std::string key(name);
  std::wstring wideKey(key.begin(), key.end());
  const wchar_t* value = _wgetenv(wideKey.c_str());
  return value ? std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>>().to_bytes(value) : "";
#else
  const char* value = std::getenv(name);
  return value ? value : "";
#endif
}

static std::basic_string<SQLWCHAR> sqlWide(const std::string& input) {
  auto utf16 =
      std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t>().from_bytes(input);
  std::basic_string<SQLWCHAR> result(utf16.size(), 0);
  std::copy(utf16.begin(), utf16.end(), result.begin());
  return result;
}

// Without IOTDB_ODBC_CONNECTION, reject a TLS/REST mismatch without network I/O.
// With it, connect to a real server and execute IOTDB_ODBC_QUERY or SHOW VERSION.
int runTest(int argc, char** argv) {
  if (argc != 2)
    return 2;
  std::string driver = argv[1];
#ifdef _WIN32
  UserDsnSandbox dsn(driver);
#endif
  SQLHENV env = nullptr;
  SQLHDBC dbc = nullptr;
  if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env)))
    return 1;
  SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<SQLPOINTER>(SQL_OV_ODBC3), 0);
  if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc)))
    return 1;
  std::string optionsText = utf8Environment("IOTDB_ODBC_CONNECTION");
  const char* options = optionsText.empty() ? nullptr : optionsText.c_str();
  const bool traceStages = !utf8Environment("IOTDB_ODBC_TRACE_STAGES").empty();
  auto stage = [&](const char* text) {
    if (traceStages)
      std::cerr << text << std::endl;
  };
#ifdef _WIN32
  std::string conn =
      "DSN=" + dsn.name() + ";SSL=0;" + (options ? options : "RESTFUL=1;SSL=1;SSLCA=missing.pem");
#else
  std::string conn = std::string("DRIVER={") + driver + "};SSL=0;" +
                     (options ? options : "RESTFUL=1;SSL=1;SSLCA=missing.pem");
#endif
  stage("driver-connect");
  auto wideConnection = sqlWide(conn);
  SQLRETURN rc = SQLDriverConnectW(dbc, nullptr, &wideConnection[0], SQL_NTS, nullptr, 0, nullptr,
                                   SQL_DRIVER_NOPROMPT);
  stage("driver-connect-returned");
  bool expectedFailure = !options || std::getenv("IOTDB_ODBC_EXPECT_FAILURE");
  bool ok = false;
  if (expectedFailure) {
    SQLCHAR state[6]{}, message[1024]{};
    SQLINTEGER native{};
    SQLSMALLINT length{};
    SQLRETURN diagnosticResult =
        SQLGetDiagRec(SQL_HANDLE_DBC, dbc, 1, state, &native, message, sizeof(message), &length);
    ok = rc == SQL_ERROR && std::string(reinterpret_cast<char*>(state)) == "08001";
    if (!ok)
      std::cerr << "connect=" << rc << ", diagnostic=" << diagnosticResult << ", " << state << ": "
                << message << '\n';
  } else if (SQL_SUCCEEDED(rc)) {
    SQLHSTMT stmt = nullptr;
    stage("allocate-statement");
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    std::string queryText = utf8Environment("IOTDB_ODBC_QUERY");
    const char* query = queryText.empty() ? nullptr : queryText.c_str();
    std::string sql = query ? query : "SHOW VERSION";
    auto wideSql = sqlWide(sql);
    std::string parameterText = utf8Environment("IOTDB_ODBC_PARAMETER_INTS");
    stage("execute");
    if (parameterText.empty()) {
      rc = SQLExecDirectW(stmt, &wideSql[0], SQL_NTS);
    } else {
      std::vector<SQLINTEGER> parameters;
      std::stringstream values(parameterText);
      std::string item;
      while (std::getline(values, item, ','))
        parameters.push_back(static_cast<SQLINTEGER>(std::stol(item)));
      std::vector<SQLLEN> lengths(parameters.size(), sizeof(SQLINTEGER));
      std::vector<SQLUSMALLINT> statuses(parameters.size(), SQL_PARAM_UNUSED);
      SQLULEN processed = 0;
      SQLSetStmtAttr(stmt, SQL_ATTR_PARAMSET_SIZE, reinterpret_cast<SQLPOINTER>(parameters.size()),
                     0);
      SQLSetStmtAttr(stmt, SQL_ATTR_PARAM_STATUS_PTR, statuses.data(), 0);
      SQLSetStmtAttr(stmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &processed, 0);
      rc = SQLPrepareW(stmt, &wideSql[0], SQL_NTS);
      if (SQL_SUCCEEDED(rc))
        rc = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 10, 0,
                              parameters.data(), sizeof(SQLINTEGER), lengths.data());
      if (SQL_SUCCEEDED(rc))
        rc = SQLExecute(stmt);
      if (SQL_SUCCEEDED(rc))
        rc = processed == parameters.size() &&
                     std::all_of(statuses.begin(), statuses.end(),
                                 [](SQLUSMALLINT status) {
                                   return status == SQL_PARAM_SUCCESS ||
                                          status == SQL_PARAM_SUCCESS_WITH_INFO;
                                 })
                 ? rc
                 : SQL_ERROR;
    }
    stage("execute-returned");
    if (SQL_SUCCEEDED(rc)) {
      SQLSMALLINT columns = 0;
      SQLNumResultCols(stmt, &columns);
      ok = columns == 0;
      int rows = 0;
      bool foundValue = false;
      std::string expectedText = utf8Environment("IOTDB_ODBC_EXPECT_VALUE");
      const char* expectedValue = expectedText.empty() ? nullptr : expectedText.c_str();
      while (stage("fetch"), SQL_SUCCEEDED(rc = SQLFetch(stmt))) {
        stage("fetch-returned-row");
        ++rows;
        for (SQLUSMALLINT col = 1; col <= columns; ++col) {
          SQLCHAR value[4096]{};
          SQLLEN length{};
          if (!SQL_SUCCEEDED(SQLGetData(stmt, col, SQL_C_CHAR, value, sizeof(value), &length)))
            return 1;
          if (length != SQL_NULL_DATA && expectedValue &&
              std::string(reinterpret_cast<char*>(value)) == expectedValue)
            foundValue = true;
        }
      }
      if (columns > 0)
        ok = rc == SQL_NO_DATA && rows > 0;
      if (const char* expectedRows = std::getenv("IOTDB_ODBC_EXPECT_ROWS"))
        ok = ok && rows == std::stoi(expectedRows);
      if (expectedValue)
        ok = ok && foundValue;
    }
    stage("free-statement");
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    stage("disconnect");
    SQLDisconnect(dbc);
    stage("disconnect-returned");
  } else {
    SQLCHAR state[6]{}, message[1024]{};
    SQLINTEGER native{};
    SQLSMALLINT length{};
    SQLGetDiagRec(SQL_HANDLE_DBC, dbc, 1, state, &native, message, sizeof(message), &length);
    std::cerr << state << ": " << message << '\n';
  }
  stage("free-connection");
  SQLFreeHandle(SQL_HANDLE_DBC, dbc);
  stage("free-environment");
  SQLFreeHandle(SQL_HANDLE_ENV, env);
  stage("done");
  return ok ? 0 : 1;
}

int main(int argc, char** argv) {
  try {
    return runTest(argc, argv);
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
