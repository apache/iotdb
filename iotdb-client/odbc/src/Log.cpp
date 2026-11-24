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
#include "Pch.h"
#include "Log.h"

// Additional standard library includes not in Pch.h
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <mutex>

#include <sqltypes.h>
#include <string>

// Project includes
#include "DriverConfig.h"

static std::ofstream logFile;
static std::once_flag logFileOnce;
static std::mutex logMutex;

static void EnsureLogFileOpen() {
#ifdef WIN32
  HMODULE hMod = nullptr;
  if (!GetModuleHandleExA(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS |
                              GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT,
                          reinterpret_cast<LPCSTR>(&logMessageInternal), &hMod)) {
    logFile.open(DEBUG_LOG_OUTPUT_PATH, std::ios::app);
    return;
  }
  char dllPath[MAX_PATH] = {};
  if (GetModuleFileNameA(hMod, dllPath, MAX_PATH) == 0) {
    logFile.open(DEBUG_LOG_OUTPUT_PATH, std::ios::app);
    return;
  }
  std::string dir(dllPath);
  const size_t lastSlash = dir.find_last_of("\\/");
  if (lastSlash != std::string::npos)
    dir.resize(lastSlash);
  else
    dir = ".";
  const std::string iniPath = dir + "\\apache_iotdb_odbc_driver.ini";
  char buf[MAX_PATH] = {};
  GetPrivateProfileStringA("logging", "LogFile", "", buf, sizeof(buf), iniPath.c_str());
  std::string path = buf;
  if (path.empty()) {
    GetPrivateProfileStringA("logging", "LogDir", "", buf, sizeof(buf), iniPath.c_str());
    if (buf[0] != '\0')
      path = std::string(buf) + "\\apache_iotdb_odbc.log";
  }
  if (path.empty())
    path = DEBUG_LOG_OUTPUT_PATH;
  logFile.open(path, std::ios::app);
#else
  logFile.open(DEBUG_LOG_OUTPUT_PATH, std::ios::app);
#endif
}

void logMessageInternal(const std::string& message) {
#if !ODBC_ENABLE_LOGGING
  (void)message;
  return;
#else
  std::call_once(logFileOnce, EnsureLogFileOpen);
  std::lock_guard<std::mutex> lock(logMutex);
  if (!logFile) {
    return;
  }

  const std::time_t now = std::time(nullptr);

#ifdef WIN32
  // Windows implementation
  char buffer[26];
  if (ctime_s(buffer, sizeof(buffer), &now) == 0) {
    std::string timeStr = buffer;
    if (!timeStr.empty() && timeStr.back() == '\n') {
      timeStr.pop_back();
    }
    logFile << timeStr << ": " << message << std::endl;
  }
#else
  // Linux & macOS implementation
  char buffer[64];
  std::tm localTime{};
  if (localtime_r(&now, &localTime) &&
      std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &localTime)) {
    logFile << buffer << ": " << message << std::endl;
  }
#endif
  else {
    logFile << "Error retrieving time: " << message << std::endl;
  }
#endif
}

/*
Logging Levels:
= 0 (ERROR):   Critical failures requiring immediate intervention. Always logged.
= 1 (WARN):    Non-critical issues or unexpected conditions that merit attention. Logged in production.
= 2 (INFO):    General operational events (startup, milestones, state changes).
= 3 (DEBUG):   Diagnostic details for troubleshooting. Enabled during development/testing.
= 4 (TRACE):   Granular execution tracing (function calls, data flows). Highest verbosity for deep debugging.
if cnct == nullptr, the log will output.
 */
void logMessage(ConnectionHandle* cnct, const std::string& message, int level) {
#if !ODBC_ENABLE_LOGGING
  (void)cnct;
  (void)message;
  (void)level;
  return;
#else
  if (!isLogLevelEnabled(cnct, level))
    return;
  logMessageInternal(message);
#endif
}

void logMessage(const std::string& message) {
#if ODBC_ENABLE_LOGGING
  logMessageInternal("(NO LOG LEVEL) " + message);
#else
  (void)message;
#endif
}

void cleanupLogging() {
#if ODBC_ENABLE_LOGGING
  std::lock_guard<std::mutex> lock(logMutex);
  if (logFile.is_open()) {
    logFile.close();
  }
#endif
}

bool isLogLevelEnabled(ConnectionHandle* cnct, int level) {
#if !ODBC_ENABLE_LOGGING
  (void)cnct;
  (void)level;
  return false;
#else
  if (cnct == nullptr)
    return true;
  return cnct->logLevel >= level;
#endif
}

std::string CDataTypeName(SQLSMALLINT dataType) {
  std::string name;
  switch (dataType) {
  case SQL_C_CHAR:
    name = "SQL_C_CHAR";
    break;
  case SQL_C_WCHAR:
    name = "SQL_C_WCHAR";
    break;
  case SQL_C_NUMERIC:
    name = "SQL_C_NUMERIC";
    break;
  case SQL_C_SSHORT:
    name = "SQL_C_SSHORT";
    break;
  case SQL_C_USHORT:
    name = "SQL_C_USHORT";
    break;
  case SQL_C_SLONG:
    name = "SQL_C_SLONG";
    break;
  case SQL_C_ULONG:
    name = "SQL_C_ULONG";
    break;
  case SQL_C_FLOAT:
    name = "SQL_C_FLOAT";
    break;
  case SQL_C_DOUBLE:
    name = "SQL_C_DOUBLE";
    break;
  case SQL_C_BIT:
    name = "SQL_C_BIT";
    break;
  case SQL_C_STINYINT:
    name = "SQL_C_STINYINT";
    break;
  case SQL_C_UTINYINT:
    name = "SQL_C_UTINYINT";
    break;
  case SQL_C_SBIGINT:
    name = "SQL_C_SBIGINT";
    break;
  case SQL_C_UBIGINT:
    name = "SQL_C_UBIGINT";
    break;
  case SQL_C_BINARY:
    name = "SQL_C_BINARY";
    break;
  case SQL_C_DATE:
    name = "SQL_C_DATE";
    break;
  case SQL_C_TIME:
    name = "SQL_C_TIME";
    break;
  case SQL_C_TIMESTAMP:
    name = "SQL_C_TIMESTAMP";
    break;
  case SQL_C_TYPE_DATE:
    name = "SQL_C_TYPE_DATE";
    break;
  case SQL_C_TYPE_TIME:
    name = "SQL_C_TYPE_TIME";
    break;
  case SQL_C_TYPE_TIMESTAMP:
    name = "SQL_C_TYPE_TIMESTAMP";
    break;
  case SQL_C_GUID:
    name = "SQL_C_GUID";
    break;
  case SQL_C_INTERVAL_YEAR:
    name = "SQL_C_INTERVAL_YEAR";
    break;
  case SQL_C_INTERVAL_MONTH:
    name = "SQL_C_INTERVAL_MONTH";
    break;
  case SQL_C_INTERVAL_DAY:
    name = "SQL_C_INTERVAL_DAY";
    break;
  case SQL_C_INTERVAL_HOUR:
    name = "SQL_C_INTERVAL_HOUR";
    break;
  case SQL_C_INTERVAL_MINUTE:
    name = "SQL_C_INTERVAL_MINUTE";
    break;
  case SQL_C_INTERVAL_SECOND:
    name = "SQL_C_INTERVAL_SECOND";
    break;
  case SQL_C_INTERVAL_YEAR_TO_MONTH:
    name = "SQL_C_INTERVAL_YEAR_TO_MONTH";
    break;
  case SQL_C_INTERVAL_DAY_TO_HOUR:
    name = "SQL_C_INTERVAL_DAY_TO_HOUR";
    break;
  case SQL_C_INTERVAL_DAY_TO_MINUTE:
    name = "SQL_C_INTERVAL_DAY_TO_MINUTE";
    break;
  case SQL_C_INTERVAL_DAY_TO_SECOND:
    name = "SQL_C_INTERVAL_DAY_TO_SECOND";
    break;
  case SQL_C_INTERVAL_HOUR_TO_MINUTE:
    name = "SQL_C_INTERVAL_HOUR_TO_MINUTE";
    break;
  case SQL_C_INTERVAL_HOUR_TO_SECOND:
    name = "SQL_C_INTERVAL_HOUR_TO_SECOND";
    break;
  case SQL_C_DEFAULT:
    name = "SQL_C_DEFAULT";
    break;
  default:
    name = "UNKNOWN_TYPE_CODE";
    break;
  }
  return name + "(" + std::to_string(dataType) + ")";
}

std::string TSDataTypeName(TSDataType::TSDataType dataType) {
  std::string name;
  switch (dataType) {
  case TSDataType::BOOLEAN:
    name = "BOOLEAN";
    break;
  case TSDataType::INT32:
    name = "INT32";
    break;
  case TSDataType::INT64:
    name = "INT64";
    break;
  case TSDataType::TIMESTAMP:
    name = "TIMESTAMP";
    break;
  case TSDataType::FLOAT:
    name = "FLOAT";
    break;
  case TSDataType::DOUBLE:
    name = "DOUBLE";
    break;
  case TSDataType::TEXT:
    name = "TEXT";
    break;
  case TSDataType::STRING:
    name = "STRING";
    break;
  case TSDataType::DATE:
    name = "DATE";
    break;
  case TSDataType::BLOB:
    name = "BLOB";
    break;
  default:
    name = "UNKNOWN_TYPE";
    break;
  }
  return "TSDataType::" + name + "(" + std::to_string(static_cast<int>(dataType)) + ")";
}

std::string sqlCharToString(SQLCHAR* str) {
  return str ? reinterpret_cast<char*>(str) : "NULL";
}

std::string handleToString(SQLHANDLE handle) {
  std::stringstream ss;
  ss << std::hex << handle;
  return ss.str();
}

std::string valueToString(SQLPOINTER value) {
  if (value == nullptr) {
    return "NULL";
  }

  std::ostringstream stream;

  // Check if Value is a standard constant or an integer
  if (reinterpret_cast<intptr_t>(value) == SQL_AUTOCOMMIT_ON) {
    return "SQL_AUTOCOMMIT_ON";
  }
  if (reinterpret_cast<intptr_t>(value) == SQL_AUTOCOMMIT_OFF) {
    return "SQL_AUTOCOMMIT_OFF";
  }

  // Handle specific types of values based on context
  try {
    // If it points to an integer
    if (reinterpret_cast<intptr_t>(value) <= std::numeric_limits<intptr_t>::max()) {
      stream << "Integer: " << reinterpret_cast<uintptr_t>(value);
    } else {
      // If it points to a string (make sure it's null-terminated)
      const auto strValue = static_cast<const char*>(value);
      stream << "String: \"" << std::string(strValue) << "\"";
    }
  } catch (...) {
    // Fallback for unknown Value types
    stream << "Unknown: " << value;
  }

  return stream.str();
}
