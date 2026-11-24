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

#include "ConnectionHandle.h"
#include "StatementHandle.h"
#include "Log.h"
#include <odbcinst.h>
#include <SessionBuilder.h>
#include <TableSessionBuilder.h>

ConnectionHandle::ConnectionHandle(EnvironmentHandle* environment)
    : ODBCHandle(SQL_HANDLE_DBC), serverHostName("127.0.0.1"), serverPort("6667"), userName("root"),
      password("root"), autoCommit(true), timeoutLogin(0), timeoutConnection(0),
      environmentHandle(environment), database(""), logLevel(LOG_LEVEL_ERROR), isTableModel(true),
      useRestful(false), sessionTimeoutMs(LLONG_MAX), batchSize(1000), sessionPtr(nullptr),
      tableSessionPtr(nullptr) {}

void ConnectionHandle::CloseSession() {
  CloseChildren();
  if (sessionPtr) {
    sessionPtr->close();
    sessionPtr = nullptr;
  }
  if (tableSessionPtr) {
    tableSessionPtr->close();
    tableSessionPtr = nullptr;
  }
}

void ConnectionHandle::CloseChildren() {
  // Release result sets while their Session and parent connection still exist.
  // Statement destruction also removes its four implicit descriptors.
  while (!statements.empty())
    delete statements.back();
  while (!descriptors.empty())
    delete descriptors.back();
}

ConnectionHandle::~ConnectionHandle() noexcept {
  try {
    CloseSession();
  } catch (...) { /* Never throw across SQLFreeHandle. */
  }
}

void SetConnectionHandle(ConnectionHandle* cnct, std::string key, std::string value) {
  std::stringstream logStream;
  std::transform(key.begin(), key.end(), key.begin(),
                 [](unsigned char c) { return std::tolower(c); }); // Convert key to lowercase

  if (key == "ssl" || key == "restful") {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    bool enabled = value == "1" || value == "true" || value == "yes" || value == "on";
    if (key == "ssl") {
      cnct->invalidSslValue =
          !enabled && value != "0" && value != "false" && value != "no" && value != "off";
      cnct->sslConfig.useSsl = enabled;
    } else {
      cnct->invalidRestfulValue =
          !enabled && value != "0" && value != "false" && value != "no" && value != "off";
      cnct->useRestful = enabled;
    }
  } else if (key == "sslca")
    cnct->sslConfig.trustCertFilePath = value;
  else if (key == "sslcert")
    cnct->sslConfig.clientCertificateFilePath = value;
  else if (key == "sslkey")
    cnct->sslConfig.clientPrivateKeyFilePath = value;
  else if (key == "dsn" || key == "driver") {
  } else if (key == "uid") {
    logStream << "Set uid:" << value;
    logMessage(cnct, logStream.str(), LOG_LEVEL_INFO);
    cnct->userName = value;
  } else if (key == "pwd") {
    logStream << "Set pwd. (hidden)"; // Hidden for security reasons
    logMessage(cnct, logStream.str(), LOG_LEVEL_INFO);
    cnct->password = value;
  } else if (key == "server") {
    logStream << "Set server:" << value;
    logMessage(cnct, logStream.str(), LOG_LEVEL_INFO);
    cnct->serverHostName = value;
  } else if (key == "port") {
    logStream << "Set port:" << value;
    logMessage(cnct, logStream.str(), LOG_LEVEL_INFO);
    cnct->serverPort = value;
  } else if (key == "database") {
    logStream << "Set database:" << value;
    logMessage(cnct, logStream.str(), LOG_LEVEL_INFO);
    cnct->database = value;
  } else if (key == "loglevel") {
    logStream << "Set loglevel:" << value;
    logMessage(logStream.str());
    try {
      size_t consumed = 0;
      const int parsed = std::stoi(value, &consumed);
      if (consumed != value.size() || parsed < LOG_LEVEL_ERROR || parsed > LOG_LEVEL_TRACE)
        throw std::invalid_argument("LOGLEVEL must be between 0 and 4");
      cnct->logLevel = parsed;
    } catch (const std::exception&) {
      throw std::invalid_argument("LOGLEVEL must be between 0 and 4");
    }
  } else if (key == "istablemodel" || key == "tablemodel") {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return std::tolower(c); }); // Convert value to lowercase
    if (value == "0" || value == "false" || value == "no" || value == "off") {
      cnct->isTableModel = false;
      logStream << "Set isTableModel: false";
    } else if (value == "1" || value == "true" || value == "yes" || value == "on") {
      cnct->isTableModel = true;
      logStream << "Set isTableModel: true";
    } else {
      logStream << "Invalid isTableModel value: " << value << ", defaulting to true";
      cnct->isTableModel = true;
    }
    logMessage(cnct, logStream.str(), LOG_LEVEL_INFO);
  } else if (key == "sessiontimeoutms") {
    logStream << "Set sessionTimeoutMs:" << value;
    logMessage(cnct, logStream.str(), LOG_LEVEL_INFO);
    size_t consumed = 0;
    cnct->sessionTimeoutMs = std::stoll(value, &consumed);
    if (consumed != value.size() || cnct->sessionTimeoutMs < 0)
      throw std::invalid_argument("SESSIONTIMEOUTMS must be a non-negative integer");
    if (cnct->sessionTimeoutMs == 0)
      cnct->sessionTimeoutMs = LLONG_MAX;
  } else if (key == "batchsize") {
    logStream << "Set batchSize:" << value;
    logMessage(cnct, logStream.str(), LOG_LEVEL_INFO);
    size_t consumed = 0;
    const long long parsed = std::stoll(value, &consumed);
    if (consumed != value.size() || parsed < 0 || parsed > std::numeric_limits<SQLINTEGER>::max())
      throw std::invalid_argument("BATCHSIZE is out of range");
    cnct->batchSize = static_cast<SQLINTEGER>(parsed);
    if (cnct->batchSize == 0)
      cnct->batchSize = 1000;
  } else {
    logStream << "Currently unsupported connection parameter: key=" << key;
    logMessage(cnct, logStream.str(), LOG_LEVEL_WARN);
  }
}

bool ConnectionHandle::LoadDsnFromOdbcIni(const std::string& dsnName) {
  logMessage(this, "LoadDsnFromOdbcIni: Loading DSN '" + dsnName + "' from ODBC.INI",
             LOG_LEVEL_INFO);

  if (dsnName.empty()) {
    logMessage(this, "LoadDsnFromOdbcIni: DSN name is empty", LOG_LEVEL_WARN);
    return false;
  }
  dataSourceName = dsnName;

  auto readKey = [&](const char* key, const char* defaultVal) -> std::string {
    char buf[1024] = {};
    SQLGetPrivateProfileString(dsnName.c_str(), key, defaultVal, buf, sizeof(buf), "odbc.ini");
    return buf;
  };

  std::string val;
  for (const char* key : {"SSL", "SSLCA", "SSLCERT", "SSLKEY", "RESTFUL"}) {
    val = readKey(key, "");
    if (!val.empty())
      SetConnectionHandle(this, key, val);
  }

  val = readKey("SERVER", "");
  if (!val.empty()) {
    serverHostName = val;
    logMessage(this, "LoadDsnFromOdbcIni: SERVER=" + val, LOG_LEVEL_DEBUG);
  }

  val = readKey("PORT", "");
  if (!val.empty()) {
    serverPort = val;
    logMessage(this, "LoadDsnFromOdbcIni: PORT=" + val, LOG_LEVEL_DEBUG);
  }

  val = readKey("UID", "");
  if (!val.empty()) {
    userName = val;
    logMessage(this, "LoadDsnFromOdbcIni: UID=" + val, LOG_LEVEL_DEBUG);
  }

  val = readKey("PWD", "");
  if (!val.empty()) {
    password = val;
    logMessage(this, "LoadDsnFromOdbcIni: PWD=(hidden)", LOG_LEVEL_DEBUG);
  }

  val = readKey("DATABASE", "");
  if (!val.empty()) {
    database = val;
    logMessage(this, "LoadDsnFromOdbcIni: DATABASE=" + val, LOG_LEVEL_DEBUG);
  }

  val = readKey("ISTABLEMODEL", "");
  if (!val.empty()) {
    SetConnectionHandle(this, "istablemodel", val);
  }

  val = readKey("LOGLEVEL", "");
  if (!val.empty()) {
    SetConnectionHandle(this, "loglevel", val);
  }

  val = readKey("SESSIONTIMEOUTMS", "");
  if (!val.empty()) {
    SetConnectionHandle(this, "sessiontimeoutms", val);
  }

  val = readKey("BATCHSIZE", "");
  if (!val.empty()) {
    SetConnectionHandle(this, "batchsize", val);
  }

  std::ostringstream oss;
  oss << "LoadDsnFromOdbcIni: Loaded - Server=" << serverHostName << ", Port=" << serverPort
      << ", UID=" << userName << ", Database=" << database
      << ", TableModel=" << (isTableModel ? "true" : "false") << ", LogLevel=" << logLevel
      << ", SessionTimeout=" << sessionTimeoutMs << ", BatchSize=" << batchSize;
  logMessage(this, oss.str(), LOG_LEVEL_INFO);

  return true;
}

void ConnectionHandle::ValidateTransport() const {
  if (invalidSslValue)
    throw std::invalid_argument("SSL must be a boolean value");
  if (invalidRestfulValue)
    throw std::invalid_argument("RESTFUL must be a boolean value");
  size_t consumed = 0;
  long port = 0;
  try {
    port = std::stol(serverPort, &consumed);
  } catch (const std::exception&) {
    throw std::invalid_argument("PORT must be an integer between 1 and 65535");
  }
  if (consumed != serverPort.size() || port < 1 || port > 65535)
    throw std::invalid_argument("PORT must be an integer between 1 and 65535");
  bool hasCert = !sslConfig.clientCertificateFilePath.empty();
  bool hasKey = !sslConfig.clientPrivateKeyFilePath.empty();
  bool hasTlsOptions =
      sslConfig.useSsl || hasCert || hasKey || !sslConfig.trustCertFilePath.empty();
  if (useRestful && hasTlsOptions)
    throw std::invalid_argument("Session TLS options cannot be used with RESTFUL");
  if (hasTlsOptions && !sslConfig.useSsl)
    throw std::invalid_argument("SSL=1 is required when certificate options are specified");
  if (hasCert != hasKey)
    throw std::invalid_argument("SSLCERT and SSLKEY must be supplied together");
  sslConfig.validate();
}

void ConnectionHandle::OpenSession() {
  ValidateTransport();
  int port = std::stoi(serverPort);
  if (isTableModel) {
    if (!tableSessionPtr) {
      TableSessionBuilder builder;
      builder.host(serverHostName)
          ->rpcPort(port)
          ->username(userName)
          ->password(password)
          ->database(database);
      builder.sslConfig = sslConfig;
      tableSessionPtr = builder.build();
    }
  } else if (!sessionPtr) {
    SessionBuilder builder;
    builder.host(serverHostName)->rpcPort(port)->username(userName)->password(password);
    builder.sslConfig = sslConfig;
    sessionPtr = builder.build();
  }
}
