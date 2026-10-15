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

#ifndef CONNECTIONHANDLE_H
#define CONNECTIONHANDLE_H

#include "Pch.h"
#include "ODBCHandle.h"
#include "EnvironmentHandle.h"
#include <sqltypes.h>
#include <Session.h>
#include <TableSession.h>

class StatementHandle;
class DescriptorHandle;

class ConnectionHandle : public ODBCHandle {
public:
  ConnectionHandle(EnvironmentHandle* environment);
  ~ConnectionHandle() noexcept;

  EnvironmentHandle* getEnvironment() const {
    return environmentHandle;
  }

  std::string serverHostName;
  std::string serverPort;
  std::string userName;
  std::string password;
  bool autoCommit;
  SQLUINTEGER timeoutLogin;
  SQLUINTEGER timeoutConnection;
  std::string database;
  std::string dataSourceName;
  std::vector<StatementHandle*> statements;
  std::vector<DescriptorHandle*> descriptors;
  int logLevel;
  bool isTableModel;
  bool useRestful;
  SslConfig sslConfig;
  bool invalidSslValue = false;
  bool invalidRestfulValue = false;
  void ValidateTransport() const;
  void OpenSession();
  SQLBIGINT sessionTimeoutMs;
  SQLINTEGER batchSize;
  std::shared_ptr<Session> sessionPtr;
  std::shared_ptr<TableSession> tableSessionPtr;

  void CloseSession();
  void CloseChildren();

  bool LoadDsnFromOdbcIni(const std::string& dsnName);

private:
  EnvironmentHandle* environmentHandle;
};

void SetConnectionHandle(ConnectionHandle* cnct, std::string key, std::string value);

#endif //CONNECTIONHANDLE_H
