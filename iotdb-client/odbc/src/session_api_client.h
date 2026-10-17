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
#ifndef SESSION_API_CLIENT_H
#define SESSION_API_CLIENT_H

#include <string>
#include <memory>
#include "Pch.h"

// Forward declarations
class ConnectionHandle;
class StatementHandle;
class TableSession;
class SessionDataSet;

// Test Session API connection and return connection result
SQLRETURN IoTDB_DriverConnect_Session(ConnectionHandle* cnct);

// Execute direct statement execution for Session API
SQLRETURN IoTDB_ExecDirect_Session(StatementHandle* stmt, const std::string& statementText);

#endif // SESSION_API_CLIENT_H
