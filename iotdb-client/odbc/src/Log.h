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
#ifndef ODBC_LOG_H
#define ODBC_LOG_H

#include "Pch.h"
#include "ConnectionHandle.h" // For ConnectionHandle definition

// Logging levels
#define LOG_LEVEL_ERROR 0
#define LOG_LEVEL_WARN 1
#define LOG_LEVEL_INFO 2
#define LOG_LEVEL_DEBUG 3
#define LOG_LEVEL_TRACE 4

// Basic log message (always logged if enabled)
void logMessageInternal(const std::string& message);
void logMessage(const std::string& message);

// Level-based log message with connection context
void logMessage(ConnectionHandle* cnct, const std::string& message, int level);

// Cleanup logging resources
void cleanupLogging();

bool isLogLevelEnabled(ConnectionHandle* cnct, int level);

std::string CDataTypeName(SQLSMALLINT dataType);
std::string TSDataTypeName(TSDataType::TSDataType dataType);

std::string sqlCharToString(SQLCHAR* str);
std::string handleToString(SQLHANDLE handle);
std::string valueToString(SQLPOINTER value);

#endif // ODBC_LOG_H
