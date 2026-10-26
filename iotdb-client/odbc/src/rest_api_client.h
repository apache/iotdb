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
#ifndef REST_API_CLIENT_H
#define REST_API_CLIENT_H

#include "Pch.h"

// Forward declarations to avoid circular dependencies
class ConnectionHandle;
class StatementHandle;

// CURL callback function for writing response data
size_t WriteCallback(void* contents, const size_t size, const size_t nmemb, std::string* response);

// Execute RESTful API call
// @param cnct connection handle
// @param relativeUrl relative URL path
// @param jsonPayload JSON request payload
// @param jsonResponse output parameter for storing JSON response
// @return SQLRETURN return code
SQLRETURN executeRestCall(ConnectionHandle* cnct, const std::string& relativeUrl,
                          const nlohmann::json& jsonPayload, nlohmann::json* jsonResponse);

// Execute a simple query or modification statement directly through connection handle and return json data, no statement handle involved
// @param cnct connection handle
// @param statementText SQL statement text
// @param jsonResponse output parameter for storing JSON response
// @return SQLRETURN return code
SQLRETURN executeQuery(ConnectionHandle* cnct, std::string statementText,
                       nlohmann::json* jsonResponse);

// Test RESTful API connection and return connection result
// @param cnct connection handle
// @return SQLRETURN return code
SQLRETURN IoTDB_DriverConnect_Rest(ConnectionHandle* cnct);

// Execute direct statement execution for RESTful API
// @param stmt statement handle
// @param statementText SQL statement text
// @return SQLRETURN return code
SQLRETURN IoTDB_ExecDirect_Rest(StatementHandle* stmt, const std::string& statementText);

// Save curl response data for tree model
// @param stmt statement handle
// @param jsonResponse JSON response data
void saveCurlResponseTree(StatementHandle* stmt, const nlohmann::json& jsonResponse);

// Save curl response data for table model
// @param stmt statement handle
// @param jsonResponse JSON response data
void saveCurlResponseTable(StatementHandle* stmt, const nlohmann::json& jsonResponse);

// Save curl response data (automatically select based on model type)
// @param stmt statement handle
// @param jsonResponse JSON response data
void saveCurlResponse(StatementHandle* stmt, const nlohmann::json& jsonResponse);

#endif // REST_API_CLIENT_H
