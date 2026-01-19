/*
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

#ifndef IOTDB_ABSTRACTSESSIONBUILDER_H
#define IOTDB_ABSTRACTSESSIONBUILDER_H

#include <string>

class AbstractSessionBuilder {
public:
    static constexpr const char* DEFAULT_HOST = "localhost";
    static constexpr int DEFAULT_RPC_PORT = 6667;
    static constexpr const char* DEFAULT_USERNAME = "root";
    static constexpr const char* DEFAULT_PASSWORD = "root";
    static constexpr int DEFAULT_FETCH_SIZE = 10000;
    static constexpr int DEFAULT_CONNECT_TIMEOUT_MS = 3 * 1000;
    static constexpr int DEFAULT_MAX_RETRIES = 3;
    static constexpr int DEFAULT_RETRY_DELAY_MS = 500;
    static constexpr const char* DEFAULT_SQL_DIALECT = "tree";
    static constexpr bool DEFAULT_ENABLE_AUTO_FETCH = true;
    static constexpr bool DEFAULT_ENABLE_REDIRECTIONS = true;
    static constexpr bool DEFAULT_ENABLE_RPC_COMPRESSION = false;

    std::string host = DEFAULT_HOST;
    int rpcPort = DEFAULT_RPC_PORT;
    std::string username = DEFAULT_USERNAME;
    std::string password = DEFAULT_PASSWORD;
    std::string zoneId = "";
    int fetchSize = DEFAULT_FETCH_SIZE;
    int connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS;
    int maxRetries = DEFAULT_MAX_RETRIES;
    int retryDelayMs = DEFAULT_RETRY_DELAY_MS;
    std::string sqlDialect = DEFAULT_SQL_DIALECT;
    std::string database = "";
    bool enableAutoFetch = DEFAULT_ENABLE_AUTO_FETCH;
    bool enableRedirections = DEFAULT_ENABLE_REDIRECTIONS;
    bool enableRPCCompression = DEFAULT_ENABLE_RPC_COMPRESSION;
    std::vector<std::string> nodeUrls;
};

#endif // IOTDB_ABSTRACTSESSIONBUILDER_H