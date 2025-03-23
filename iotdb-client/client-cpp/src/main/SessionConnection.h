/**
* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#ifndef IOTDB_SESSIONCONNECTION_H
#define IOTDB_SESSIONCONNECTION_H

#include <memory>
#include <vector>
#include <string>
#include <functional>
#include <thrift/transport/TTransport.h>
#include "IClientRPCService.h"
#include "common_types.h"

class SessionDataSet;
class Session;

class SessionConnection {
public:
    SessionConnection() = default;

    SessionConnection(const TEndPoint& endpoint,
                     const std::string& zoneId,
                     std::function<std::vector<TEndPoint>()> nodeSupplier,
                     int maxRetries = 60,
                     int64_t retryInterval = 500,
                     std::string dialect = "tree",
                     std::string db = "",
                     std::string version = "V_1_0",
                     std::string username = "root",
                     std::string password = "root",
                     TSProtocolVersion::type protocolVersion = TSProtocolVersion::IOTDB_SERVICE_PROTOCOL_V3);

    ~SessionConnection();

    void setTimeZone(const std::string& newZoneId);

    void setEndpoint(const TEndPoint& endpoint) {
        this->endPoint = endpoint;
    }

    void setZoneId(const std::string& zoneId) {
        this->zoneId = zoneId;
    }

    void setNodeSupplier(std::function<std::vector<TEndPoint>()> nodeSupplier) {
        this->availableNodes = nodeSupplier;
    }

    void setMaxRetries(int maxRetries) {
        this->maxRetryCount = maxRetries;
    }

    void setRetryInterval(int64_t retryInterval) {
        this->retryIntervalMs = retryInterval;
    }

    void setDialect(const std::string& dialect) {
        this->sqlDialect = dialect;
    }

    void setDb(const std::string& db) {
        this->database = db;
    }

    void setVersion(const std::string& version) {
        this->version = version;
    }

    void setUsername(const std::string& username) {
        this->userName = username;
    }

    void setPassword(const std::string& password) {
        this->password = password;
    }

    void setProtocolVersion(const TSProtocolVersion::type& protocolVersion) {
        this->protocolVersion = protocolVersion;
    }

    const TEndPoint& getEndPoint();

    void init(const TEndPoint& endpoint);

    bool isInitialized() const {
        return inited;
    }

    std::unique_ptr<SessionDataSet> executeQueryStatement(const std::string& sql, int64_t timeoutInMs = -1);

private:
    void close();
    std::string getSystemDefaultZoneId();

    std::shared_ptr<apache::thrift::transport::TTransport> transport;
    std::shared_ptr<IClientRPCServiceClient> client;
    int64_t sessionId;
    int64_t statementId;
    int64_t connectionTimeoutInMs;
    bool enableRPCCompression;
    std::string zoneId;
    TEndPoint endPoint;
    std::vector<TEndPoint> endPointList;
    std::function<std::vector<TEndPoint>()> availableNodes;
    int maxRetryCount;
    int64_t retryIntervalMs;
    std::string sqlDialect;
    std::string database;
    std::string version;
    std::string userName;
    std::string password;
    TSProtocolVersion::type protocolVersion;
    bool inited = false;
};

#endif
