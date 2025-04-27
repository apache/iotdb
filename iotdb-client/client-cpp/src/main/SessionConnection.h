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
#include <thrift/transport/TTransport.h>
#include "IClientRPCService.h"
#include "common_types.h"
#include "NodesSupplier.h"
#include "Enums.h"

class SessionDataSet;
class Session;

class SessionConnection : std::enable_shared_from_this<SessionConnection> {
public:
    SessionConnection(Session* session_ptr, const TEndPoint& endpoint,
                     const std::string& zoneId,
                     std::shared_ptr<INodesSupplier> nodeSupplier,
                     int fetchSize = 10000,
                     int maxRetries = 60,
                     int64_t retryInterval = 500,
                     std::string dialect = "tree",
                     std::string db = "");

    ~SessionConnection();

    void setTimeZone(const std::string& newZoneId);


    const TEndPoint& getEndPoint();

    void init(const TEndPoint& endpoint);

    void insertRecord(const std::string &deviceId, int64_t time,
                           const std::vector<std::string> &measurements,
                           const std::vector<std::string> &values);

    void insertRecord(const std::string &prefixPath, int64_t time,
                           const std::vector<std::string> &measurements,
                           const std::vector<TSDataType::TSDataType> &types,
                           const std::vector<char *> &values);

    void insertAlignedRecord(const std::string &deviceId, int64_t time,
                                  const std::vector<std::string> &measurements,
                                  const std::vector<std::string> &values);

    void insertAlignedRecord(const std::string &prefixPath, int64_t time,
                                  const std::vector<std::string> &measurements,
                                  const std::vector<TSDataType::TSDataType> &types,
                                  const std::vector<char *> &values);

    std::unique_ptr<SessionDataSet> executeRawDataQuery(const std::vector<std::string> &paths, int64_t startTime, int64_t endTime);

    std::unique_ptr<SessionDataSet> executeLastDataQuery(const std::vector<std::string> &paths, int64_t lastTime);

    void executeNonQueryStatement(const std::string &sql);

    std::unique_ptr<SessionDataSet> executeQueryStatement(const std::string& sql, int64_t timeoutInMs = -1);

    std::shared_ptr<IClientRPCServiceClient> getSessionClient() {
        return client;
    }

private:
    void close();
    std::string getSystemDefaultZoneId();
    bool reconnect();

    std::shared_ptr<apache::thrift::transport::TTransport> transport;
    std::shared_ptr<IClientRPCServiceClient> client;
    Session* session;
    int64_t sessionId;
    int64_t statementId;
    int64_t connectionTimeoutInMs;
    bool enableRPCCompression = false;
    std::string zoneId;
    TEndPoint endPoint;
    std::vector<TEndPoint> endPointList;
    std::shared_ptr<INodesSupplier> availableNodes;
    int fetchSize;
    int maxRetryCount;
    int64_t retryIntervalMs;
    std::string sqlDialect;
    std::string database;
};

#endif
