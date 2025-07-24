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
#ifndef IOTDB_THRIFTCONNECTION_H
#define IOTDB_THRIFTCONNECTION_H

#include <memory>
#include <thrift/transport/TBufferTransports.h>
#include "IClientRPCService.h"

class SessionDataSet;

class ThriftConnection {
public:
    static const int THRIFT_DEFAULT_BUFFER_SIZE;
    static const int THRIFT_MAX_FRAME_SIZE;
    static const int CONNECTION_TIMEOUT_IN_MS;
    static const int DEFAULT_FETCH_SIZE;

    explicit ThriftConnection(const TEndPoint& endPoint,
                     int thriftDefaultBufferSize = THRIFT_DEFAULT_BUFFER_SIZE,
                     int thriftMaxFrameSize = THRIFT_MAX_FRAME_SIZE,
                     int connectionTimeoutInMs = CONNECTION_TIMEOUT_IN_MS,
                     int fetchSize = DEFAULT_FETCH_SIZE);

    ~ThriftConnection();

    void init(const std::string& username,
              const std::string& password,
              bool enableRPCCompression = false,
              const std::string& zoneId = std::string(),
              const std::string& version = "V_1_0");

    std::unique_ptr<SessionDataSet> executeQueryStatement(const std::string& sql, int64_t timeoutInMs = -1);

    void close();

private:
    TEndPoint endPoint_;

    int thriftDefaultBufferSize_;
    int thriftMaxFrameSize_;
    int connectionTimeoutInMs_;
    int fetchSize_;

    std::shared_ptr<apache::thrift::transport::TTransport> transport_;
    std::shared_ptr<IClientRPCServiceClient> client_;
    int64_t sessionId_{};
    int64_t statementId_{};
    std::string zoneId_;
    int timeFactor_{};

    void initZoneId();
};

#endif
