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
#ifndef IOTDB_NODES_SUPPLIER_H
#define IOTDB_NODES_SUPPLIER_H

#include <vector>
#include <atomic>
#include <boost/optional.hpp>
#include <mutex>
#include <chrono>
#include <thread>
#include <functional>
#include <condition_variable>
#include <algorithm>

#include "ThriftConnection.h"

class TEndPoint;

class RoundRobinPolicy {
public:
    static TEndPoint select(const std::vector<TEndPoint>& nodes);
};

class INodesSupplier {
public:
    virtual ~INodesSupplier() = default;
    virtual boost::optional<TEndPoint> getQueryEndPoint() = 0;
    virtual std::vector<TEndPoint> getEndPointList() = 0;
    using NodeSelectionPolicy = std::function<TEndPoint(const std::vector<TEndPoint>&)>;
};

class StaticNodesSupplier : public INodesSupplier {
public:
    explicit StaticNodesSupplier(const std::vector<TEndPoint>& nodes, 
                                NodeSelectionPolicy policy = RoundRobinPolicy::select);

    boost::optional<TEndPoint> getQueryEndPoint() override;

    std::vector<TEndPoint> getEndPointList() override;

    ~StaticNodesSupplier() override;

private:
    const std::vector<TEndPoint> availableNodes_;
    NodeSelectionPolicy policy_;
};

class NodesSupplier : public INodesSupplier {
public:
    static const std::string SHOW_DATA_NODES_COMMAND;
    static const std::string STATUS_COLUMN_NAME;
    static const std::string IP_COLUMN_NAME;
    static const std::string PORT_COLUMN_NAME;
    static const std::string REMOVING_STATUS;

    static const int64_t TIMEOUT_IN_MS;
    static const int FETCH_SIZE;
    static const int THRIFT_DEFAULT_BUFFER_SIZE;
    static const int THRIFT_MAX_FRAME_SIZE;
    static const int CONNECTION_TIMEOUT_IN_MS;

    static std::shared_ptr<NodesSupplier> create(
        std::vector<TEndPoint> endpoints,
        std::string userName, std::string password, std::string zoneId = "",
        int32_t thriftDefaultBufferSize = ThriftConnection::THRIFT_DEFAULT_BUFFER_SIZE,
        int32_t thriftMaxFrameSize = ThriftConnection::THRIFT_MAX_FRAME_SIZE,
        int32_t connectionTimeoutInMs = ThriftConnection::CONNECTION_TIMEOUT_IN_MS,
        bool useSSL = false, bool enableRPCCompression = false,
        std::string version = "V_1_0",
        std::chrono::milliseconds refreshInterval = std::chrono::milliseconds(TIMEOUT_IN_MS),
        NodeSelectionPolicy policy = RoundRobinPolicy::select
    );

    NodesSupplier(
        std::string userName, std::string password, const std::string& zoneId,
        int32_t thriftDefaultBufferSize, int32_t thriftMaxFrameSize,
        int32_t connectionTimeoutInMs, bool useSSL, bool enableRPCCompression,
        std::string version, std::vector<TEndPoint> endpoints, NodeSelectionPolicy policy
    );
    std::vector<TEndPoint> getEndPointList() override;

    boost::optional<TEndPoint> getQueryEndPoint() override;

    ~NodesSupplier() override;

private:
    std::string userName;
    std::string password;
    int32_t thriftDefaultBufferSize;
    int32_t thriftMaxFrameSize;
    int32_t connectionTimeoutInMs;
    bool useSSL;
    bool enableRPCCompression;
    std::string version;
    std::string zoneId;

    std::mutex mutex;
    std::vector<TEndPoint> endpoints;
    NodeSelectionPolicy selectionPolicy;

    std::atomic<bool> isRunning{false};
    std::thread refreshThread;
    std::condition_variable refreshCondition;

    std::shared_ptr<ThriftConnection> client;

    void deduplicateEndpoints();

    void startBackgroundRefresh(std::chrono::milliseconds interval);

    std::vector<TEndPoint> fetchLatestEndpoints();

    void refreshEndpointList();

    TEndPoint selectQueryEndpoint();

    void stopBackgroundRefresh() noexcept;
};

#endif