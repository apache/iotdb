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
#include <mutex>
#include <chrono>
#include <thread>
#include <functional>
#include <condition_variable>
#include <algorithm>

#include "Session.h"
#include "ThriftConnection.h"

class TEndPoint;

class NodesSupplier {
public:
    const std::string SHOW_DATA_NODES_COMMAND = "SHOW DATANODES";
    const std::string STATUS_COLUMN_NAME = "Status";
    const std::string IP_COLUMN_NAME = "RpcAddress";
    const std::string PORT_COLUMN_NAME = "RpcPort";
    const std::string REMOVING_STATUS = "Removing";
    const int64_t TIMEOUT_IN_MS = 60000;
    const int FETCH_SIZE = 10000;
    const int THRIFT_DEFAULT_BUFFER_SIZE = 4096;
    const int THRIFT_MAX_FRAME_SIZE = 1048576;
    const int CONNECTION_TIMEOUT_IN_MS = 1000;
    std::shared_ptr<ThriftConnection> client;

    std::string userName;
    std::string password;
    int32_t thriftDefaultBufferSize;
    int32_t thriftMaxFrameSize;
    int32_t connectionTimeoutInMs;
    bool useSSL;
    bool enableRPCCompression;
    std::string version;
    std::string zoneId;

    using NodeSelectionPolicy = std::function<TEndPoint(const std::vector<TEndPoint>&)>;
    
    static std::shared_ptr<NodesSupplier> create(std::vector<TEndPoint> endpoints,
        std::string userName, std::string password, std::string zoneId = "",
        int32_t thriftDefaultBufferSize = ThriftConnection::THRIFT_DEFAULT_BUFFER_SIZE,
        int32_t thriftMaxFrameSize = ThriftConnection::THRIFT_MAX_FRAME_SIZE,
        int32_t connectionTimeoutInMs = ThriftConnection::CONNECTION_TIMEOUT_IN_MS,
        bool useSSL = false, bool enableRPCCompression = false,
        std::string version = "V_1_0",
        std::chrono::milliseconds refreshInterval = std::chrono::milliseconds(TIMEOUT_IN_MS),
        NodeSelectionPolicy policy = roundRobinPolicy) {
        if (endpoints.empty()) {
            return nullptr;
        }
        auto supplier = std::make_shared<NodesSupplier>(userName, password, zoneId, thriftDefaultBufferSize,
            thriftMaxFrameSize, connectionTimeoutInMs, useSSL, enableRPCCompression, version, std::move(endpoints), std::move(policy));
        supplier->startBackgroundRefresh(refreshInterval);
        return supplier;
    }

    NodesSupplier(std::string userName, std::string password, std::string zoneId, int32_t thriftDefaultBufferSize,
        int32_t thriftMaxFrameSize, int32_t connectionTimeoutInMs, bool useSSL, bool enableRPCCompression,
        std::string version, std::vector<TEndPoint> endpoints, NodeSelectionPolicy policy) : userName(userName),
            password(password), zoneId(zoneId),
            thriftDefaultBufferSize(thriftDefaultBufferSize),
            thriftMaxFrameSize(thriftMaxFrameSize), connectionTimeoutInMs(connectionTimeoutInMs),
            useSSL(useSSL), enableRPCCompression(enableRPCCompression),
            version(version), endpoints(std::move(endpoints)),
            selectionPolicy(std::move(policy)){
        deduplicateEndpoints();
    }

    std::vector<TEndPoint> getCurrentEndpoints() {
        std::lock_guard<std::mutex> lock(mutex);
        return endpoints;
    }

    TEndPoint selectQueryEndpoint() {
        std::lock_guard<std::mutex> lock(mutex);
        return selectionPolicy(endpoints);
    }

    ~NodesSupplier() {
        stopBackgroundRefresh();
    }

private:
    std::mutex mutex;
    std::vector<TEndPoint> endpoints;
    NodeSelectionPolicy selectionPolicy;
    
    std::atomic<bool> isRunning{false};
    std::thread refreshThread;
    std::condition_variable refreshCondition;

    std::shared_ptr<IClientRPCServiceIf> rpcClient;

    void deduplicateEndpoints() {
        std::vector<TEndPoint> uniqueEndpoints;
        uniqueEndpoints.reserve(endpoints.size());
        for (const auto& endpoint : endpoints) {
            if (std::find(uniqueEndpoints.begin(), uniqueEndpoints.end(), endpoint) == uniqueEndpoints.end()) {
                uniqueEndpoints.push_back(endpoint);
            }
        }
        endpoints = std::move(uniqueEndpoints);
    }

    void startBackgroundRefresh(std::chrono::milliseconds interval) {
        isRunning = true;
        refreshThread = std::thread([this, interval] {
            while (isRunning) {
                refreshEndpointList();
                std::unique_lock<std::mutex> cvLock(this->mutex);
                refreshCondition.wait_for(cvLock, interval, [this]() {
                    return !isRunning.load();
                });
            }
        });
    }

    std::vector<TEndPoint> fetchLatestEndpoints() {
        if (client == nullptr) {
            client = std::make_shared<ThriftConnection>(roundRobinPolicy(endpoints));
            client->init(userName, password, enableRPCCompression, zoneId, version);
        }
        unique_ptr<SessionDataSet> sessionDataSet = client->executeQueryStatement(SHOW_DATA_NODES_COMMAND);
        uint32_t columnAddrIdx = -1, columnPortIdx = -1, columnStatusIdx = -1;
        auto columnNames = sessionDataSet->getColumnNames();
        for (uint32_t i = 0; i < columnNames.size(); i++) {
            if (columnNames[i] == IP_COLUMN_NAME) {
                columnAddrIdx = i;
            } else if (columnNames[i] == PORT_COLUMN_NAME) {
                columnPortIdx = i;
            } else if (columnNames[i] == STATUS_COLUMN_NAME) {
                columnStatusIdx = i;
            }
        }
        std::vector<TEndPoint> ret;
        try {
            while (sessionDataSet->hasNext()) {
                RowRecord* record = sessionDataSet->next();
                std::string ip = record->fields.at(columnAddrIdx).stringV;
                int32_t port = record->fields.at(columnPortIdx).intV;
                if (ip == "0.0.0.0" || REMOVING_STATUS == record->fields.at(columnStatusIdx).stringV) {
                    std::cout << ip << ":" << port << std::endl;
                    continue;
                }
                TEndPoint endpoint;
                endpoint.ip = ip;
                endpoint.port = port;
                ret.emplace_back(endpoint);
            }
        }
        catch (exception& e) {
            client.reset();
            throw IoTDBException(std::string("NodesSupplier::fetchLatestEndpoints") + e.what());
        }
        return ret;
    }

    void refreshEndpointList() {
        auto newEndpoints = fetchLatestEndpoints();
        if (newEndpoints.empty()) {
            return;
        }
        std::lock_guard<std::mutex> lock(mutex);
        endpoints.swap(newEndpoints);
        deduplicateEndpoints();
    }

    void stopBackgroundRefresh() noexcept {
        if (isRunning.exchange(false)) {
            refreshCondition.notify_all();
            if (refreshThread.joinable()) {
                refreshThread.join();
            }
        }
    }

    static TEndPoint roundRobinPolicy(const std::vector<TEndPoint>& nodes) {
        static std::atomic_uint roundRobinIndex{0};
        if (nodes.empty()) {
            throw IoTDBException("No available nodes");
        }
        return nodes[roundRobinIndex++ % nodes.size()];
    }
};

#endif