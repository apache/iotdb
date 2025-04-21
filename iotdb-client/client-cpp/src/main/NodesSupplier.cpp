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
#include "NodesSupplier.h"
#include "Session.h"
#include <algorithm>
#include <iostream>
#include <utility>

const std::string NodesSupplier::SHOW_DATA_NODES_COMMAND = "SHOW DATANODES";
const std::string NodesSupplier::STATUS_COLUMN_NAME = "Status";
const std::string NodesSupplier::IP_COLUMN_NAME = "RpcAddress";
const std::string NodesSupplier::PORT_COLUMN_NAME = "RpcPort";
const std::string NodesSupplier::REMOVING_STATUS = "Removing";

const int64_t NodesSupplier::TIMEOUT_IN_MS = 60000;
const int NodesSupplier::FETCH_SIZE = 10000;
const int NodesSupplier::THRIFT_DEFAULT_BUFFER_SIZE = 4096;
const int NodesSupplier::THRIFT_MAX_FRAME_SIZE = 1048576;
const int NodesSupplier::CONNECTION_TIMEOUT_IN_MS = 1000;

TEndPoint RoundRobinPolicy::select(const std::vector<TEndPoint>& nodes) {
    static std::atomic_uint index{0};

    if (nodes.empty()) {
        throw IoTDBException("No available nodes");
    }

    return nodes[index++ % nodes.size()];
}

StaticNodesSupplier::StaticNodesSupplier(const std::vector<TEndPoint>& nodes,
                                       NodeSelectionPolicy policy)
    : availableNodes_(nodes), policy_(std::move(policy)) {}

boost::optional<TEndPoint> StaticNodesSupplier::getQueryEndPoint() {
    try {
        if (availableNodes_.empty()) {
            return boost::none;
        }
        return policy_(availableNodes_);
    } catch (const IoTDBException& e) {
        return boost::none;
    }
}

std::vector<TEndPoint> StaticNodesSupplier::getEndPointList() {
    return availableNodes_;
}

StaticNodesSupplier::~StaticNodesSupplier() = default;

std::shared_ptr<NodesSupplier> NodesSupplier::create(
    std::vector<TEndPoint> endpoints,
    std::string userName, std::string password, std::string zoneId,
    int32_t thriftDefaultBufferSize, int32_t thriftMaxFrameSize,
    int32_t connectionTimeoutInMs, bool useSSL, bool enableRPCCompression,
    std::string version, std::chrono::milliseconds refreshInterval,
    NodeSelectionPolicy policy) {
    if (endpoints.empty()) {
        return nullptr;
    }
    auto supplier = std::make_shared<NodesSupplier>(
        userName, password, zoneId, thriftDefaultBufferSize,
        thriftMaxFrameSize, connectionTimeoutInMs, useSSL,
        enableRPCCompression, version, std::move(endpoints), std::move(policy)
    );
    supplier->startBackgroundRefresh(refreshInterval);
    return supplier;
}

NodesSupplier::NodesSupplier(
    std::string userName, std::string password, const std::string& zoneId,
    int32_t thriftDefaultBufferSize, int32_t thriftMaxFrameSize,
    int32_t connectionTimeoutInMs, bool useSSL, bool enableRPCCompression,
    std::string version, std::vector<TEndPoint> endpoints, NodeSelectionPolicy policy) : userName(std::move(userName)), password(std::move(password)), zoneId(zoneId),
    thriftDefaultBufferSize(thriftDefaultBufferSize), thriftMaxFrameSize(thriftMaxFrameSize),
    connectionTimeoutInMs(connectionTimeoutInMs), useSSL(useSSL), enableRPCCompression(enableRPCCompression), version(version), endpoints(std::move(endpoints)),
    selectionPolicy(std::move(policy)) {
    deduplicateEndpoints();
}

std::vector<TEndPoint> NodesSupplier::getEndPointList() {
    std::lock_guard<std::mutex> lock(mutex);
    return endpoints;
}

TEndPoint NodesSupplier::selectQueryEndpoint() {
    std::lock_guard<std::mutex> lock(mutex);
    try {
        return selectionPolicy(endpoints);
    } catch (const std::exception& e) {
        log_error("NodesSupplier::selectQueryEndpoint exception: %s", e.what());
        throw IoTDBException("NodesSupplier::selectQueryEndpoint exception, " + std::string(e.what()));
    }
}

boost::optional<TEndPoint> NodesSupplier::getQueryEndPoint() {
    try {
        return selectQueryEndpoint();
    } catch (const IoTDBException& e) {
        return boost::none;
    }
}

NodesSupplier::~NodesSupplier() {
    stopBackgroundRefresh();
    client->close();
}

void NodesSupplier::deduplicateEndpoints() {
    std::vector<TEndPoint> uniqueEndpoints;
    uniqueEndpoints.reserve(endpoints.size());
    for (const auto& endpoint : endpoints) {
        if (std::find(uniqueEndpoints.begin(), uniqueEndpoints.end(), endpoint) == uniqueEndpoints.end()) {
            uniqueEndpoints.push_back(endpoint);
        }
    }
    endpoints = std::move(uniqueEndpoints);
}

void NodesSupplier::startBackgroundRefresh(std::chrono::milliseconds interval) {
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

std::vector<TEndPoint> NodesSupplier::fetchLatestEndpoints() {
    try {
        if (client == nullptr) {
            client = std::make_shared<ThriftConnection>(selectionPolicy(endpoints));
            client->init(userName, password, enableRPCCompression, zoneId, version);
        }

        auto sessionDataSet = client->executeQueryStatement(SHOW_DATA_NODES_COMMAND);

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

        if (columnAddrIdx == -1 || columnPortIdx == -1 || columnStatusIdx == -1) {
            throw IoTDBException("Required columns not found in query result.");
        }

        std::vector<TEndPoint> ret;
        while (sessionDataSet->hasNext()) {
            RowRecord* record = sessionDataSet->next();
            std::string ip = record->fields.at(columnAddrIdx).stringV;
            int32_t port = record->fields.at(columnPortIdx).intV;
            std::string status = record->fields.at(columnStatusIdx).stringV;

            if (ip == "0.0.0.0" || status == REMOVING_STATUS) {
                log_warn("Skipping invalid node: " + ip + ":" + to_string(port));
                continue;
            }
            TEndPoint endpoint;
            endpoint.ip = ip;
            endpoint.port = port;
            ret.emplace_back(endpoint);
        }

        return ret;
    } catch (const IoTDBException& e) {
        client.reset();
        throw IoTDBException(std::string("NodesSupplier::fetchLatestEndpoints failed: ") + e.what());
    }
}

void NodesSupplier::refreshEndpointList() {
    try {
        auto newEndpoints = fetchLatestEndpoints();
        if (newEndpoints.empty()) {
            return;
        }

        std::lock_guard<std::mutex> lock(mutex);
        endpoints.swap(newEndpoints);
        deduplicateEndpoints();
    } catch (const IoTDBException& e) {
        log_error(std::string("NodesSupplier::refreshEndpointList failed: ") + e.what());
    }
}

void NodesSupplier::stopBackgroundRefresh() noexcept {
    if (isRunning.exchange(false)) {
        refreshCondition.notify_all();
        if (refreshThread.joinable()) {
            refreshThread.join();
        }
    }
}