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
#include "SessionDataSet.h"
#include <algorithm>
#include <iostream>
#include <utility>

const std::string NodesSupplier::SHOW_DATA_NODES_COMMAND = "SHOW DATANODES";
const std::string NodesSupplier::RUNNING_STATUS = "Running";
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
    std::string version, std::vector<TEndPoint> endpoints, NodeSelectionPolicy policy) : userName_(std::move(userName)), password_(std::move(password)), zoneId_(zoneId),
    thriftDefaultBufferSize_(thriftDefaultBufferSize), thriftMaxFrameSize_(thriftMaxFrameSize),
    connectionTimeoutInMs_(connectionTimeoutInMs), useSSL_(useSSL), enableRPCCompression_(enableRPCCompression), version(version), endpoints_(std::move(endpoints)),
    selectionPolicy_(std::move(policy)) {
    deduplicateEndpoints();
}

std::vector<TEndPoint> NodesSupplier::getEndPointList() {
    std::lock_guard<std::mutex> lock(mutex_);
    return endpoints_;
}

TEndPoint NodesSupplier::selectQueryEndpoint() {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        return selectionPolicy_(endpoints_);
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
    if (client_ != nullptr) {
        client_->close();
    }
}

void NodesSupplier::deduplicateEndpoints() {
    std::vector<TEndPoint> uniqueEndpoints;
    uniqueEndpoints.reserve(endpoints_.size());
    for (const auto& endpoint : endpoints_) {
        if (std::find(uniqueEndpoints.begin(), uniqueEndpoints.end(), endpoint) == uniqueEndpoints.end()) {
            uniqueEndpoints.push_back(endpoint);
        }
    }
    endpoints_ = std::move(uniqueEndpoints);
}

void NodesSupplier::startBackgroundRefresh(std::chrono::milliseconds interval) {
    isRunning_ = true;
    refreshEndpointList();
    refreshThread_ = std::thread([this, interval] {
        while (isRunning_) {
            refreshEndpointList();
            std::unique_lock<std::mutex> cvLock(this->mutex_);
            refreshCondition_.wait_for(cvLock, interval, [this]() {
                return !isRunning_.load();
            });
        }
    });
}

std::vector<TEndPoint> NodesSupplier::fetchLatestEndpoints() {
  for (const auto& endpoint : endpoints_) {
    try {
      if (client_ == nullptr) {
        client_ = std::make_shared<ThriftConnection>(endpoint);
        client_->init(userName_, password_, enableRPCCompression_, zoneId_, version);
      }

      auto sessionDataSet = client_->executeQueryStatement(SHOW_DATA_NODES_COMMAND);

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
        auto record = sessionDataSet->next();
        std::string ip;
        int32_t port = 0;
        std::string status;

        if (record->fields.at(columnAddrIdx).stringV.is_initialized()) {
          ip = record->fields.at(columnAddrIdx).stringV.value();
        }
        if (record->fields.at(columnPortIdx).intV.is_initialized()) {
          port = record->fields.at(columnPortIdx).intV.value();
        }
        if (record->fields.at(columnStatusIdx).stringV.is_initialized()) {
          status = record->fields.at(columnStatusIdx).stringV.value();
        }

        if (ip == "0.0.0.0" || status != RUNNING_STATUS) {
          log_warn("Skipping invalid node: " + ip + ":" + std::to_string(port));
          continue;
        }
        TEndPoint newEndpoint;
        newEndpoint.ip = ip;
        newEndpoint.port = port;
        ret.emplace_back(newEndpoint);
      }
      return ret;  // success
    } catch (const std::exception& e) {
      log_warn("Failed to fetch endpoints from " + endpoint.ip + ":" +
               std::to_string(endpoint.port) + " , error=" + e.what());
      client_.reset();  // reset client before retrying next endpoint
      continue;         // try next endpoint
    }
  }
  throw IoTDBException("NodesSupplier::fetchLatestEndpoints failed: all nodes unreachable.");
}

void NodesSupplier::refreshEndpointList() {
    try {
        auto newEndpoints = fetchLatestEndpoints();
        if (newEndpoints.empty()) {
            return;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        endpoints_.swap(newEndpoints);
        deduplicateEndpoints();
    } catch (const IoTDBException& e) {
        log_error(std::string("NodesSupplier::refreshEndpointList failed: ") + e.what());
    }
}

void NodesSupplier::stopBackgroundRefresh() noexcept {
    if (isRunning_.exchange(false)) {
        refreshCondition_.notify_all();
        if (refreshThread_.joinable()) {
            refreshThread_.join();
        }
    }
}