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
#pragma once
#include <vector>
#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <functional>
#include <condition_variable>
#include <algorithm>

#include "Session.h"

class TEndPoint;

class NodesSupplier {
public:
    using NodeSelectionPolicy = std::function<TEndPoint(const std::vector<TEndPoint>&)>;
    
    static std::shared_ptr<NodesSupplier> create(
        std::vector<TEndPoint> endpoints,
        std::chrono::seconds refreshInterval = std::chrono::seconds(5),
        NodeSelectionPolicy policy = roundRobinPolicy) {
        auto supplier = std::make_shared<NodesSupplier>(std::move(endpoints), std::move(policy));
        supplier->startBackgroundRefresh(refreshInterval);
        return supplier;
    }

    NodesSupplier(std::vector<TEndPoint> endpoints, NodeSelectionPolicy policy)
        : endpoints(std::move(endpoints)), selectionPolicy(std::move(policy)) 
    {
        deduplicateEndpoints();
    }

    std::vector<TEndPoint> getCurrentEndpoints() const {
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
    mutable std::mutex mutex;
    std::vector<TEndPoint> endpoints;
    NodeSelectionPolicy selectionPolicy;
    
    std::atomic_bool isRunning{false};
    std::thread refreshThread;
    std::condition_variable refreshCondition;

    std::shared_ptr<IClientRPCServiceIf> rpcClient;

    void deduplicateEndpoints() {
        std::sort(endpoints.begin(), endpoints.end());
        endpoints.erase(std::unique(endpoints.begin(), endpoints.end()), endpoints.end());
    }

    void startBackgroundRefresh(std::chrono::seconds interval) {
        isRunning = true;
        refreshThread = std::thread([this, interval] {
            while (isRunning) {
                refreshEndpointList();
                
                std::unique_lock<std::mutex> lock(mutex);
                refreshCondition.wait_for(lock, interval, [this] { 
                    return !isRunning.load(); 
                });
            }
        });
    }

    std::vector<TEndPoint> fetchLatestEndpoints() {
        // 实际实现需要连接服务端获取
        return std::vector<TEndPoint>();
    }

    void refreshEndpointList() {
        auto newEndpoints = fetchLatestEndpoints();
        
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
            throw std::runtime_error("No available nodes");
        }
        return nodes[roundRobinIndex++ % nodes.size()];
    }
};