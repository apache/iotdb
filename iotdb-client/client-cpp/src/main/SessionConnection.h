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
#include "Common.h"

class SessionDataSet;
class Session;

class SessionConnection : public std::enable_shared_from_this<SessionConnection> {
public:
    SessionConnection(Session* session_ptr, const TEndPoint& endpoint,
                      const std::string& zoneId,
                      std::shared_ptr<INodesSupplier> nodeSupplier,
                      int fetchSize = 10000,
                      int maxRetries = 3,
                      int64_t retryInterval = 500,
                      int64_t connectionTimeoutMs = 3 * 1000,
                      std::string dialect = "tree",
                      std::string db = "");

    ~SessionConnection();

    void setTimeZone(const std::string& newZoneId);


    const TEndPoint& getEndPoint();

    void init(const TEndPoint& endpoint);

    void insertStringRecord(const TSInsertStringRecordReq& request);

    void insertRecord(const TSInsertRecordReq& request);

    void insertStringRecords(const TSInsertStringRecordsReq& request);

    void insertRecords(const TSInsertRecordsReq& request);

    void insertRecordsOfOneDevice(TSInsertRecordsOfOneDeviceReq request);

    void insertStringRecordsOfOneDevice(TSInsertStringRecordsOfOneDeviceReq request);

    void insertTablet(TSInsertTabletReq request);

    void insertTablets(TSInsertTabletsReq request);

    void testInsertStringRecord(TSInsertStringRecordReq& request);

    void testInsertTablet(TSInsertTabletReq& request);

    void testInsertRecords(TSInsertRecordsReq& request);

    void deleteTimeseries(const vector<string>& paths);

    void deleteData(const TSDeleteDataReq& request);

    void setStorageGroup(const string& storageGroupId);

    void deleteStorageGroups(const vector<string>& storageGroups);

    void createTimeseries(TSCreateTimeseriesReq& req);

    void createMultiTimeseries(TSCreateMultiTimeseriesReq& req);

    void createAlignedTimeseries(TSCreateAlignedTimeseriesReq& req);

    TSGetTimeZoneResp getTimeZone();

    void setTimeZone(TSSetTimeZoneReq& req);

    void createSchemaTemplate(TSCreateSchemaTemplateReq req);

    void setSchemaTemplate(TSSetSchemaTemplateReq req);

    void unsetSchemaTemplate(TSUnsetSchemaTemplateReq req);

    void appendSchemaTemplate(TSAppendSchemaTemplateReq req);

    void pruneSchemaTemplate(TSPruneSchemaTemplateReq req);

    TSQueryTemplateResp querySchemaTemplate(TSQueryTemplateReq req);

    std::unique_ptr<SessionDataSet> executeRawDataQuery(const std::vector<std::string>& paths, int64_t startTime,
                                                        int64_t endTime);

    std::unique_ptr<SessionDataSet> executeLastDataQuery(const std::vector<std::string>& paths, int64_t lastTime);

    void executeNonQueryStatement(const std::string& sql);

    std::unique_ptr<SessionDataSet> executeQueryStatement(const std::string& sql, int64_t timeoutInMs = -1);

    std::shared_ptr<IClientRPCServiceClient> getSessionClient() {
        return client;
    }

    friend class Session;

private:
    void close();
    std::string getSystemDefaultZoneId();
    bool reconnect();

    template <typename T>
    struct RetryResult {
        T result;
        std::exception_ptr exception;
        int retryAttempts;

        RetryResult(T r, std::exception_ptr e, int a)
            : result(r), exception(e), retryAttempts(a) {
        }

        int getRetryAttempts() const { return retryAttempts; }
        T getResult() const { return result; }
        std::exception_ptr getException() const { return exception; }
    };

    template <typename T>
    void callWithRetryAndVerifyWithRedirection(std::function<T()> rpc);

    template <typename T>
    void callWithRetryAndVerifyWithRedirectionForMultipleDevices(
        std::function<T()> rpc, const vector<string>& deviceIds);

    template <typename T>
    RetryResult<T> callWithRetryAndVerify(std::function<T()> rpc);

    template <typename T>
    RetryResult<T> callWithRetry(std::function<T()> rpc);

    template <typename T, typename RpcFunc>
    RetryResult<T> callWithRetryAndReconnect(RpcFunc rpc);

    template <typename T, typename RpcFunc, typename StatusGetter>
    RetryResult<T> callWithRetryAndReconnect(RpcFunc rpc, StatusGetter statusGetter);

    template <typename T, typename RpcFunc, typename ShouldRetry, typename ForceReconnect>
    RetryResult<T> callWithRetryAndReconnect(RpcFunc rpc, ShouldRetry shouldRetry, ForceReconnect forceReconnect);

    TSStatus insertStringRecordInternal(TSInsertStringRecordReq request);

    TSStatus insertRecordInternal(TSInsertRecordReq request);

    TSStatus insertStringRecordsInternal(TSInsertStringRecordsReq request);

    TSStatus insertRecordsInternal(TSInsertRecordsReq request);

    TSStatus insertRecordsOfOneDeviceInternal(TSInsertRecordsOfOneDeviceReq request);

    TSStatus insertStringRecordsOfOneDeviceInternal(TSInsertStringRecordsOfOneDeviceReq request);

    TSStatus insertTabletInternal(TSInsertTabletReq request);

    TSStatus insertTabletsInternal(TSInsertTabletsReq request);

    TSStatus deleteDataInternal(TSDeleteDataReq request);

    std::shared_ptr<TTransport> transport;
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
    int timeFactor = 1000;
};

template <typename T>
SessionConnection::RetryResult<T> SessionConnection::callWithRetry(std::function<T()> rpc) {
    std::exception_ptr lastException = nullptr;
    TSStatus status;
    int i;
    for (i = 0; i <= maxRetryCount; i++) {
        if (i > 0) {
            lastException = nullptr;
            status = TSStatus();
            try {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(retryIntervalMs));
            }
            catch (const std::exception& e) {
                break;
            }
            if (!reconnect()) {
                continue;
            }
        }

        try {
            status = rpc();
            if (status.__isset.needRetry && status.needRetry) {
                continue;
            }
            break;
        }
        catch (...) {
            lastException = std::current_exception();
        }
    }
    return {status, lastException, i};
}

template <typename T>
void SessionConnection::callWithRetryAndVerifyWithRedirection(std::function<T()> rpc) {
    auto result = callWithRetry<T>(rpc);

    auto status = result.getResult();
    if (result.getRetryAttempts() == 0) {
        RpcUtils::verifySuccessWithRedirection(status);
    }
    else {
        RpcUtils::verifySuccess(status);
    }

    if (result.getException()) {
        try {
            std::rethrow_exception(result.getException());
        }
        catch (const std::exception& e) {
            throw IoTDBConnectionException(e.what());
        }
    }
}

template <typename T>
void SessionConnection::callWithRetryAndVerifyWithRedirectionForMultipleDevices(
    std::function<T()> rpc, const vector<string>& deviceIds) {
    auto result = callWithRetry<T>(rpc);
    auto status = result.getResult();
    if (result.getRetryAttempts() == 0) {
        RpcUtils::verifySuccessWithRedirectionForMultiDevices(status, deviceIds);
    }
    else {
        RpcUtils::verifySuccess(status);
    }
    if (result.getException()) {
        try {
            std::rethrow_exception(result.getException());
        }
        catch (const std::exception& e) {
            throw IoTDBConnectionException(e.what());
        }
    }
    result.exception = nullptr;
}

template <typename T>
SessionConnection::RetryResult<T> SessionConnection::callWithRetryAndVerify(std::function<T()> rpc) {
    auto result = callWithRetry<T>(rpc);
    RpcUtils::verifySuccess(result.getResult());
    if (result.getException()) {
        try {
            std::rethrow_exception(result.getException());
        }
        catch (const std::exception& e) {
            throw IoTDBConnectionException(e.what());
        }
    }
    return result;
}

template <typename T, typename RpcFunc>
SessionConnection::RetryResult<T> SessionConnection::callWithRetryAndReconnect(RpcFunc rpc) {
    return callWithRetryAndReconnect<T>(rpc,
                                        [](const TSStatus& status) {
                                            return status.__isset.needRetry && status.needRetry;
                                        },
                                        [](const TSStatus& status) {
                                            return status.code == TSStatusCode::PLAN_FAILED_NETWORK_PARTITION;
                                        }
    );
}

template <typename T, typename RpcFunc, typename StatusGetter>
SessionConnection::RetryResult<T> SessionConnection::callWithRetryAndReconnect(RpcFunc rpc, StatusGetter statusGetter) {
    auto shouldRetry = [&statusGetter](const T& t) {
        auto status = statusGetter(t);
        return status.__isset.needRetry && status.needRetry;
    };
    auto forceReconnect = [&statusGetter](const T& t) {
        auto status = statusGetter(t);
        return status.code == TSStatusCode::PLAN_FAILED_NETWORK_PARTITION;;
    };
    return callWithRetryAndReconnect<T>(rpc, shouldRetry, forceReconnect);
}


template <typename T, typename RpcFunc, typename ShouldRetry, typename ForceReconnect>
SessionConnection::RetryResult<T> SessionConnection::callWithRetryAndReconnect(RpcFunc rpc,
                                                                               ShouldRetry shouldRetry,
                                                                               ForceReconnect forceReconnect) {
    std::exception_ptr lastException = nullptr;
    T result;
    int retryAttempt;
    for (retryAttempt = 0; retryAttempt <= maxRetryCount; retryAttempt++) {
        try {
            result = rpc();
            lastException = nullptr;
        }
        catch (...) {
            result = T();
            lastException = std::current_exception();
        }

        if (!shouldRetry(result)) {
            return {result, lastException, retryAttempt};
        }

        if (lastException != nullptr ||
            std::find(availableNodes->getEndPointList().begin(), availableNodes->getEndPointList().end(),
                      this->endPoint) == availableNodes->getEndPointList().end() ||
            forceReconnect(result)) {
            reconnect();
        }

        try {
            std::this_thread::sleep_for(std::chrono::milliseconds(retryIntervalMs));
        }
        catch (const std::exception& e) {
            log_debug("Thread was interrupted during retry " +
                std::to_string(retryAttempt) +
                " with wait time " +
                std::to_string(retryIntervalMs) +
                " ms. Exiting retry loop.");
            break;
        }
    }

    return {result, lastException, retryAttempt};
}

#endif
