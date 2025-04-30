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

    void insertStringRecord(const TSInsertStringRecordReq& request) {
        auto rpc = [this, request]() {
            return this->insertStringRecordInternal(request);
        };
        callWithRetryAndVerifyWithRedirection<TSStatus>(rpc);
    }

    TSStatus insertStringRecordInternal(TSInsertStringRecordReq request) {
        request.sessionId = sessionId;
        TSStatus ret;
        client->insertStringRecord(ret, request);
        return ret;
    }

    void insertRecord(const TSInsertRecordReq& request) {
        auto rpc = [this, request]() {
            return this->insertRecordInternal(request);
        };
        callWithRetryAndVerifyWithRedirection<TSStatus>(rpc);
    }

    TSStatus insertRecordInternal(TSInsertRecordReq request) {
        request.sessionId = sessionId;
        TSStatus ret;
        client->insertRecord(ret, request);
        return ret;
    }

    void insertStringRecords(const TSInsertStringRecordsReq& request) {
        auto rpc = [this, request]() {
            return this->insertStringRecordsInternal(request);
        };
        callWithRetryAndVerifyWithRedirection<TSStatus>(rpc);
    }

    TSStatus insertStringRecordsInternal(TSInsertStringRecordsReq request) {
        request.sessionId = sessionId;
        TSStatus ret;
        client->insertStringRecords(ret, request);
        return ret;
    }

    void insertRecords(const TSInsertRecordsReq& request) {
        auto rpc = [this, request]() {
            return this->insertRecordsInternal(request);
        };
        callWithRetryAndVerifyWithRedirection<TSStatus>(rpc);
    }

    TSStatus insertRecordsInternal(TSInsertRecordsReq request) {
        request.sessionId = sessionId;
        TSStatus ret;
        client->insertRecords(ret, request);
        return ret;
    }

    void insertRecordsOfOneDevice(TSInsertRecordsOfOneDeviceReq request) {
        auto rpc = [this, request]() {
            return this->insertRecordsOfOneDeviceInternal(request);
        };
        callWithRetryAndVerifyWithRedirection<TSStatus>(rpc);
    }

    TSStatus insertRecordsOfOneDeviceInternal(TSInsertRecordsOfOneDeviceReq request) {
        request.sessionId = sessionId;
        TSStatus ret;
        client->insertRecordsOfOneDevice(ret, request);
        return ret;
    }

    void insertStringRecordsOfOneDevice(TSInsertStringRecordsOfOneDeviceReq request) {
        auto rpc = [this, request]() {
            return this->insertStringRecordsOfOneDeviceInternal(request);
        };
        callWithRetryAndVerifyWithRedirection<TSStatus>(rpc);
    }

    TSStatus insertStringRecordsOfOneDeviceInternal(TSInsertStringRecordsOfOneDeviceReq request){
        request.sessionId = sessionId;
        TSStatus ret;
        client->insertStringRecordsOfOneDevice(ret, request);
        return ret;
    }

    void insertTablet(TSInsertTabletReq request) {
        auto rpc = [this, request]() {
            return this->insertTabletInternal(request);
        };
        callWithRetryAndVerifyWithRedirection<TSStatus>(rpc);
    }

    TSStatus insertTabletInternal(TSInsertTabletReq request) {
        request.sessionId = sessionId;
        TSStatus ret;
        client->insertTablet(ret, request);
        return ret;
    }

    void insertTablets(TSInsertTabletsReq request) {
        auto rpc = [this, request]() {
            return this->insertTabletsInternal(request);
        };
        callWithRetryAndVerifyWithRedirection<TSStatus>(rpc);
    }

    TSStatus insertTabletsInternal(TSInsertTabletsReq request) {
        request.sessionId = sessionId;
        TSStatus ret;
        client->insertTablets(ret, request);
        return ret;
    }

    void testInsertStringRecord(TSInsertStringRecordReq& request) {
        auto rpc = [this, &request]() {
            request.sessionId = sessionId;
            TSStatus ret;
            client->testInsertStringRecord(ret, request);
            return ret;
        };
        auto status = callWithRetryAndReconnect<TSStatus>(rpc).getResult();
        RpcUtils::verifySuccess(status);
    }

    void testInsertTablet(TSInsertTabletReq& request) {
        auto rpc = [this, &request]() {
            request.sessionId = sessionId;
            TSStatus ret;
            client->testInsertTablet(ret, request);
            return ret;
        };
        auto status = callWithRetryAndReconnect<TSStatus>(rpc).getResult();
        RpcUtils::verifySuccess(status);
    }

    void testInsertRecords(TSInsertRecordsReq& request) {
        auto rpc = [this, &request]() {
            request.sessionId = sessionId;
            TSStatus ret;
            client->testInsertRecords(ret, request);
            return ret;
        };
        auto status = callWithRetryAndReconnect<TSStatus>(rpc).getResult();
        RpcUtils::verifySuccess(status);
    }

    void deleteTimeseries(const vector<string> &paths) {
        auto rpc = [this, &paths]() {
            TSStatus ret;
            client->deleteTimeseries(ret, sessionId, paths);
            return ret;
        };
        callWithRetryAndVerify<TSStatus>(rpc);
    }

    void deleteData(const TSDeleteDataReq& request) {
        auto rpc = [this, request]() {
            return this->deleteDataInternal(request);
        };
        callWithRetryAndVerify<TSStatus>(rpc);
    }

    TSStatus deleteDataInternal(TSDeleteDataReq request) {
        request.sessionId = sessionId;
        TSStatus ret;
        client->deleteData(ret, request);
        return ret;
    }

    void setStorageGroup(const string &storageGroupId) {
        auto rpc = [this, &storageGroupId]() {
            TSStatus ret;
            client->setStorageGroup(ret, sessionId, storageGroupId);
            return ret;
        };
        auto ret = callWithRetryAndReconnect<TSStatus>(rpc);
        RpcUtils::verifySuccess(ret.getResult());
    }

    void deleteStorageGroups(const vector<string> &storageGroups) {
        auto rpc = [this, &storageGroups]() {
            TSStatus ret;
            client->deleteStorageGroups(ret, sessionId, storageGroups);
            return ret;
        };
        auto ret = callWithRetryAndReconnect<TSStatus>(rpc);
        RpcUtils::verifySuccess(ret.getResult());
    }

    void createTimeseries(TSCreateTimeseriesReq& req) {
        auto rpc = [this, &req]() {
            TSStatus ret;
            req.sessionId = sessionId;
            client->createTimeseries(ret, req);
            return ret;
        };
        auto ret = callWithRetryAndReconnect<TSStatus>(rpc);
        RpcUtils::verifySuccess(ret.getResult());
    }

    void createMultiTimeseries(TSCreateMultiTimeseriesReq& req) {
        auto rpc = [this, &req]() {
            TSStatus ret;
            req.sessionId = sessionId;
            client->createMultiTimeseries(ret, req);
            return ret;
        };
        auto ret = callWithRetryAndReconnect<TSStatus>(rpc);
        RpcUtils::verifySuccess(ret.getResult());
    }

    void createAlignedTimeseries(TSCreateAlignedTimeseriesReq& req) {
        auto rpc = [this, &req]() {
            TSStatus ret;
            req.sessionId = sessionId;
            client->createAlignedTimeseries(ret, req);
            return ret;
        };
        auto ret = callWithRetryAndReconnect<TSStatus>(rpc);
        RpcUtils::verifySuccess(ret.getResult());
    }

    TSGetTimeZoneResp getTimeZone() {
        auto rpc = [this]() {
            TSGetTimeZoneResp resp;
            client->getTimeZone(resp, sessionId);
            zoneId = resp.timeZone;
            return resp;
        };
        auto ret = callWithRetryAndReconnect<TSGetTimeZoneResp>(rpc,
            [](const TSGetTimeZoneResp& resp) {
            return resp.status;
        });
        RpcUtils::verifySuccess(ret.getResult().status);
        return ret.result;
    }

    void setTimeZone(TSSetTimeZoneReq& req) {
        auto rpc = [this, &req]() {
            TSStatus ret;
            req.sessionId = sessionId;
            client->setTimeZone(ret, req);
            zoneId = req.timeZone;
            return ret;
        };
        auto ret = callWithRetryAndReconnect<TSStatus>(rpc);
        RpcUtils::verifySuccess(ret.getResult());
    }

    void createSchemaTemplate(TSCreateSchemaTemplateReq req) {
        auto rpc = [this, &req]() {
            TSStatus ret;
            req.sessionId = sessionId;
            client->createSchemaTemplate(ret, req);
            return ret;
        };
        auto ret = callWithRetryAndReconnect<TSStatus>(rpc);
        RpcUtils::verifySuccess(ret.getResult());
    }

    void setSchemaTemplate(TSSetSchemaTemplateReq req) {
        auto rpc = [this, &req]() {
            TSStatus ret;
            req.sessionId = sessionId;
            client->setSchemaTemplate(ret, req);
            return ret;
        };
        auto ret = callWithRetryAndReconnect<TSStatus>(rpc);
        RpcUtils::verifySuccess(ret.getResult());
    }

    void unsetSchemaTemplate(TSUnsetSchemaTemplateReq req) {
        auto rpc = [this, &req]() {
            TSStatus ret;
            req.sessionId = sessionId;
            client->unsetSchemaTemplate(ret, req);
            return ret;
        };
        auto ret = callWithRetryAndReconnect<TSStatus>(rpc);
        RpcUtils::verifySuccess(ret.getResult());
    }

    void appendSchemaTemplate(TSAppendSchemaTemplateReq req) {
        auto rpc = [this, &req]() {
            TSStatus ret;
            req.sessionId = sessionId;
            client->appendSchemaTemplate(ret, req);
            return ret;
        };
        auto ret = callWithRetryAndReconnect<TSStatus>(rpc);
        RpcUtils::verifySuccess(ret.getResult());
    }

    void pruneSchemaTemplate(TSPruneSchemaTemplateReq req) {
        auto rpc = [this, &req]() {
            TSStatus ret;
            req.sessionId = sessionId;
            client->pruneSchemaTemplate(ret, req);
            return ret;
        };
        auto ret = callWithRetryAndReconnect<TSStatus>(rpc);
        RpcUtils::verifySuccess(ret.getResult());
    }

    TSQueryTemplateResp querySchemaTemplate(TSQueryTemplateReq req) {
        auto rpc = [this, &req]() {
            TSQueryTemplateResp  ret;
            req.sessionId = sessionId;
            client->querySchemaTemplate(ret, req);
            return ret;
        };
        auto ret = callWithRetryAndReconnect<TSQueryTemplateResp>(rpc,
        [](const TSQueryTemplateResp& resp) {
            return resp.status;
        });
        RpcUtils::verifySuccess(ret.getResult().status);
        return ret.getResult();
    }

    std::unique_ptr<SessionDataSet> executeRawDataQuery(const std::vector<std::string> &paths, int64_t startTime, int64_t endTime);

    std::unique_ptr<SessionDataSet> executeLastDataQuery(const std::vector<std::string> &paths, int64_t lastTime);

    void executeNonQueryStatement(const std::string &sql);

    std::unique_ptr<SessionDataSet> executeQueryStatement(const std::string& sql, int64_t timeoutInMs = -1);

    std::shared_ptr<IClientRPCServiceClient> getSessionClient() {
        return client;
    }

    friend class Session;

private:
    void close();
    std::string getSystemDefaultZoneId();
    bool reconnect();

    template<typename T>
    struct RetryResult {
        T result;
        std::exception_ptr exception;
        int retryAttempts;

        RetryResult(T r, std::exception_ptr e, int a)
            : result(r), exception(e), retryAttempts(a) {}

        int getRetryAttempts() const {return retryAttempts;}
        T getResult() const { return result; }
        std::exception_ptr getException() const { return exception; }
    };

    template<typename T>
    void callWithRetryAndVerifyWithRedirection(std::function<T()> rpc) {
        auto result = callWithRetry<T>(rpc);

        auto status = result.getResult();
        if (result.getRetryAttempts() == 0) {
            RpcUtils::verifySuccessWithRedirection(status);
        } else {
            RpcUtils::verifySuccess(status);
        }

        if (result.getException()) {
            try {
                std::rethrow_exception(result.getException());
            } catch (const std::exception& e) {
                throw IoTDBConnectionException(e.what());
            }
        }
    }

    template<typename T>
    void callWithRetryAndVerifyWithRedirectionForMultipleDevices(
        std::function<T()> rpc, const vector<string>& deviceIds) {
        auto result = callWithRetry<T>(rpc);
        auto status = result.getResult();
        if (result.getRetryAttempts() == 0) {
            RpcUtils::verifySuccessWithRedirectionForMultiDevices(status, deviceIds);
        } else {
            RpcUtils::verifySuccess(status);
        }
        if (result.getException()) {
            try {
                std::rethrow_exception(result.getException());
            } catch (const std::exception& e) {
                throw IoTDBConnectionException(e.what());
            }
        }
        result.exception = nullptr;
    }

    template<typename T>
    RetryResult<T> callWithRetryAndVerify(std::function<T()> rpc) {
        auto result = callWithRetry<T>(rpc);
        RpcUtils::verifySuccess(result.getResult());
        if (result.getException()) {
            try {
                std::rethrow_exception(result.getException());
            } catch (const std::exception& e) {
                throw IoTDBConnectionException(e.what());
            }
        }
        return result;
    }

    template<typename T>
    RetryResult<T> callWithRetry(std::function<T()> rpc) {
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
                } catch (const std::exception& e) {
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
            } catch (...) {
                lastException = std::current_exception();
            }
        }
        return {status, lastException, i};
    }

    template <typename T, typename RpcFunc>
    RetryResult<T> callWithRetryAndReconnect(RpcFunc rpc) {
         return callWithRetryAndReconnect<T>(rpc,
            [](const TSStatus& status) { return status.__isset.needRetry && status.needRetry; },
            [](const TSStatus& status) { return status.code == TSStatusCode::PLAN_FAILED_NETWORK_PARTITION; }
        );
    }

    template <typename T, typename RpcFunc, typename StatusGetter>
    RetryResult<T> callWithRetryAndReconnect(RpcFunc rpc, StatusGetter statusGetter) {
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
    RetryResult<T> callWithRetryAndReconnect(RpcFunc rpc,
                                            ShouldRetry shouldRetry,
                                            ForceReconnect forceReconnect) {
        std::exception_ptr lastException = nullptr;
        T result;
        int retryAttempt;
        for (retryAttempt = 0; retryAttempt <= maxRetryCount; retryAttempt++) {
            try {
                result = rpc();
                lastException = nullptr;
            } catch (...) {
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
            } catch (const std::exception& e) {
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
};

#endif
