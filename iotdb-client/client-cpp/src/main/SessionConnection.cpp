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
#include "SessionConnection.h"
#include "Session.h"
#include "common_types.h"
#include <thrift/protocol/TCompactProtocol.h>

#include <utility>

#include "SessionDataSet.h"

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

SessionConnection::SessionConnection(Session* session_ptr, const TEndPoint& endpoint,
                                     const std::string& zoneId,
                                     std::shared_ptr<INodesSupplier> nodeSupplier,
                                     int fetchSize,
                                     int maxRetries,
                                     int64_t retryInterval,
                                     int64_t connectionTimeout,
                                     std::string dialect,
                                     std::string db)
    : session(session_ptr),
      zoneId(zoneId),
      endPoint(endpoint),
      availableNodes(std::move(nodeSupplier)),
      fetchSize(fetchSize),
      maxRetryCount(maxRetries),
      retryIntervalMs(retryInterval),
      connectionTimeoutInMs(connectionTimeout),
      sqlDialect(std::move(dialect)),
      database(std::move(db)) {
    this->zoneId = zoneId.empty() ? getSystemDefaultZoneId() : zoneId;
    endPointList.push_back(endpoint);
    init(endPoint);
}

void SessionConnection::close() {
    bool needThrowException = false;
    string errMsg;
    session = nullptr;
    try {
        TSCloseSessionReq req;
        req.__set_sessionId(sessionId);
        TSStatus tsStatus;
        client->closeSession(tsStatus, req);
    }
    catch (const TTransportException& e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const exception& e) {
        log_debug(e.what());
        errMsg = errMsg + "Session::close() client->closeSession() error, maybe remote server is down. " + e.what() +
            "\n";
        needThrowException = true;
    }

    try {
        if (transport->isOpen()) {
            transport->close();
        }
    }
    catch (const exception& e) {
        log_debug(e.what());
        errMsg = errMsg + "Session::close() transport->close() error. " + e.what() + "\n";
        needThrowException = true;
    }

    if (needThrowException) {
        throw IoTDBException(errMsg);
    }
}

SessionConnection::~SessionConnection() {
    try {
        close();
    }
    catch (const exception& e) {
        log_debug(e.what());
    }
}

void SessionConnection::init(const TEndPoint& endpoint) {
    shared_ptr<TSocket> socket(new TSocket(endpoint.ip, endpoint.port));
    transport = std::make_shared<TFramedTransport>(socket);
    socket->setConnTimeout(connectionTimeoutInMs);
    if (!transport->isOpen()) {
        try {
            transport->open();
        }
        catch (TTransportException& e) {
            log_debug(e.what());
            throw IoTDBConnectionException(e.what());
        }
    }
    if (enableRPCCompression) {
        shared_ptr<TCompactProtocol> protocol(new TCompactProtocol(transport));
        client = std::make_shared<IClientRPCServiceClient>(protocol);
    }
    else {
        shared_ptr<TBinaryProtocol> protocol(new TBinaryProtocol(transport));
        client = std::make_shared<IClientRPCServiceClient>(protocol);
    }

    std::map<std::string, std::string> configuration;
    configuration["version"] = session->getVersionString(session->version);
    configuration["sql_dialect"] = sqlDialect;
    if (database != "") {
        configuration["db"] = database;
    }
    TSOpenSessionReq openReq;
    openReq.__set_username(session->username_);
    openReq.__set_password(session->password_);
    openReq.__set_zoneId(zoneId);
    openReq.__set_configuration(configuration);
    try {
        TSOpenSessionResp openResp;
        client->openSession(openResp, openReq);
        RpcUtils::verifySuccess(openResp.status);
        if (session->protocolVersion_ != openResp.serverProtocolVersion) {
            if (openResp.serverProtocolVersion == 0) {
                // less than 0.10
                throw logic_error(string("Protocol not supported, Client version is ") +
                    to_string(session->protocolVersion_) +
                    ", but Server version is " + to_string(openResp.serverProtocolVersion));
            }
        }

        sessionId = openResp.sessionId;
        statementId = client->requestStatementId(sessionId);

        if (!zoneId.empty()) {
            setTimeZone(zoneId);
        }
    }
    catch (const TTransportException& e) {
        log_debug(e.what());
        transport->close();
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException& e) {
        log_debug(e.what());
        transport->close();
        throw;
    } catch (const exception& e) {
        log_debug(e.what());
        transport->close();
        throw;
    }
}

std::unique_ptr<SessionDataSet> SessionConnection::executeQueryStatement(const std::string& sql, int64_t timeoutInMs) {
    TSExecuteStatementReq req;
    req.__set_sessionId(sessionId);
    req.__set_statementId(statementId);
    req.__set_statement(sql);
    req.__set_timeout(timeoutInMs);
    req.__set_enableRedirectQuery(true);

    auto result = callWithRetryAndReconnect<TSExecuteStatementResp>(
        [this, &req]() {
            TSExecuteStatementResp resp;
            client->executeQueryStatementV2(resp, req);
            return resp;
        },
        [](const TSExecuteStatementResp& resp) {
            return resp.status;
        }
    );
    TSExecuteStatementResp resp = result.getResult();
    if (result.getRetryAttempts() == 0) {
        RpcUtils::verifySuccessWithRedirection(resp.status);
    }
    else {
        RpcUtils::verifySuccess(resp.status);
    }

    return std::unique_ptr<SessionDataSet>(new SessionDataSet(sql, resp.columns, resp.dataTypeList,
                                                              resp.columnNameIndexMap, resp.queryId, statementId,
                                                              client, sessionId, resp.queryResult, resp.ignoreTimeStamp,
                                                              connectionTimeoutInMs, resp.moreData, fetchSize, zoneId,
                                                              timeFactor, resp.columnIndex2TsBlockColumnIndexList));
}

std::unique_ptr<SessionDataSet> SessionConnection::executeRawDataQuery(const std::vector<std::string>& paths,
                                                                       int64_t startTime, int64_t endTime) {
    TSRawDataQueryReq req;
    req.__set_sessionId(sessionId);
    req.__set_statementId(statementId);
    req.__set_fetchSize(fetchSize);
    req.__set_paths(paths);
    req.__set_startTime(startTime);
    req.__set_endTime(endTime);
    auto result = callWithRetryAndReconnect<TSExecuteStatementResp>(
        [this, &req]() {
            TSExecuteStatementResp resp;
            client->executeRawDataQueryV2(resp, req);
            return resp;
        },
        [](const TSExecuteStatementResp& resp) {
            return resp.status;
        }
    );
    TSExecuteStatementResp resp = result.getResult();
    if (result.getRetryAttempts() == 0) {
        RpcUtils::verifySuccessWithRedirection(resp.status);
    }
    else {
        RpcUtils::verifySuccess(resp.status);
    }
    return std::unique_ptr<SessionDataSet>(new SessionDataSet("", resp.columns, resp.dataTypeList,
                                                              resp.columnNameIndexMap, resp.queryId, statementId,
                                                              client, sessionId, resp.queryResult, resp.ignoreTimeStamp,
                                                              connectionTimeoutInMs, resp.moreData, fetchSize, zoneId,
                                                              timeFactor, resp.columnIndex2TsBlockColumnIndexList));
}

std::unique_ptr<SessionDataSet> SessionConnection::executeLastDataQuery(const std::vector<std::string>& paths,
                                                                        int64_t lastTime) {
    TSLastDataQueryReq req;
    req.__set_sessionId(sessionId);
    req.__set_statementId(statementId);
    req.__set_fetchSize(fetchSize);
    req.__set_paths(paths);
    req.__set_time(lastTime);

    auto result = callWithRetryAndReconnect<TSExecuteStatementResp>(
        [this, &req]() {
            TSExecuteStatementResp resp;
            client->executeLastDataQuery(resp, req);
            return resp;
        },
        [](const TSExecuteStatementResp& resp) {
            return resp.status;
        }
    );
    TSExecuteStatementResp resp = result.getResult();
    if (result.getRetryAttempts() == 0) {
        RpcUtils::verifySuccessWithRedirection(resp.status);
    }
    else {
        RpcUtils::verifySuccess(resp.status);
    }
    return std::unique_ptr<SessionDataSet>(new SessionDataSet("", resp.columns, resp.dataTypeList,
                                                              resp.columnNameIndexMap, resp.queryId, statementId,
                                                              client, sessionId, resp.queryResult, resp.ignoreTimeStamp,
                                                              connectionTimeoutInMs, resp.moreData, fetchSize, zoneId,
                                                              timeFactor, resp.columnIndex2TsBlockColumnIndexList));
}

void SessionConnection::executeNonQueryStatement(const string& sql) {
    TSExecuteStatementReq req;
    req.__set_sessionId(sessionId);
    req.__set_statementId(statementId);
    req.__set_statement(sql);
    req.__set_timeout(0); //0 means no timeout. This value keep consistent to JAVA SDK.
    TSExecuteStatementResp resp;
    try {
        client->executeUpdateStatementV2(resp, req);
        if (resp.database != "") {
            database = resp.database;
            session->database_ = database;
        }
        RpcUtils::verifySuccess(resp.status);
    }
    catch (const TTransportException& e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException& e) {
        log_debug(e.what());
        throw;
    } catch (const exception& e) {
        throw IoTDBException(e.what());
    }
}

const TEndPoint& SessionConnection::getEndPoint() {
    return endPoint;
}

void SessionConnection::setTimeZone(const std::string& newZoneId) {
    TSSetTimeZoneReq req;
    req.__set_sessionId(sessionId);
    req.__set_timeZone(newZoneId);

    try {
        TSStatus tsStatus;
        client->setTimeZone(tsStatus, req);
        zoneId = newZoneId;
    }
    catch (const TException& e) {
        throw IoTDBConnectionException(e.what());
    }
}

std::string SessionConnection::getSystemDefaultZoneId() {
    time_t ts = 0;
    struct tm tmv{};
#if defined(_WIN64) || defined (WIN32) || defined (_WIN32)
    localtime_s(&tmv, &ts);
#else
    localtime_r(&ts, &tmv);
#endif
    char zoneStr[32];
    strftime(zoneStr, sizeof(zoneStr), "%z", &tmv);
    return zoneStr;
}

bool SessionConnection::reconnect() {
    bool reconnect = false;
    for (int i = 1; i <= 3; i++) {
        if (transport != nullptr) {
            transport->close();
            endPointList = std::move(availableNodes->getEndPointList());
            int currHostIndex = rand() % endPointList.size();
            int tryHostNum = 0;
            for (int j = currHostIndex; j < endPointList.size(); j++) {
                if (tryHostNum == endPointList.size()) {
                    break;
                }
                this->endPoint = endPointList[j];
                if (j == endPointList.size() - 1) {
                    j = -1;
                }
                tryHostNum++;
                try {
                    init(this->endPoint);
                    reconnect = true;
                }
                catch (const IoTDBConnectionException& e) {
                    log_warn("The current node may have been down, connection exception: %s", e.what());
                    continue;
                } catch (exception& e) {
                    log_warn("login in failed, because  %s", e.what());
                }
                break;
            }
        }
        if (reconnect) {
            session->removeBrokenSessionConnection(shared_from_this());
            session->defaultEndPoint_ = this->endPoint;
            session->defaultSessionConnection_ = shared_from_this();
            session->endPointToSessionConnection.insert(make_pair(this->endPoint, shared_from_this()));
        }
    }
    return reconnect;
}

void SessionConnection::insertStringRecord(const TSInsertStringRecordReq& request) {
    auto rpc = [this, request]() {
        return this->insertStringRecordInternal(request);
    };
    callWithRetryAndVerifyWithRedirection<TSStatus>(rpc);
}

void SessionConnection::insertRecord(const TSInsertRecordReq& request) {
    auto rpc = [this, request]() {
        return this->insertRecordInternal(request);
    };
    callWithRetryAndVerifyWithRedirection<TSStatus>(rpc);
}

void SessionConnection::insertStringRecords(const TSInsertStringRecordsReq& request) {
    auto rpc = [this, request]() {
        return this->insertStringRecordsInternal(request);
    };
    callWithRetryAndVerifyWithRedirection<TSStatus>(rpc);
}

void SessionConnection::insertRecords(const TSInsertRecordsReq& request) {
    auto rpc = [this, request]() {
        return this->insertRecordsInternal(request);
    };
    callWithRetryAndVerifyWithRedirection<TSStatus>(rpc);
}

void SessionConnection::insertRecordsOfOneDevice(TSInsertRecordsOfOneDeviceReq request) {
    auto rpc = [this, request]() {
        return this->insertRecordsOfOneDeviceInternal(request);
    };
    callWithRetryAndVerifyWithRedirection<TSStatus>(rpc);
}

void SessionConnection::insertStringRecordsOfOneDevice(TSInsertStringRecordsOfOneDeviceReq request) {
    auto rpc = [this, request]() {
        return this->insertStringRecordsOfOneDeviceInternal(request);
    };
    callWithRetryAndVerifyWithRedirection<TSStatus>(rpc);
}

void SessionConnection::insertTablet(TSInsertTabletReq request) {
    auto rpc = [this, request]() {
        return this->insertTabletInternal(request);
    };
    callWithRetryAndVerifyWithRedirection<TSStatus>(rpc);
}

void SessionConnection::insertTablets(TSInsertTabletsReq request) {
    auto rpc = [this, request]() {
        return this->insertTabletsInternal(request);
    };
    callWithRetryAndVerifyWithRedirection<TSStatus>(rpc);
}

void SessionConnection::testInsertStringRecord(TSInsertStringRecordReq& request) {
    auto rpc = [this, &request]() {
        request.sessionId = sessionId;
        TSStatus ret;
        client->testInsertStringRecord(ret, request);
        return ret;
    };
    auto status = callWithRetryAndReconnect<TSStatus>(rpc).getResult();
    RpcUtils::verifySuccess(status);
}

void SessionConnection::testInsertTablet(TSInsertTabletReq& request) {
    auto rpc = [this, &request]() {
        request.sessionId = sessionId;
        TSStatus ret;
        client->testInsertTablet(ret, request);
        return ret;
    };
    auto status = callWithRetryAndReconnect<TSStatus>(rpc).getResult();
    RpcUtils::verifySuccess(status);
}

void SessionConnection::testInsertRecords(TSInsertRecordsReq& request) {
    auto rpc = [this, &request]() {
        request.sessionId = sessionId;
        TSStatus ret;
        client->testInsertRecords(ret, request);
        return ret;
    };
    auto status = callWithRetryAndReconnect<TSStatus>(rpc).getResult();
    RpcUtils::verifySuccess(status);
}

void SessionConnection::deleteTimeseries(const vector<string>& paths) {
    auto rpc = [this, &paths]() {
        TSStatus ret;
        client->deleteTimeseries(ret, sessionId, paths);
        return ret;
    };
    callWithRetryAndVerify<TSStatus>(rpc);
}

void SessionConnection::deleteData(const TSDeleteDataReq& request) {
    auto rpc = [this, request]() {
        return this->deleteDataInternal(request);
    };
    callWithRetryAndVerify<TSStatus>(rpc);
}

void SessionConnection::setStorageGroup(const string& storageGroupId) {
    auto rpc = [this, &storageGroupId]() {
        TSStatus ret;
        client->setStorageGroup(ret, sessionId, storageGroupId);
        return ret;
    };
    auto ret = callWithRetryAndReconnect<TSStatus>(rpc);
    RpcUtils::verifySuccess(ret.getResult());
}

void SessionConnection::deleteStorageGroups(const vector<string>& storageGroups) {
    auto rpc = [this, &storageGroups]() {
        TSStatus ret;
        client->deleteStorageGroups(ret, sessionId, storageGroups);
        return ret;
    };
    auto ret = callWithRetryAndReconnect<TSStatus>(rpc);
    RpcUtils::verifySuccess(ret.getResult());
}

void SessionConnection::createTimeseries(TSCreateTimeseriesReq& req) {
    auto rpc = [this, &req]() {
        TSStatus ret;
        req.sessionId = sessionId;
        client->createTimeseries(ret, req);
        return ret;
    };
    auto ret = callWithRetryAndReconnect<TSStatus>(rpc);
    RpcUtils::verifySuccess(ret.getResult());
}

void SessionConnection::createMultiTimeseries(TSCreateMultiTimeseriesReq& req) {
    auto rpc = [this, &req]() {
        TSStatus ret;
        req.sessionId = sessionId;
        client->createMultiTimeseries(ret, req);
        return ret;
    };
    auto ret = callWithRetryAndReconnect<TSStatus>(rpc);
    RpcUtils::verifySuccess(ret.getResult());
}

void SessionConnection::createAlignedTimeseries(TSCreateAlignedTimeseriesReq& req) {
    auto rpc = [this, &req]() {
        TSStatus ret;
        req.sessionId = sessionId;
        client->createAlignedTimeseries(ret, req);
        return ret;
    };
    auto ret = callWithRetryAndReconnect<TSStatus>(rpc);
    RpcUtils::verifySuccess(ret.getResult());
}

TSGetTimeZoneResp SessionConnection::getTimeZone() {
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

void SessionConnection::setTimeZone(TSSetTimeZoneReq& req) {
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

void SessionConnection::createSchemaTemplate(TSCreateSchemaTemplateReq req) {
    auto rpc = [this, &req]() {
        TSStatus ret;
        req.sessionId = sessionId;
        client->createSchemaTemplate(ret, req);
        return ret;
    };
    auto ret = callWithRetryAndReconnect<TSStatus>(rpc);
    RpcUtils::verifySuccess(ret.getResult());
}

void SessionConnection::setSchemaTemplate(TSSetSchemaTemplateReq req) {
    auto rpc = [this, &req]() {
        TSStatus ret;
        req.sessionId = sessionId;
        client->setSchemaTemplate(ret, req);
        return ret;
    };
    auto ret = callWithRetryAndReconnect<TSStatus>(rpc);
    RpcUtils::verifySuccess(ret.getResult());
}

void SessionConnection::unsetSchemaTemplate(TSUnsetSchemaTemplateReq req) {
    auto rpc = [this, &req]() {
        TSStatus ret;
        req.sessionId = sessionId;
        client->unsetSchemaTemplate(ret, req);
        return ret;
    };
    auto ret = callWithRetryAndReconnect<TSStatus>(rpc);
    RpcUtils::verifySuccess(ret.getResult());
}

void SessionConnection::appendSchemaTemplate(TSAppendSchemaTemplateReq req) {
    auto rpc = [this, &req]() {
        TSStatus ret;
        req.sessionId = sessionId;
        client->appendSchemaTemplate(ret, req);
        return ret;
    };
    auto ret = callWithRetryAndReconnect<TSStatus>(rpc);
    RpcUtils::verifySuccess(ret.getResult());
}

void SessionConnection::pruneSchemaTemplate(TSPruneSchemaTemplateReq req) {
    auto rpc = [this, &req]() {
        TSStatus ret;
        req.sessionId = sessionId;
        client->pruneSchemaTemplate(ret, req);
        return ret;
    };
    auto ret = callWithRetryAndReconnect<TSStatus>(rpc);
    RpcUtils::verifySuccess(ret.getResult());
}

TSQueryTemplateResp SessionConnection::querySchemaTemplate(TSQueryTemplateReq req) {
    auto rpc = [this, &req]() {
        TSQueryTemplateResp ret;
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

TSStatus SessionConnection::insertStringRecordInternal(TSInsertStringRecordReq request) {
    request.sessionId = sessionId;
    TSStatus ret;
    client->insertStringRecord(ret, request);
    return ret;
}

TSStatus SessionConnection::insertRecordInternal(TSInsertRecordReq request) {
    request.sessionId = sessionId;
    TSStatus ret;
    client->insertRecord(ret, request);
    return ret;
}

TSStatus SessionConnection::insertStringRecordsInternal(TSInsertStringRecordsReq request) {
    request.sessionId = sessionId;
    TSStatus ret;
    client->insertStringRecords(ret, request);
    return ret;
}

TSStatus SessionConnection::insertRecordsInternal(TSInsertRecordsReq request) {
    request.sessionId = sessionId;
    TSStatus ret;
    client->insertRecords(ret, request);
    return ret;
}

TSStatus SessionConnection::insertRecordsOfOneDeviceInternal(TSInsertRecordsOfOneDeviceReq request) {
    request.sessionId = sessionId;
    TSStatus ret;
    client->insertRecordsOfOneDevice(ret, request);
    return ret;
}

TSStatus SessionConnection::insertStringRecordsOfOneDeviceInternal(TSInsertStringRecordsOfOneDeviceReq request) {
    request.sessionId = sessionId;
    TSStatus ret;
    client->insertStringRecordsOfOneDevice(ret, request);
    return ret;
}

TSStatus SessionConnection::insertTabletInternal(TSInsertTabletReq request) {
    request.sessionId = sessionId;
    TSStatus ret;
    client->insertTablet(ret, request);
    return ret;
}

TSStatus SessionConnection::insertTabletsInternal(TSInsertTabletsReq request) {
    request.sessionId = sessionId;
    TSStatus ret;
    client->insertTablets(ret, request);
    return ret;
}

TSStatus SessionConnection::deleteDataInternal(TSDeleteDataReq request) {
    request.sessionId = sessionId;
    TSStatus ret;
    client->deleteData(ret, request);
    return ret;
}
