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

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

SessionConnection::SessionConnection(Session* session_ptr, const TEndPoint& endpoint,
                     const std::string& zoneId,
                     std::shared_ptr<INodesSupplier> nodeSupplier,
                     int maxRetries,
                     int64_t retryInterval,
                     std::string dialect,
                     std::string db)
    : session(session_ptr),
      zoneId(zoneId),
      endPoint(endpoint),
      availableNodes(std::move(nodeSupplier)),
      maxRetryCount(maxRetries),
      retryIntervalMs(retryInterval),
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
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const exception &e) {
        log_debug(e.what());
        errMsg = errMsg + "Session::close() client->closeSession() error, maybe remote server is down. " + e.what() + "\n" ;
        needThrowException = true;
    }

    try {
        if (transport->isOpen()) {
            transport->close();
        }
    }
    catch (const exception &e) {
        log_debug(e.what());
        errMsg = errMsg + "Session::close() transport->close() error. " + e.what() + "\n" ;
        needThrowException = true;
    }

    if (needThrowException) {
        throw IoTDBException(errMsg);
    }
}

SessionConnection::~SessionConnection() {
    try {
        close();
    } catch (const exception &e) {
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
        catch (TTransportException &e) {
            log_debug(e.what());
            throw IoTDBConnectionException(e.what());
        }
    }
    if (enableRPCCompression) {
        shared_ptr<TCompactProtocol> protocol(new TCompactProtocol(transport));
        client = std::make_shared<IClientRPCServiceClient>(protocol);
    } else {
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
    openReq.__set_username(session->username);
    openReq.__set_password(session->password);
    openReq.__set_zoneId(zoneId);
    openReq.__set_configuration(configuration);
    try {
        TSOpenSessionResp openResp;
        client->openSession(openResp, openReq);
        RpcUtils::verifySuccess(openResp.status);
        if (session->protocolVersion != openResp.serverProtocolVersion) {
            if (openResp.serverProtocolVersion == 0) {// less than 0.10
                throw logic_error(string("Protocol not supported, Client version is ") +
                    to_string(session->protocolVersion) +
                    ", but Server version is " + to_string(openResp.serverProtocolVersion));
            }
        }

        sessionId = openResp.sessionId;
        statementId = client->requestStatementId(sessionId);

        if (!zoneId.empty()) {
            setTimeZone(zoneId);
        }
    } catch (const TTransportException &e) {
        log_debug(e.what());
        transport->close();
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException &e) {
        log_debug(e.what());
        transport->close();
        throw;
    } catch (const exception &e) {
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
    TSExecuteStatementResp resp;
    try {
        client->executeStatement(resp, req);
        RpcUtils::verifySuccessWithRedirection(resp.status);
    } catch (const TException &e) {
        log_debug(e.what());
        if (reconnect()) {
            try {
                req.__set_sessionId(sessionId);
                req.__set_statementId(statementId);
                client->executeStatement(resp, req);
            } catch (TException &e) {
                throw IoTDBConnectionException(e.what());
            }
        } else {
            throw IoTDBConnectionException(e.what());
        }
    }
    std::shared_ptr<TSQueryDataSet> queryDataSet(new TSQueryDataSet(resp.queryDataSet));
    return std::unique_ptr<SessionDataSet>(new SessionDataSet(
            sql, resp.columns, resp.dataTypeList, resp.columnNameIndexMap, resp.ignoreTimeStamp, resp.queryId,
            statementId, client, sessionId, queryDataSet));
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
    } catch (const TException& e) {
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
                } catch (const IoTDBConnectionException &e) {
                    log_warn("The current node may have been down, connection exception: %s", e.what());
                    continue;
                } catch (exception &e) {
                    log_warn("login in failed, because  %s", e.what());
                }
                break;
            }
        }
        if (reconnect) {
            session->removeBrokenSessionConnection(shared_from_this());
            session->defaultEndPoint = this->endPoint;
            session->defaultSessionConnection = shared_from_this();
            session->endPointToSessionConnection.insert(make_pair(this->endPoint, shared_from_this()));
        }
    }
    return reconnect;
}