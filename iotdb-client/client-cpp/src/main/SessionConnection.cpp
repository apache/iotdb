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

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

SessionConnection::SessionConnection(const TEndPoint& endpoint,
                     const std::string& zoneId,
                     std::function<std::vector<TEndPoint>()> nodeSupplier,
                     int maxRetries,
                     int64_t retryInterval,
                     std::string dialect,
                     std::string db,
                     std::string version,
                     std::string username,
                     std::string password,
                     TSProtocolVersion::type protocolVersion)
    : zoneId(zoneId),
      endPoint(endpoint),
      availableNodes(std::move(nodeSupplier)),
      maxRetryCount(maxRetries),
      retryIntervalMs(retryInterval),
      sqlDialect(dialect),
      database(db),
      version(version),
      userName(username),
      password(password),
      protocolVersion(protocolVersion){
    this->zoneId = zoneId.empty() ? getSystemDefaultZoneId() : zoneId;
    endPointList.push_back(endpoint);
}

void SessionConnection::close() {
    bool needThrowException = false;
    string errMsg;
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
    configuration["version"] = version;
    // configuration["sql_dialect"] = sqlDialect;
    // if (database != "") {
    //     configuration["db"] = database;
    // }

    TSOpenSessionReq openReq;
    openReq.__set_username(userName);
    openReq.__set_password(password);
    openReq.__set_zoneId(zoneId);
    openReq.__set_configuration(configuration);
    std::cout << endPoint << " " << userName << " " << password << " " << zoneId << std::endl;
    try {
        TSOpenSessionResp openResp;
        client->openSession(openResp, openReq);
        RpcUtils::verifySuccess(openResp.status);
        if (protocolVersion != openResp.serverProtocolVersion) {
            if (openResp.serverProtocolVersion == 0) {// less than 0.10
                throw logic_error(string("Protocol not supported, Client version is ") + to_string(protocolVersion) +
                                  ", but Server version is " + to_string(openResp.serverProtocolVersion));
            }
        }

        sessionId = openResp.sessionId;
        statementId = client->requestStatementId(sessionId);

        if (!zoneId.empty()) {
            setTimeZone(zoneId);
        }
        inited = true;
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
        throw IoTDBException(e.what());
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
        throw IoTDBConnectionException(e.what());
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
