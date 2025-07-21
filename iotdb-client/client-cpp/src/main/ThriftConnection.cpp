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
#include "ThriftConnection.h"
#include <ctime>
#include <iostream>
#include <thrift/transport/TSocket.h>

#include "Session.h"
#include "SessionDataSet.h"

const int ThriftConnection::THRIFT_DEFAULT_BUFFER_SIZE = 4096;
const int ThriftConnection::THRIFT_MAX_FRAME_SIZE = 1048576;
const int ThriftConnection::CONNECTION_TIMEOUT_IN_MS = 1000;
const int ThriftConnection::DEFAULT_FETCH_SIZE = 10000;

ThriftConnection::ThriftConnection(const TEndPoint& endPoint,
                                   int thriftDefaultBufferSize,
                                   int thriftMaxFrameSize,
                                   int connectionTimeoutInMs,
                                   int fetchSize)
    : endPoint_(endPoint),
      thriftDefaultBufferSize_(thriftDefaultBufferSize),
      thriftMaxFrameSize_(thriftMaxFrameSize),
      connectionTimeoutInMs_(connectionTimeoutInMs),
      fetchSize_(fetchSize){
}

ThriftConnection::~ThriftConnection() = default;

void ThriftConnection::initZoneId() {
    if (!zoneId_.empty()) {
        return;
    }

    time_t ts = 0;
    struct tm tmv{};
#if defined(_WIN64) || defined (WIN32) || defined (_WIN32)
    localtime_s(&tmv, &ts);
#else
    localtime_r(&ts, &tmv);
#endif

    char zoneStr[32];
    strftime(zoneStr, sizeof(zoneStr), "%z", &tmv);
    zoneId_ = zoneStr;
}

void ThriftConnection::init(const std::string& username,
                            const std::string& password,
                            bool enableRPCCompression,
                            const std::string& zoneId,
                            const std::string& version) {
    std::shared_ptr<TSocket> socket(new TSocket(endPoint_.ip, endPoint_.port));
    socket->setConnTimeout(connectionTimeoutInMs_);
    transport_ = std::make_shared<TFramedTransport>(socket);
    if (!transport_->isOpen()) {
        try {
            transport_->open();
        }
        catch (TTransportException& e) {
            throw IoTDBConnectionException(e.what());
        }
    }
    if (zoneId.empty()) {
        initZoneId();
    }
    else {
        this->zoneId_ = zoneId;
    }

    if (enableRPCCompression) {
        std::shared_ptr<TCompactProtocol> protocol(new TCompactProtocol(transport_));
        client_ = std::make_shared<IClientRPCServiceClient>(protocol);
    }
    else {
        std::shared_ptr<TBinaryProtocol> protocol(new TBinaryProtocol(transport_));
        client_ = std::make_shared<IClientRPCServiceClient>(protocol);
    }

    std::map<std::string, std::string> configuration;
    configuration["version"] = version;
    TSOpenSessionReq openReq;
    openReq.__set_username(username);
    openReq.__set_password(password);
    openReq.__set_zoneId(this->zoneId_);
    openReq.__set_configuration(configuration);
    try {
        TSOpenSessionResp openResp;
        client_->openSession(openResp, openReq);
        RpcUtils::verifySuccess(openResp.status);
        sessionId_ = openResp.sessionId;
        statementId_ = client_->requestStatementId(sessionId_);
    }
    catch (const TTransportException& e) {
        transport_->close();
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException& e) {
        transport_->close();
        throw IoTDBException(e.what());
    } catch (const std::exception& e) {
        transport_->close();
        throw IoTDBException(e.what());
    }
}

std::unique_ptr<SessionDataSet> ThriftConnection::executeQueryStatement(const std::string& sql, int64_t timeoutInMs) {
    TSExecuteStatementReq req;
    req.__set_sessionId(sessionId_);
    req.__set_statementId(statementId_);
    req.__set_statement(sql);
    req.__set_timeout(timeoutInMs);
    TSExecuteStatementResp resp;
    try {
        client_->executeQueryStatementV2(resp, req);
        RpcUtils::verifySuccess(resp.status);
    }
    catch (const TTransportException& e) {
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException& e) {
        throw IoTDBConnectionException(e.what());
    } catch (const std::exception& e) {
        throw IoTDBException(e.what());
    }
    std::shared_ptr<TSQueryDataSet> queryDataSet(new TSQueryDataSet(resp.queryDataSet));
    return std::unique_ptr<SessionDataSet>(new SessionDataSet("", resp.columns, resp.dataTypeList,
                                                              resp.columnNameIndexMap, resp.queryId, statementId_,
                                                              client_, sessionId_, resp.queryResult, resp.ignoreTimeStamp,
                                                              connectionTimeoutInMs_, resp.moreData, fetchSize_, zoneId_,
                                                              timeFactor_, resp.columnIndex2TsBlockColumnIndexList));
}

void ThriftConnection::close() {
    try {
        if (client_) {
            TSCloseSessionReq req;
            req.__set_sessionId(sessionId_);
            TSStatus tsStatus;
            client_->closeSession(tsStatus, req);
        }
    }
    catch (const TTransportException& e) {
        throw IoTDBConnectionException(e.what());
    } catch (const std::exception& e) {
        throw IoTDBConnectionException(e.what());
    }

    try {
        if (transport_->isOpen()) {
            transport_->close();
        }
    }
    catch (const std::exception& e) {
        throw IoTDBConnectionException(e.what());
    }
}
