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
#ifndef IOTDB_THRIFTCONNECTION_H
#define IOTDB_THRIFTCONNECTION_H

#include <memory>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TCompactProtocol.h>
#include "IClientRPCService.h"
#include "Session.h"

class ThriftConnection {
public:
    const static int THRIFT_DEFAULT_BUFFER_SIZE = 4096;
    const static int THRIFT_MAX_FRAME_SIZE = 1048576;
    const static int CONNECTION_TIMEOUT_IN_MS = 1000;
private:
    TEndPoint endPoint;

    int thriftDefaultBufferSize;
    int thriftMaxFrameSize;
    int connectionTimeoutInMs;

    std::shared_ptr<TTransport> transport;
    std::shared_ptr<IClientRPCServiceClient> client;
    int64_t sessionId;
    int64_t statementId;
    std::string zoneId;
    int timeFactor;

    void initZoneId() {
        if (!zoneId.empty()) {
            return;
        }

        time_t ts = 0;
        struct tm tmv;
#if defined(_WIN64) || defined (WIN32) || defined (_WIN32)
        localtime_s(&tmv, &ts);
#else
        localtime_r(&ts, &tmv);
#endif

        char zoneStr[32];
        strftime(zoneStr, sizeof(zoneStr), "%z", &tmv);
        zoneId = zoneStr;
    }

public:
    ThriftConnection(const TEndPoint& endPoint,
                    int thriftDefaultBufferSize = THRIFT_DEFAULT_BUFFER_SIZE,
                    int thriftMaxFrameSize = THRIFT_MAX_FRAME_SIZE,
                    int connectionTimeoutInMs = CONNECTION_TIMEOUT_IN_MS)
        : endPoint(endPoint),
          thriftDefaultBufferSize(thriftDefaultBufferSize),
          thriftMaxFrameSize(thriftMaxFrameSize),
          connectionTimeoutInMs(connectionTimeoutInMs) {}

    ~ThriftConnection() {}

    void init(const std::string& username,
             const std::string& password,
             bool enableRPCCompression = false,
             const std::string& zoneId = std::string(),
             const std::string& version = "V_1_0") {
        std::shared_ptr<TSocket> socket(new TSocket(endPoint.ip, endPoint.port));
        socket->setConnTimeout(connectionTimeoutInMs);
        transport = std::make_shared<TFramedTransport>(socket);
        if (!transport->isOpen()) {
            try {
                transport->open();
            }
            catch (TTransportException &e) {
                log_debug(e.what());
                throw IoTDBConnectionException(e.what());
            }
        }
        if (zoneId.empty()) {
            initZoneId();
        } else {
            this->zoneId = zoneId;
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
        TSOpenSessionReq openReq;
        openReq.__set_username(username);
        openReq.__set_password(password);
        openReq.__set_zoneId(this->zoneId);
        openReq.__set_configuration(configuration);

        try {
            TSOpenSessionResp openResp;
            client->openSession(openResp, openReq);
            RpcUtils::verifySuccess(openResp.status);
            if (Session::protocolVersion != openResp.serverProtocolVersion) {
                if (openResp.serverProtocolVersion == 0) {
                    throw IoTDBException(string("Protocol not supported, Client version is ") + to_string(Session::protocolVersion) +
                                      ", but Server version is " + to_string(openResp.serverProtocolVersion));
                }
            }
            sessionId = openResp.sessionId;
            statementId = client->requestStatementId(sessionId);
        } catch (const TTransportException &e) {
            log_debug(e.what());
            transport->close();
            throw IoTDBConnectionException(e.what());
        } catch (const IoTDBException &e) {
            log_debug(e.what());
            transport->close();
            throw IoTDBException(e.what());
        } catch (const exception &e) {
            log_debug(e.what());
            transport->close();
            throw IoTDBException(e.what());
        }
    }

    unique_ptr<SessionDataSet> executeQueryStatement(const string &sql, int64_t timeoutInMs = -1) {
        TSExecuteStatementReq req;
        req.__set_sessionId(sessionId);
        req.__set_statementId(statementId);
        req.__set_statement(sql);
        req.__set_timeout(timeoutInMs);
        TSExecuteStatementResp resp;
        try {
            client->executeStatement(resp, req);
            RpcUtils::verifySuccess(resp.status);
        } catch (const TTransportException &e) {
            log_debug(e.what());
            throw IoTDBConnectionException(e.what());
        } catch (const IoTDBException &e) {
            log_debug(e.what());
            throw IoTDBConnectionException(e.what());;
        }  catch (const exception &e) {
            throw IoTDBException(e.what());
        }
        shared_ptr<TSQueryDataSet> queryDataSet(new TSQueryDataSet(resp.queryDataSet));
        return unique_ptr<SessionDataSet>(new SessionDataSet(
                sql, resp.columns, resp.dataTypeList, resp.columnNameIndexMap, resp.ignoreTimeStamp, resp.queryId,
                statementId, client, sessionId, queryDataSet));
    }

    void close() {
        try {
            if (client) {
                TSCloseSessionReq req;
                req.__set_sessionId(sessionId);
                TSStatus tsStatus;
                client->closeSession(tsStatus, req);
            }
        } catch (const TTransportException &e) {
            log_debug(e.what());
            throw IoTDBConnectionException(e.what());
        } catch (const exception &e) {
            log_debug(e.what());
            throw IoTDBConnectionException(e.what());
        }

        try {
            if (transport->isOpen()) {
                transport->close();
            }
        }
        catch (const exception &e) {
            log_debug(e.what());
            throw IoTDBConnectionException(e.what());
        }
    }
};

#endif
