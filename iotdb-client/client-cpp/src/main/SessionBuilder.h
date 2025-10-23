/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef IOTDB_SESSION_BUILDER_H
#define IOTDB_SESSION_BUILDER_H

#include "AbstractSessionBuilder.h"

class SessionBuilder : public AbstractSessionBuilder {
public:
    SessionBuilder* host(const std::string &host) {
        AbstractSessionBuilder::host = host;
        return this;
    }

    SessionBuilder* rpcPort(int rpcPort) {
        AbstractSessionBuilder::rpcPort = rpcPort;
        return this;
    }

    SessionBuilder* username(const std::string &username) {
        AbstractSessionBuilder::username = username;
        return this;
    }

    SessionBuilder* password(const std::string &password) {
        AbstractSessionBuilder::password = password;
        return this;
    }

    SessionBuilder* zoneId(const std::string &zoneId) {
        AbstractSessionBuilder::zoneId = zoneId;
        return this;
    }

    SessionBuilder* fetchSize(int fetchSize) {
        AbstractSessionBuilder::fetchSize = fetchSize;
        return this;
    }

    SessionBuilder* database(const std::string &database) {
        AbstractSessionBuilder::database = database;
        return this;
    }

    SessionBuilder* nodeUrls(const std::vector<std::string>& nodeUrls) {
        AbstractSessionBuilder::nodeUrls = nodeUrls;
        return this;
    }

    SessionBuilder* enableAutoFetch(bool enableAutoFetch) {
        AbstractSessionBuilder::enableAutoFetch = enableAutoFetch;
        return this;
    }

    SessionBuilder* enableRedirections(bool enableRedirections) {
        AbstractSessionBuilder::enableRedirections = enableRedirections;
        return this;
    }

    SessionBuilder* enableRPCCompression(bool enableRPCCompression) {
        AbstractSessionBuilder::enableRPCCompression = enableRPCCompression;
        return this;
    }

   std::shared_ptr<Session> build() {
        sqlDialect = "tree";
        auto newSession = std::make_shared<Session>(this);
        newSession->open(false);
        return newSession;
   }
};

#endif // IOTDB_SESSION_BUILDER_H