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

// This file is a translation of the Java file iotdb-client/session/src/main/java/org/apache/iotdb/session/TableSessionBuilder.java

#include "TableSession.h"
#include "AbstractSessionBuilder.h"
#include <string>

class TableSessionBuiler : public AbstractSessionBuilder {
    /*
        std::string host;
        int rpcPort;
        std::string username;
        std::string password;
        std::string zoneId;
        int fetchSize;
        std::string sqlDialect = "tree"; // default sql dialect
        std::string database;
    */
public:
    inline TableSessionBuiler* host(const std::string &host) {
        this->host = host;
        return this;
    }
    inline TableSessionBuiler* rpcPort(int rpcPort) {
        this->rpcPort = rpcPort;
        return this;
    }
    inline TableSessionBuiler* username(const std::string &username) {
        this->username = username;
        return this;
    }
    inline TableSessionBuiler* password(const std::string &password) {
        this->password = password;
        return this;
    }
    inline TableSessionBuiler* zoneId(const std::string &zoneId) {
        this->zoneId = zoneId;
        return this;
    }
    inline TableSessionBuiler* fetchSize(int fetchSize) {
        this->fetchSize = fetchSize;
        return this;
    }
    inline TableSessionBuiler* database(const std::string &database) {
        this->database = database;
        return this;
    }
    inline TableSession* build() {
        this->sqlDialect = "table";
        Session* newSession = new Session(this);
        newSession->open(false);
        return new TableSession(newSession);
    }
}