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

// This file is a translation of the Java file iotdb-client/session/src/main/java/org/apache/iotdb/session/TableSession.java

#ifndef IOTDB_TABLESESSION_H
#define IOTDB_TABLESESSION_H

#include "PreparedParameterBinary.h"
#include "Session.h"

class TableSession {
private:
    std::shared_ptr<Session> session_;
    string getDatabase();
public:
    TableSession(std::shared_ptr<Session> session) {
        this->session_ = session;
    }
    ~TableSession() {}

    void insert(Tablet& tablet, bool sorted = false);
    void executeNonQueryStatement(const std::string& sql);
    unique_ptr<SessionDataSet> executeQueryStatement(const std::string& sql);
    unique_ptr<SessionDataSet> executeQueryStatement(const std::string& sql, int64_t timeoutInMs);

    /**
     * Prepare a table-model SQL with '?' placeholders on server side.
     *
     * @param sql SQL text for server execution.
     * @param statementName user-defined unique identifier in this session.
     * @return parameter count (number of '?' placeholders).
     */
    int32_t prepareStatement(const std::string& sql, const std::string& statementName);

    /**
     * Execute a prepared statement with already-serialized binary parameters.
     *
     * @param sqlForDisplay SQL text for logs / tracing.
     * @param statementName prepared statement identifier returned by prepare flow.
     * @param parametersBinary binary payload that matches server prepared-parameter codec.
     * @param timeoutInMs query timeout in milliseconds; -1 means server default behavior.
     */
    unique_ptr<SessionDataSet> executePreparedStatement(const std::string& sqlForDisplay,
                                                        const std::string& statementName,
                                                        const std::string& parametersBinary,
                                                        int64_t timeoutInMs = -1);
    /**
     * Execute a prepared statement with typed parameter slots.
     * This overload serializes params internally to reduce caller-side boilerplate.
     */
    unique_ptr<SessionDataSet> executePreparedStatement(
        const std::string& sqlForDisplay, const std::string& statementName,
        const std::vector<iotdb::prepared::ParamSlot>& params,
        int64_t timeoutInMs = -1);

    /** Deallocate a server-side prepared statement by name. */
    void deallocatePreparedStatement(const std::string& statementName);
    void open(bool enableRPCCompression = false);
    void close();
};

#endif // IOTDB_TABLESESSION_H