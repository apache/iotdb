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

#include "TableSession.h"
#include "SessionDataSet.h"

void TableSession::insert(Tablet& tablet, bool sorted) {
  session_->insertRelationalTablet(tablet, sorted);
}
void TableSession::executeNonQueryStatement(const string& sql) {
  session_->executeNonQueryStatement(sql);
}
unique_ptr<SessionDataSet> TableSession::executeQueryStatement(const string& sql) {
  return session_->executeQueryStatement(sql);
}
unique_ptr<SessionDataSet> TableSession::executeQueryStatement(const string& sql,
                                                               int64_t timeoutInMs) {
  return session_->executeQueryStatement(sql, timeoutInMs);
}

int32_t TableSession::prepareStatement(const std::string& sql, const std::string& statementName) {
    return session_->prepareStatementMayRedirect(sql, statementName);
}

unique_ptr<SessionDataSet> TableSession::executePreparedStatement(const std::string& sqlForDisplay,
                                                                  const std::string& statementName,
                                                                  const std::string& parametersBinary,
                                                                  int64_t timeoutInMs) {
    return session_->executePreparedStatementMayRedirect(sqlForDisplay, statementName, parametersBinary, timeoutInMs);
}

unique_ptr<SessionDataSet> TableSession::executePreparedStatement(
    const std::string& sqlForDisplay, const std::string& statementName,
    const std::vector<iotdb::prepared::ParamSlot>& params, int64_t timeoutInMs) {
    return executePreparedStatement(
        sqlForDisplay, statementName, iotdb::prepared::serializeParameters(params), timeoutInMs);
}

void TableSession::deallocatePreparedStatement(const std::string& statementName) {
    session_->deallocatePreparedStatementMayRedirect(statementName);
}

string TableSession::getDatabase() {
  return session_->getDatabase();
}
void TableSession::open(bool enableRPCCompression) {
  session_->open(enableRPCCompression);
}
void TableSession::close() {
  session_->close();
}