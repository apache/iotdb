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

void TableSession::insert(Tablet &tablet, bool sorted = false) {
    session->insertRelationalTablet(tablet, sorted);
}
void TableSession::executeNonQueryStatement(const string &sql) {
    session->executeNonQueryStatement(sql);
}
unique_ptr<SessionDataSet> TableSession::executeQueryStatement(const string &sql) {
    return session->executeQueryStatement(sql);
}
unique_ptr<SessionDataSet> TableSession::executeQueryStatement(const string &sql, int64_t timeoutInMs) {
    return session->executeQueryStatement(sql, timeoutInMs);
}
string TableSession::getDatabase() {
    return session->getDatabase();
}
void TableSession::open(bool enableRPCCompression) {
    session->open(enableRPCCompression);
}
void TableSession::close() {
    session->close();
}