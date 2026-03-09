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

package org.apache.iotdb.db.protocol.session;

import org.apache.iotdb.service.rpc.thrift.TSConnectionType;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RestClientSession extends IClientSession {

  private final String clientID;

  // Map from statement name to PreparedStatementInfo
  private final Map<String, PreparedStatementInfo> preparedStatements = new ConcurrentHashMap<>();

  public RestClientSession(String clientID) {
    this.clientID = clientID;
  }

  @Override
  public String getClientAddress() {
    return clientID;
  }

  @Override
  public int getClientPort() {
    return 0;
  }

  @Override
  TSConnectionType getConnectionType() {
    return TSConnectionType.REST_BASED;
  }

  @Override
  String getConnectionId() {
    return clientID;
  }

  @Override
  public Iterable<Long> getStatementIds() {
    return Collections.emptySet();
  }

  @Override
  public void addStatementId(long statementId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<Long> removeStatementId(long statementId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addQueryId(Long statementId, long queryId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeQueryId(Long statementId, Long queryId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addPreparedStatement(String statementName, PreparedStatementInfo info) {
    preparedStatements.put(statementName, info);
  }

  @Override
  public PreparedStatementInfo removePreparedStatement(String statementName) {
    return preparedStatements.remove(statementName);
  }

  @Override
  public PreparedStatementInfo getPreparedStatement(String statementName) {
    return preparedStatements.get(statementName);
  }

  @Override
  public Set<String> getPreparedStatementNames() {
    return preparedStatements.keySet();
  }
}
