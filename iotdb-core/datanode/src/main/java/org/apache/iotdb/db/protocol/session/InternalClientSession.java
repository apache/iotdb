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
import java.util.concurrent.CopyOnWriteArraySet;

/** For Internal usage, like CQ and Select Into. */
public class InternalClientSession extends IClientSession {

  // For CQ, it will be cq_id
  // For Select Into, it will be SELECT_INTO constant string
  private final String clientID;

  private final Map<Long, Set<Long>> statementIdToQueryId = new ConcurrentHashMap<>();

  public InternalClientSession(String clientID) {
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
    return TSConnectionType.INTERNAL;
  }

  @Override
  String getConnectionId() {
    return clientID;
  }

  @Override
  public Set<Long> getStatementIds() {
    return statementIdToQueryId.keySet();
  }

  @Override
  public void addStatementId(long statementId) {
    statementIdToQueryId.computeIfAbsent(statementId, sid -> new CopyOnWriteArraySet<>());
  }

  @Override
  public Set<Long> removeStatementId(long statementId) {
    return statementIdToQueryId.remove(statementId);
  }

  @Override
  public void addQueryId(Long statementId, long queryId) {
    Set<Long> queryIds = statementIdToQueryId.get(statementId);
    if (queryIds == null) {
      throw new IllegalStateException(
          "StatementId: " + statementId + "doesn't exist in this session " + this);
    }
    queryIds.add(queryId);
  }

  @Override
  public void removeQueryId(Long statementId, Long queryId) {
    ClientSession.removeQueryId(statementIdToQueryId, statementId, queryId);
  }

  @Override
  public void addPreparedStatement(String statementName, PreparedStatementInfo info) {
    throw new UnsupportedOperationException(
        "InternalClientSession should never call PREPARE statement methods.");
  }

  @Override
  public PreparedStatementInfo removePreparedStatement(String statementName) {
    throw new UnsupportedOperationException(
        "InternalClientSession should never call PREPARE statement methods.");
  }

  @Override
  public PreparedStatementInfo getPreparedStatement(String statementName) {
    throw new UnsupportedOperationException(
        "InternalClientSession should never call PREPARE statement methods.");
  }

  @Override
  public Set<String> getPreparedStatementNames() {
    return Collections.emptySet();
  }
}
