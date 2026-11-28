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

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The session of a client that reaches the database through an external service speaking another
 * wire protocol (for example the PostgreSQL wire protocol). Like a client-port session it carries
 * statement and query ids and prepared statements, and it logs in through {@link
 * SessionManager#login} like any other client.
 */
public final class ExternalServiceClientSession extends IClientSession {

  private final String protocol;
  private final InetSocketAddress remote;
  private final Map<Long, Set<Long>> statementIdToQueryId = new ConcurrentHashMap<>();
  private final Map<String, PreparedStatementInfo> preparedStatements = new ConcurrentHashMap<>();

  /**
   * @param protocol the service's protocol, e.g. {@code postgresql}
   * @param remote the client's address and port
   */
  public ExternalServiceClientSession(String protocol, InetSocketAddress remote) {
    this.protocol = Objects.requireNonNull(protocol, "protocol");
    this.remote = Objects.requireNonNull(remote, "remote");
  }

  /** The service's protocol, e.g. {@code postgresql}. */
  public String getProtocol() {
    return protocol;
  }

  @Override
  public String getClientAddress() {
    return remote.getAddress() != null
        ? remote.getAddress().getHostAddress()
        : remote.getHostString();
  }

  @Override
  public int getClientPort() {
    return remote.getPort();
  }

  /**
   * A network client like the client port's. The Thrift enum has no value for other protocols, and
   * adding one would change the client IDL; the connection id carries the protocol name.
   */
  @Override
  TSConnectionType getConnectionType() {
    return TSConnectionType.THRIFT_BASED;
  }

  @Override
  String getConnectionId() {
    return protocol + ':' + getClientAddress() + ':' + getClientPort();
  }

  @Override
  public Set<Long> getStatementIds() {
    return statementIdToQueryId.keySet();
  }

  @Override
  public void addStatementId(long statementId) {
    statementIdToQueryId.computeIfAbsent(statementId, sid -> ConcurrentHashMap.newKeySet());
  }

  @Override
  public Set<Long> removeStatementId(long statementId) {
    return statementIdToQueryId.remove(statementId);
  }

  @Override
  public void addQueryId(Long statementId, long queryId) {
    statementIdToQueryId
        .computeIfAbsent(statementId, sid -> ConcurrentHashMap.newKeySet())
        .add(queryId);
  }

  @Override
  public boolean containsQueryId(Long statementId, long queryId) {
    return ClientSession.containsQueryId(statementIdToQueryId, statementId, queryId);
  }

  @Override
  public void removeQueryId(Long statementId, Long queryId) {
    ClientSession.removeQueryId(statementIdToQueryId, statementId, queryId);
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
