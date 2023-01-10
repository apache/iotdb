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
package org.apache.iotdb.db.query.control.clientsession;

import org.apache.iotdb.service.rpc.thrift.TSConnectionType;

import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/** Client Session is the only identity for a connection. */
public class ClientSession extends IClientSession {

  private final Socket clientSocket;

  private final Map<Long, Set<Long>> statementIdToQueryId = new ConcurrentHashMap<>();

  public ClientSession(Socket clientSocket) {
    this.clientSocket = clientSocket;
  }

  @Override
  public String getClientAddress() {
    return clientSocket.getInetAddress().getHostAddress();
  }

  @Override
  public int getClientPort() {
    return clientSocket.getPort();
  }

  @Override
  TSConnectionType getConnectionType() {
    return TSConnectionType.THRIFT_BASED;
  }

  @Override
  String getConnectionId() {
    return getClientAddress() + ':' + getClientPort();
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
          "StatementId: " + statementId + " doesn't exist in this session " + this);
    }
    queryIds.add(queryId);
  }

  @Override
  public void removeQueryId(Long statementId, Long queryId) {
    Set<Long> queryIds = statementIdToQueryId.get(statementId);
    if (queryIds != null) {
      queryIds.remove(queryId);
    }
  }

  /**
   * shutdownStream will close the socket stream directly, which cause a TTransportException with
   * type = TTransportException.END_OF_FILE. In this case, thrift client thread will be finished
   * asap.
   */
  public void shutdownStream() {
    if (!clientSocket.isInputShutdown()) {
      try {
        clientSocket.shutdownInput();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    if (!clientSocket.isOutputShutdown()) {
      try {
        clientSocket.shutdownOutput();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
