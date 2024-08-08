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
package org.apache.iotdb.itbase.runtime;

import org.apache.iotdb.it.framework.IoTDBTestLogger;

import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;

/** this is the connection instance of one single node. */
public class NodeConnection {

  private static final Logger logger = IoTDBTestLogger.logger;
  private final String endpoint;
  private final NodeRole nodeRole;
  private final ConnectionRole connectionRole;
  private final Connection underlyingConnecton;

  public NodeConnection(
      String endpoint,
      NodeRole nodeRole,
      ConnectionRole connectionRole,
      Connection underlyingConnecton) {
    this.endpoint = endpoint;
    this.nodeRole = nodeRole;
    this.connectionRole = connectionRole;
    this.underlyingConnecton = underlyingConnecton;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public NodeRole getNodeRole() {
    return nodeRole;
  }

  public ConnectionRole getConnectionRole() {
    return connectionRole;
  }

  public Connection getUnderlyingConnecton() {
    return underlyingConnecton;
  }

  public void close() {
    try {
      underlyingConnecton.close();
    } catch (SQLException e) {
      logger.error("Close connection {} error", this, e);
    }
  }

  @Override
  public String toString() {
    return String.format("%s-%s@%s", nodeRole, connectionRole, endpoint);
  }

  public enum NodeRole {
    CONFIG_NODE,
    DATA_NODE,
  }

  public enum ConnectionRole {
    READ,
    WRITE,
  }
}
