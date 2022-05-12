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

package org.apache.iotdb.db.engine.trigger.sink.http;

import org.apache.iotdb.db.engine.trigger.sink.api.Configuration;

public class ForwardConfiguration implements Configuration {
  private final String protocol;
  private final boolean stopIfException;

  // HTTP config items
  private String endpoint;

  // MQTT config items
  private String host;
  private int port;
  private String username;
  private String password;
  private long reconnectDelay;
  private long connectAttemptsMax;

  public ForwardConfiguration(String protocol, boolean stopIfException) {
    this.protocol = protocol;
    this.stopIfException = stopIfException;
  }

  public ForwardConfiguration(String protocol, boolean stopIfException, String endpoint) {
    this(protocol, stopIfException);
    this.endpoint = endpoint;
  }

  public ForwardConfiguration(
      String protocol,
      boolean stopIfException,
      String host,
      int port,
      String username,
      String password,
      long reconnectDelay,
      long connectAttemptsMax) {
    this(protocol, stopIfException);
    this.host = host;
    this.port = port;
    this.username = username;
    this.password = password;
    this.reconnectDelay = reconnectDelay;
    this.connectAttemptsMax = connectAttemptsMax;
  }

  public ForwardConfiguration(
      String protocol,
      boolean stopIfException,
      String host,
      int port,
      String username,
      String password) {
    this(protocol, stopIfException, host, port, username, password, 10L, 3L);
  }

  public String getProtocol() {
    return protocol;
  }

  public boolean isStopIfException() {
    return stopIfException;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public long getReconnectDelay() {
    return reconnectDelay;
  }

  public long getConnectAttemptsMax() {
    return connectAttemptsMax;
  }
}
