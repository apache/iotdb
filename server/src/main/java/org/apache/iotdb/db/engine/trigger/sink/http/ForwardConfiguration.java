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
import org.apache.iotdb.db.engine.trigger.sink.exception.SinkException;

import org.fusesource.mqtt.client.QoS;

public class ForwardConfiguration implements Configuration {
  private final String protocol;
  private final boolean stopIfException;

  // ForwardQueue config items
  private final int maxQueueCount;
  private final int maxQueueSize;
  private final int forwardBatchSize;

  // HTTP config items
  private String endpoint;

  // MQTT config items
  private String host;
  private int port;
  private String username;
  private String password;
  private long reconnectDelay;
  private long connectAttemptsMax;
  private QoS qos;
  private boolean retain;

  // TODO support payloadFormatter

  public ForwardConfiguration(
      String protocol,
      boolean stopIfException,
      int maxQueueCount,
      int maxQueueSize,
      int forwardBatchSize) {
    this.protocol = protocol;
    this.stopIfException = stopIfException;

    this.maxQueueCount = maxQueueCount;
    this.maxQueueSize = maxQueueSize;
    this.forwardBatchSize = forwardBatchSize;
  }

  public static void setHTTPConfig(ForwardConfiguration configuration, String endpoint) {
    configuration.setEndpoint(endpoint);
  }

  public static void setMQTTConfig(
      ForwardConfiguration configuration,
      String host,
      int port,
      String username,
      String password,
      long reconnectDelay,
      long connectAttemptsMax,
      String qos,
      boolean retain)
      throws SinkException {
    configuration.setHost(host);
    configuration.setPort(port);
    configuration.setUsername(username);
    configuration.setPassword(password);
    configuration.setReconnectDelay(reconnectDelay);
    configuration.setConnectAttemptsMax(connectAttemptsMax);
    configuration.setQos(parseQoS(qos));
    configuration.setRetain(retain);
  }

  private static QoS parseQoS(String qos) throws SinkException {
    switch (qos.toLowerCase()) {
      case "exactly_once":
        return QoS.EXACTLY_ONCE;
      case "at_least_once":
        return QoS.AT_LEAST_ONCE;
      case "at_most_once":
        return QoS.AT_MOST_ONCE;
      default:
        throw new SinkException("Unable to identify QoS config");
    }
  }

  public void checkHTTPConfig() throws SinkException {
    if (endpoint == null || endpoint.isEmpty()) {
      throw new SinkException("HTTP config item error");
    }
  }

  public void checkMQTTConfig() throws SinkException {
    if (host == null
        || host.isEmpty()
        || port < 0
        || port > 65535
        || username == null
        || username.isEmpty()
        || password == null
        || password.isEmpty()) {
      throw new SinkException("MQTT config item error");
    }
  }

  public String getProtocol() {
    return protocol;
  }

  public boolean isStopIfException() {
    return stopIfException;
  }

  public int getMaxQueueCount() {
    return maxQueueCount;
  }

  public int getMaxQueueSize() {
    return maxQueueSize;
  }

  public int getForwardBatchSize() {
    return forwardBatchSize;
  }

  public String getEndpoint() {
    return endpoint;
  }

  private void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  public String getHost() {
    return host;
  }

  private void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  private void setPort(int port) {
    this.port = port;
  }

  public String getUsername() {
    return username;
  }

  private void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  private void setPassword(String password) {
    this.password = password;
  }

  public long getReconnectDelay() {
    return reconnectDelay;
  }

  private void setReconnectDelay(long reconnectDelay) {
    this.reconnectDelay = reconnectDelay;
  }

  public long getConnectAttemptsMax() {
    return connectAttemptsMax;
  }

  private void setConnectAttemptsMax(long connectAttemptsMax) {
    this.connectAttemptsMax = connectAttemptsMax;
  }

  public QoS getQos() {
    return qos;
  }

  private void setQos(QoS qos) {
    this.qos = qos;
  }

  public boolean isRetain() {
    return retain;
  }

  private void setRetain(boolean retain) {
    this.retain = retain;
  }
}
