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

package org.apache.iotdb.db.engine.trigger.sink.mqtt;

import org.apache.iotdb.db.engine.trigger.sink.api.Configuration;
import org.apache.iotdb.db.engine.trigger.sink.exception.SinkException;
import org.fusesource.mqtt.client.QoS;

public class MQTTForwardConfiguration implements Configuration {

  private String host;
  private int port;
  private String username;
  private String password;
  private String topic;
  private long reconnectDelay;
  private long connectAttemptsMax;
  private QoS qos;
  private boolean retain;
  private final boolean stopIfException;

  // TODO support payloadFormatter

  public MQTTForwardConfiguration(
      String host,
      int port,
      String username,
      String password,
      String topic,
      long reconnectDelay,
      long connectAttemptsMax,
      String qos,
      boolean retain,
      boolean stopIfException)
      throws SinkException {
    this.host = host;
    this.port = port;
    this.username = username;
    this.password = password;
    this.topic = topic;
    this.reconnectDelay = reconnectDelay;
    this.connectAttemptsMax = connectAttemptsMax;
    this.qos = parseQoS(qos);
    this.retain = retain;
    this.stopIfException = stopIfException;
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

  public void checkConfig() throws SinkException {
    if (host == null
        || host.isEmpty()
        || port < 0
        || port > 65535
        || username == null
        || username.isEmpty()
        || password == null
        || password.isEmpty()
        || topic == null
        || topic.isEmpty()) {
      throw new SinkException("MQTT config item error");
    }
  }

  public boolean isStopIfException() {
    return stopIfException;
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

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }
}
