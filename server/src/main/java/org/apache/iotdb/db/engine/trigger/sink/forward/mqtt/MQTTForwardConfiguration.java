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

package org.apache.iotdb.db.engine.trigger.sink.forward.mqtt;

import org.apache.iotdb.db.engine.trigger.sink.api.Configuration;
import org.apache.iotdb.db.engine.trigger.sink.exception.SinkException;

import org.fusesource.mqtt.client.QoS;

public class MQTTForwardConfiguration implements Configuration {
  private final String host;
  private final int port;
  private final String username;
  private final String password;
  private final String topic;
  private final long reconnectDelay;
  private final long connectAttemptsMax;
  private final QoS qos;
  private final boolean retain;
  private final int poolSize;
  private final boolean stopIfException;

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
      int poolSize,
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
    this.poolSize = poolSize;
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

  public String getTopic() {
    return topic;
  }

  public long getReconnectDelay() {
    return reconnectDelay;
  }

  public long getConnectAttemptsMax() {
    return connectAttemptsMax;
  }

  public QoS getQos() {
    return qos;
  }

  public boolean isRetain() {
    return retain;
  }

  public int getPoolSize() {
    return poolSize;
  }

  public boolean isStopIfException() {
    return stopIfException;
  }
}
