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

package org.apache.iotdb.db.engine.trigger.utils;

import org.apache.iotdb.db.engine.trigger.sink.exception.SinkException;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;

import java.util.concurrent.atomic.AtomicInteger;

public class MQTTConnectionFactory extends BasePooledObjectFactory<BlockingConnection> {
  private final String host;
  private final int port;
  private final String username;
  private final String password;
  private final long connectAttemptsMax;
  private final long reconnectDelay;

  private static final AtomicInteger atomicCount = new AtomicInteger();
  private static final String CLIENT_NAME = "MQTTClient";

  public MQTTConnectionFactory(
      String host,
      int port,
      String username,
      String password,
      long connectAttemptsMax,
      long reconnectDelay) {
    this.host = host;
    this.port = port;
    this.username = username;
    this.password = password;
    this.connectAttemptsMax = connectAttemptsMax;
    this.reconnectDelay = reconnectDelay;
  }

  @Override
  public BlockingConnection create() throws Exception {
    MQTT mqtt = new MQTT();
    mqtt.setClientId(CLIENT_NAME + atomicCount.incrementAndGet());
    mqtt.setHost(host, port);
    mqtt.setUserName(username);
    mqtt.setPassword(password);
    mqtt.setConnectAttemptsMax(connectAttemptsMax);
    mqtt.setReconnectDelay(reconnectDelay);

    BlockingConnection connection = mqtt.blockingConnection();
    try {
      connection.connect();
    } catch (Exception e) {
      if (connection != null) {
        if (connection.isConnected()) {
          connection.disconnect();
        }
        connection.kill();
      }
      throw new SinkException("MQTT Connection activate error", e);
    }
    return connection;
  }

  @Override
  public PooledObject<BlockingConnection> wrap(BlockingConnection blockingConnection) {
    return new DefaultPooledObject<>(blockingConnection);
  }

  @Override
  public boolean validateObject(PooledObject<BlockingConnection> p) {
    if (p == null) {
      return false;
    }
    BlockingConnection connection = p.getObject();
    return connection != null && connection.isConnected();
  }

  @Override
  public void destroyObject(PooledObject<BlockingConnection> p) throws Exception {
    if (p == null) {
      return;
    }
    BlockingConnection connection = p.getObject();
    try {
      if (connection != null) {
        if (connection.isConnected()) {
          connection.disconnect();
        }
        connection.kill();
      }
    } catch (Exception e) {
      throw new SinkException("MQTT connection destroy error", e);
    }
    super.destroyObject(p);
  }
}
