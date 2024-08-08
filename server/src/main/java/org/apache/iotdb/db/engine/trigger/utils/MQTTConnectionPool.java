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

import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.QoS;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MQTTConnectionPool extends GenericObjectPool<BlockingConnection> {

  // Each host:port,username corresponds to a singleton instance
  private static final Map<String, MQTTConnectionPool> MQTT_CONNECTION_POOL_MAP =
      new ConcurrentHashMap<>();
  private final AtomicInteger referenceCount = new AtomicInteger(0);

  public static MQTTConnectionPool getInstance(
      String host, int port, String username, MQTTConnectionFactory factory, int size)
      throws Exception {
    String key = host + ":" + port + "," + username;
    MQTTConnectionPool connectionPool =
        MQTT_CONNECTION_POOL_MAP.computeIfAbsent(key, k -> new MQTTConnectionPool(factory, size));
    if (connectionPool.referenceCount.getAndIncrement() == 0) {
      connectionPool.preparePool();
    }
    return connectionPool;
  }

  private MQTTConnectionPool(MQTTConnectionFactory factory, int size) {
    super(factory);
    setMaxTotal(
        Math.min(size, IoTDBDescriptor.getInstance().getConfig().getTriggerForwardMQTTPoolSize()));
    setMinIdle(1);
  }

  public void connect() throws Exception {
    BlockingConnection connection = borrowObject();
    if (!connection.isConnected()) {
      connection.connect();
    }
    returnObject(connection);
  }

  public void clearAndClose() {
    clear();
    if (referenceCount.decrementAndGet() == 0) {
      close();
    }
  }

  public void publish(final String topic, final byte[] payload, final QoS qos, final boolean retain)
      throws Exception {
    BlockingConnection connection = this.borrowObject();
    connection.publish(topic, payload, qos, retain);
    returnObject(connection);
  }
}
