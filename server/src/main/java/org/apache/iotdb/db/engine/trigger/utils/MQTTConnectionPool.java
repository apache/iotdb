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
import org.apache.iotdb.db.engine.trigger.sink.exception.SinkException;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.QoS;

import java.util.HashMap;
import java.util.Map;

public class MQTTConnectionPool extends GenericObjectPool<BlockingConnection> {

  // Each host:port corresponds to a singleton instance
  private static final Map<String, MQTTConnectionPool> MQTT_CONNECTION_POOL_MAP = new HashMap<>();
  private static final Map<String, Integer> MQTT_CONNECTION_REFERENCE_COUNT = new HashMap<>();

  public static MQTTConnectionPool getInstance(
      String host, int port, String username, MQTTConnectionFactory factory, int size) {
    String key = host + ":" + port + "," + username;
    MQTT_CONNECTION_REFERENCE_COUNT.merge(key, 1, Integer::sum);
    MQTTConnectionPool pool = MQTT_CONNECTION_POOL_MAP.get(key);
    if (pool == null || pool.isClosed()) {
      pool = new MQTTConnectionPool(factory, size);
      MQTT_CONNECTION_POOL_MAP.put(key, pool);
    }
    return pool;
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

  public void clearAndClose(String host, int port, String username) throws SinkException {
    String key = host + ":" + port + "," + username;
    clear();
    if (!MQTT_CONNECTION_REFERENCE_COUNT.containsKey(key)) {
      throw new SinkException("The MQTT connection pool doesn't exist");
    }
    if (0 == MQTT_CONNECTION_REFERENCE_COUNT.merge(key, -1, Integer::sum)) {
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
