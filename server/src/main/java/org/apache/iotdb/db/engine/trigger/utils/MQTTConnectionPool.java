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

public class MQTTConnectionPool extends GenericObjectPool<BlockingConnection> {

  public MQTTConnectionPool(MQTTConnectionFactory factory, int size) {
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
    close();
  }

  public void publish(final String topic, final byte[] payload, final QoS qos, final boolean retain)
      throws Exception {
    BlockingConnection connection = this.borrowObject();
    connection.publish(topic, payload, qos, retain);
    returnObject(connection);
  }
}
