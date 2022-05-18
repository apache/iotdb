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

import org.apache.iotdb.db.engine.trigger.sink.api.Handler;
import org.apache.iotdb.db.engine.trigger.sink.exception.SinkException;
import org.apache.iotdb.db.engine.trigger.utils.MQTTConnectionFactory;
import org.apache.iotdb.db.engine.trigger.utils.MQTTConnectionPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MQTTForwardHandler implements Handler<MQTTForwardConfiguration, MQTTForwardEvent> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MQTTForwardHandler.class);

  private MQTTConnectionPool connectionPool;
  private MQTTForwardConfiguration configuration;

  @Override
  public void open(MQTTForwardConfiguration configuration) throws Exception {
    this.configuration = configuration;
    MQTTConnectionFactory factory =
        new MQTTConnectionFactory(
            configuration.getHost(),
            configuration.getPort(),
            configuration.getUsername(),
            configuration.getPassword(),
            configuration.getConnectAttemptsMax(),
            configuration.getReconnectDelay());
    connectionPool =
        MQTTConnectionPool.getInstance(
            configuration.getHost(), configuration.getPort(), factory, configuration.getPoolSize());
    connectionPool.preparePool();
  }

  @Override
  public void close() throws Exception {
    connectionPool.clearAndClose();
  }

  @Override
  public void onEvent(MQTTForwardEvent event) throws SinkException {
    try {
      String device = configuration.getDevice();
      String measurement = configuration.getMeasurement();
      if (device != null && measurement != null) {
        connectionPool.publish(
            configuration.getTopic(),
            ("[" + event.toJsonString(device, measurement) + "]").getBytes(),
            configuration.getQos(),
            configuration.isRetain());
      } else {
        connectionPool.publish(
            configuration.getTopic(),
            ("[" + event.toJsonString() + "]").getBytes(),
            configuration.getQos(),
            configuration.isRetain());
      }
    } catch (Exception e) {
      if (configuration.isStopIfException()) {
        throw new SinkException("MQTT Forward Exception", e);
      }
      LOGGER.error("MQTT Forward Exception", e);
    }
  }

  @Override
  public void onEvent(List<MQTTForwardEvent> events) throws SinkException {
    StringBuilder sb = new StringBuilder().append("[");
    String device = configuration.getDevice();
    String measurement = configuration.getMeasurement();
    if (device != null && measurement != null) {
      for (MQTTForwardEvent event : events) {
        sb.append(event.toJsonString(device, measurement)).append(", ");
      }
    } else {
      for (MQTTForwardEvent event : events) {
        sb.append(event.toJsonString()).append(", ");
      }
    }
    sb.replace(sb.lastIndexOf(", "), sb.length(), "").append("]");
    try {
      connectionPool.publish(
          configuration.getTopic(),
          sb.toString().getBytes(),
          configuration.getQos(),
          configuration.isRetain());
    } catch (Exception e) {
      if (configuration.isStopIfException()) {
        throw new SinkException("MQTT Forward Exception", e);
      }
      LOGGER.error("MQTT Forward Exception", e);
    }
  }
}
