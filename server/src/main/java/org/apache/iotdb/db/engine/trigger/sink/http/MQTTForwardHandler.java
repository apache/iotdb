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

import org.apache.iotdb.db.engine.trigger.sink.api.Handler;
import org.apache.iotdb.db.engine.trigger.sink.exception.SinkException;

import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MQTTForwardHandler implements Handler<ForwardConfiguration, ForwardEvent> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MQTTForwardHandler.class);

  private BlockingConnection connection;
  private ForwardConfiguration configuration;

  @Override
  public void open(ForwardConfiguration configuration) throws Exception {
    this.configuration = configuration;
    MQTT mqtt = new MQTT();
    mqtt.setHost(configuration.getHost(), configuration.getPort());
    mqtt.setUserName(configuration.getUsername());
    mqtt.setPassword(configuration.getPassword());
    mqtt.setConnectAttemptsMax(configuration.getConnectAttemptsMax());
    mqtt.setReconnectDelay(configuration.getReconnectDelay());

    connection = mqtt.blockingConnection();
    connection.connect();
  }

  @Override
  public void close() throws Exception {
    connection.disconnect();
  }

  @Override
  public void onEvent(ForwardEvent event) throws SinkException {
    try {
      connection.publish(
          event.getTopic(), event.toJsonString().getBytes(), event.getQos(), event.isRetain());
    } catch (Exception e) {
      if (configuration.isStopIfException()) {
        throw new SinkException("MQTT Forward Exception", e);
      }
      LOGGER.error("MQTT Forward Exception", e);
    }
  }

  @Override
  public void onEvent(List<ForwardEvent> events) throws SinkException {
    // TODO need merge
    for (ForwardEvent event : events) {
      try {
        connection.publish(
            event.getTopic(), event.toJsonString().getBytes(), event.getQos(), event.isRetain());
      } catch (Exception e) {
        if (configuration.isStopIfException()) {
          throw new SinkException("MQTT Forward Exception", e);
        }
        LOGGER.error("MQTT Forward Exception", e);
      }
    }
  }
}
