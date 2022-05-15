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

package org.apache.iotdb.db.engine.trigger.impl;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.trigger.api.Trigger;
import org.apache.iotdb.db.engine.trigger.api.TriggerAttributes;
import org.apache.iotdb.db.engine.trigger.sink.api.Configuration;
import org.apache.iotdb.db.engine.trigger.sink.api.Event;
import org.apache.iotdb.db.engine.trigger.sink.api.Handler;
import org.apache.iotdb.db.engine.trigger.sink.exception.SinkException;
import org.apache.iotdb.db.engine.trigger.sink.http.HTTPForwardConfiguration;
import org.apache.iotdb.db.engine.trigger.sink.http.HTTPForwardEvent;
import org.apache.iotdb.db.engine.trigger.sink.http.HTTPForwardHandler;
import org.apache.iotdb.db.engine.trigger.sink.mqtt.MQTTForwardConfiguration;
import org.apache.iotdb.db.engine.trigger.sink.mqtt.MQTTForwardEvent;
import org.apache.iotdb.db.engine.trigger.sink.mqtt.MQTTForwardHandler;
import org.apache.iotdb.db.exception.TriggerExecutionException;

import java.util.HashMap;

public class ForwardTrigger implements Trigger {

  private static final String PROTOCOL_HTTP = "http";
  private static final String PROTOCOL_MQTT = "mqtt";

  private Handler forwardHandler;
  private Configuration forwardConfig;
  private BatchHandlerQueue<Event> queue;
  private final HashMap<String, String> labels = new HashMap<>();
  private String protocol;

  @Override
  public void onCreate(TriggerAttributes attributes) throws Exception {
    protocol = attributes.getStringOrDefault("protocol", PROTOCOL_HTTP).toLowerCase();
    int queueNumber = attributes.getIntOrDefault("queueNumber", 8);
    int queueSize = attributes.getIntOrDefault("queueSize", 2000);
    int batchSize = attributes.getIntOrDefault("batchSize", 50);

    switch (protocol) {
      case PROTOCOL_HTTP:
        forwardConfig = createHTTPConfiguration(attributes);
        forwardHandler = new HTTPForwardHandler();
        queue = new BatchHandlerQueue<>(queueNumber, queueSize, batchSize, forwardHandler);
        forwardHandler.open(forwardConfig);
        break;
      case PROTOCOL_MQTT:
        forwardConfig = createMQTTConfiguration(attributes);
        forwardHandler = new MQTTForwardHandler();
        queue = new BatchHandlerQueue<>(queueNumber, queueSize, batchSize, forwardHandler);
        forwardHandler.open(forwardConfig);
        break;
      default:
        throw new TriggerExecutionException("Forward protocol doesn't support.");
    }
  }

  private HTTPForwardConfiguration createHTTPConfiguration(TriggerAttributes attributes)
      throws SinkException {
    String endpoint = attributes.getString("endpoint");
    boolean stopIfException = attributes.getBooleanOrDefault("stopIfException", false);
    HTTPForwardConfiguration forwardConfig =
        new HTTPForwardConfiguration(endpoint, stopIfException);
    forwardConfig.checkConfig();
    return forwardConfig;
  }

  private MQTTForwardConfiguration createMQTTConfiguration(TriggerAttributes attributes)
      throws SinkException {
    boolean stopIfException = attributes.getBooleanOrDefault("stopIfException", false);
    String host = attributes.getString("host");
    int port = attributes.getInt("port");
    String username = attributes.getString("username");
    String password = attributes.getString("password");
    String topic = attributes.getString("topic");
    long reconnectDelay = attributes.getLongOrDefault("reconnectDelay", 10L);
    long connectAttemptsMax = attributes.getLongOrDefault("connectAttemptsMax", 3L);
    String qos = attributes.getStringOrDefault("qos", "exactly_once");
    boolean retain = attributes.getBooleanOrDefault("retain", false);
    MQTTForwardConfiguration forwardConfig =
        new MQTTForwardConfiguration(
            host,
            port,
            username,
            password,
            topic,
            reconnectDelay,
            connectAttemptsMax,
            qos,
            retain,
            stopIfException);
    forwardConfig.checkConfig();
    return forwardConfig;
  }

  @Override
  public void onDrop() throws Exception {
    forwardHandler.close();
  }

  @Override
  public void onStart() throws Exception {
    forwardHandler.open(forwardConfig);
  }

  @Override
  public void onStop() throws Exception {
    forwardHandler.close();
  }

  @Override
  public Double fire(long timestamp, Double value, PartialPath path) throws Exception {
    Event event;
    switch (protocol) {
      case PROTOCOL_HTTP:
        event = new HTTPForwardEvent(timestamp, value, path);
        break;
      case PROTOCOL_MQTT:
        event = new MQTTForwardEvent(timestamp, value, path);
        break;
      default:
        throw new TriggerExecutionException("Forward protocol doesn't support.");
    }

    queue.offer(event);
    return value;
  }

  @Override
  public double[] fire(long[] timestamps, double[] values, PartialPath path) throws Exception {
    for (int i = 0; i < timestamps.length; i++) {
      Event event;
      switch (protocol) {
        case PROTOCOL_HTTP:
          event = new HTTPForwardEvent(timestamps[i], values[i], path);
          break;
        case PROTOCOL_MQTT:
          event = new MQTTForwardEvent(timestamps[i], values[i], path);
          break;
        default:
          throw new TriggerExecutionException("Forward protocol doesn't support.");
      }
      queue.offer(event);
    }
    return values;
  }
}
