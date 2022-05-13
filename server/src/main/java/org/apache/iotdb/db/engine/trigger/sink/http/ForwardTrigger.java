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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.trigger.api.Trigger;
import org.apache.iotdb.db.engine.trigger.api.TriggerAttributes;
import org.apache.iotdb.db.engine.trigger.sink.api.Event;
import org.apache.iotdb.db.engine.trigger.sink.api.Handler;
import org.apache.iotdb.db.engine.trigger.sink.exception.SinkException;
import org.apache.iotdb.db.exception.TriggerExecutionException;

import org.fusesource.mqtt.client.QoS;

import java.util.HashMap;

public class ForwardTrigger implements Trigger {

  private Handler forwardHandler;
  private ForwardConfiguration forwardConfig;
  private ForwardQueue<Event> queue;
  private final HashMap<String, String> labels = new HashMap<>();
  private String protocol;

  @Override
  public void onCreate(TriggerAttributes attributes) throws Exception {
    protocol = attributes.getString("protocol").toLowerCase();
    boolean stopIfException = attributes.getBooleanOrDefault("stopIfException", false);
    int maxQueueCount = attributes.getIntOrDefault("maxQueueCount", 8);
    int maxQueueSize = attributes.getIntOrDefault("maxQueueSize", 2000);
    int forwardBatchSize = attributes.getIntOrDefault("forwardBatchSize", 100);

    forwardConfig =
        new ForwardConfiguration(
            protocol, stopIfException, maxQueueCount, maxQueueSize, forwardBatchSize);

    switch (protocol) {
      case "http":
        createHTTPConfiguration(forwardConfig, attributes);
        forwardHandler = new HTTPForwardHandler();
        queue = new ForwardQueue<>(forwardHandler, forwardConfig);
        forwardHandler.open(forwardConfig);
        break;
      case "mqtt":
        createMQTTConfiguration(forwardConfig, attributes);
        forwardHandler = new MQTTForwardHandler();
        queue = new ForwardQueue<>(forwardHandler, forwardConfig);
        forwardHandler.open(forwardConfig);
        break;
      default:
        throw new TriggerExecutionException("Forward protocol doesn't support.");
    }
  }

  private void createHTTPConfiguration(
      ForwardConfiguration configuration, TriggerAttributes attributes) throws SinkException {
    String endpoint = attributes.getString("endpoint");
    ForwardConfiguration.setHTTPConfig(configuration, endpoint);
    configuration.checkHTTPConfig();
  }

  private void createMQTTConfiguration(
      ForwardConfiguration configuration, TriggerAttributes attributes) throws SinkException {
    String host = attributes.getString("host");
    int port = attributes.getInt("port");
    String username = attributes.getString("username");
    String password = attributes.getString("password");
    long reconnectDelay = attributes.getLongOrDefault("reconnectDelay", 10L);
    long connectAttemptsMax = attributes.getLongOrDefault("connectAttemptsMax", 3L);
    ForwardConfiguration.setMQTTConfig(
        configuration, host, port, username, password, reconnectDelay, connectAttemptsMax);
    configuration.checkMQTTConfig();
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
    labels.put("value", String.valueOf(value));
    labels.put("severity", "critical");

    ForwardEvent event;
    switch (protocol) {
      case "http":
        event = new ForwardEvent("topic-http", timestamp, value, path, labels);
        break;
      case "mqtt":
        event =
            new ForwardEvent("topic-mqtt", timestamp, value, path, QoS.EXACTLY_ONCE, false, labels);
        break;
      default:
        throw new TriggerExecutionException("Forward protocol doesn't support.");
    }

    queue.offer(event);
    return value;
  }

  @Override
  public double[] fire(long[] timestamps, double[] values, PartialPath path) throws Exception {
    // TODO need merge
    for (int i = 0; i < timestamps.length; i++) {
      labels.put("value", String.valueOf(values[i]));
      labels.put("severity", "warning");

      ForwardEvent event;
      switch (protocol) {
        case "http":
          event = new ForwardEvent("topic-http", timestamps[i], values[i], path, labels);
          break;
        case "mqtt":
          event =
              new ForwardEvent(
                  "topic-mqtt", timestamps[i], values[i], path, QoS.EXACTLY_ONCE, false, labels);
          break;
        default:
          throw new TriggerExecutionException("Forward protocol doesn't support.");
      }
      queue.offer(event);
    }
    return values;
  }
}
