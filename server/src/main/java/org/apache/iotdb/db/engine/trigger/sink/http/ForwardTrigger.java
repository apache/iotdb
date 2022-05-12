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
import org.apache.iotdb.db.engine.trigger.sink.api.Configuration;
import org.apache.iotdb.db.engine.trigger.sink.api.Event;
import org.apache.iotdb.db.engine.trigger.sink.api.Handler;
import org.apache.iotdb.db.exception.TriggerExecutionException;

import org.fusesource.mqtt.client.QoS;

import java.util.HashMap;

public class ForwardTrigger implements Trigger {

  private Handler forwardManagerHandler;
  private Configuration forwardManagerConfiguration;
  private ForwardQueue<Event> queue;
  private final HashMap<String, String> labels = new HashMap<>();
  private String protocol;

  @Override
  public void onCreate(TriggerAttributes attributes) throws Exception {
    protocol = attributes.getString("protocol").toLowerCase();
    boolean stopIfException = Boolean.parseBoolean(attributes.getString("stopIfException"));

    switch (protocol) {
      case "http":
        forwardManagerConfiguration = createHTTPConfiguration(attributes, stopIfException);
        forwardManagerHandler = new HTTPForwardHandler();
        queue = new ForwardQueue<>(forwardManagerHandler);
        forwardManagerHandler.open(forwardManagerConfiguration);
        break;
      case "mqtt":
        forwardManagerConfiguration = createMQTTConfiguration(attributes, stopIfException);
        forwardManagerHandler = new MQTTForwardHandler();
        queue = new ForwardQueue<>(forwardManagerHandler);
        forwardManagerHandler.open(forwardManagerConfiguration);
        break;
      default:
        throw new TriggerExecutionException("Forward protocol doesn't support.");
    }
  }

  private Configuration createHTTPConfiguration(
      TriggerAttributes attributes, boolean stopIfException) {
    String endpoint = attributes.getString("endpoint");
    return new ForwardConfiguration("http", stopIfException, endpoint);
  }

  private Configuration createMQTTConfiguration(
      TriggerAttributes attributes, boolean stopIfException) {
    String host = attributes.getString("host");
    String port = attributes.getString("port");
    String username = attributes.getString("username");
    String password = attributes.getString("password");
    String reconnectDelay = attributes.getString("reconnectDelay");
    String connectAttemptsMax = attributes.getString("connectAttemptsMax");
    return new ForwardConfiguration(
        "mqtt",
        stopIfException,
        host,
        Integer.parseInt(port),
        username,
        password,
        Long.parseLong(reconnectDelay),
        Long.parseLong(connectAttemptsMax));
  }

  @Override
  public void onDrop() throws Exception {
    forwardManagerHandler.close();
  }

  @Override
  public void onStart() throws Exception {
    forwardManagerHandler.open(forwardManagerConfiguration);
  }

  @Override
  public void onStop() throws Exception {
    forwardManagerHandler.close();
  }

  @Override
  public Double fire(long timestamp, Double value, PartialPath path) throws Exception {
    labels.put("value", String.valueOf(value));
    labels.put("severity", "critical");

    ForwardEvent event;
    switch (protocol) {
      case "http":
        event = new ForwardEvent("topic-http", timestamp, value, path.toString(), labels);
        break;
      case "mqtt":
        event =
            new ForwardEvent(
                "topic-mqtt", timestamp, value, path.toString(), QoS.EXACTLY_ONCE, false, labels);
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
          event = new ForwardEvent("topic-http", timestamps[i], values[i], path.toString(), labels);
          break;
        case "mqtt":
          event =
              new ForwardEvent(
                  "topic-mqtt",
                  timestamps[i],
                  values[i],
                  path.toString(),
                  QoS.EXACTLY_ONCE,
                  false,
                  labels);
          break;
        default:
          throw new TriggerExecutionException("Forward protocol doesn't support.");
      }
      queue.offer(event);
    }
    return values;
  }
}
