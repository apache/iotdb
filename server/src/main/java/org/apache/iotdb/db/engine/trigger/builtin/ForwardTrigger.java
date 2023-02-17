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

package org.apache.iotdb.db.engine.trigger.builtin;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.trigger.exception.TriggerExecutionException;
import org.apache.iotdb.db.engine.trigger.api.Trigger;
import org.apache.iotdb.db.engine.trigger.sink.api.Configuration;
import org.apache.iotdb.db.engine.trigger.sink.api.Event;
import org.apache.iotdb.db.engine.trigger.sink.api.Handler;
import org.apache.iotdb.db.engine.trigger.sink.exception.SinkException;
import org.apache.iotdb.db.engine.trigger.sink.forward.http.HTTPForwardConfiguration;
import org.apache.iotdb.db.engine.trigger.sink.forward.http.HTTPForwardEvent;
import org.apache.iotdb.db.engine.trigger.sink.forward.http.HTTPForwardHandler;
import org.apache.iotdb.db.engine.trigger.sink.forward.mqtt.MQTTForwardConfiguration;
import org.apache.iotdb.db.engine.trigger.sink.forward.mqtt.MQTTForwardEvent;
import org.apache.iotdb.db.engine.trigger.sink.forward.mqtt.MQTTForwardHandler;
import org.apache.iotdb.db.engine.trigger.utils.BatchHandlerQueue;
import org.apache.iotdb.trigger.api.TriggerAttributes;
import org.apache.iotdb.tsfile.utils.Binary;

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
        break;
      case PROTOCOL_MQTT:
        forwardConfig = createMQTTConfiguration(attributes);
        forwardHandler = new MQTTForwardHandler();
        break;
      default:
        throw new TriggerExecutionException("Forward protocol doesn't support.");
    }
    queue = new BatchHandlerQueue<>(queueNumber, queueSize, batchSize, forwardHandler);
    forwardHandler.open(forwardConfig);
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
    String host = attributes.getString("host");
    int port = attributes.getInt("port");
    String username = attributes.getString("username");
    String password = attributes.getString("password");
    String topic = attributes.getString("topic");
    long reconnectDelay = attributes.getLongOrDefault("reconnectDelay", 10L);
    long connectAttemptsMax = attributes.getLongOrDefault("connectAttemptsMax", 3L);
    String qos = attributes.getStringOrDefault("qos", "exactly_once");
    int poolSize = attributes.getIntOrDefault("poolSize", 4);
    boolean retain = attributes.getBooleanOrDefault("retain", false);
    boolean stopIfException = attributes.getBooleanOrDefault("stopIfException", false);

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
            poolSize,
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

  private void offerEventToQueue(long timestamp, Object value, PartialPath path) throws Exception {
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
  }

  @Override
  public Integer fire(long timestamp, Integer value, PartialPath path) throws Exception {
    offerEventToQueue(timestamp, value, path);
    return value;
  }

  @Override
  public int[] fire(long[] timestamps, int[] values, PartialPath path) throws Exception {
    for (int i = 0; i < timestamps.length; i++) {
      offerEventToQueue(timestamps[i], values[i], path);
    }
    return values;
  }

  @Override
  public Long fire(long timestamp, Long value, PartialPath path) throws Exception {
    offerEventToQueue(timestamp, value, path);
    return value;
  }

  @Override
  public long[] fire(long[] timestamps, long[] values, PartialPath path) throws Exception {
    for (int i = 0; i < timestamps.length; i++) {
      offerEventToQueue(timestamps[i], values[i], path);
    }
    return values;
  }

  @Override
  public Float fire(long timestamp, Float value, PartialPath path) throws Exception {
    offerEventToQueue(timestamp, value, path);
    return value;
  }

  @Override
  public float[] fire(long[] timestamps, float[] values, PartialPath path) throws Exception {
    for (int i = 0; i < timestamps.length; i++) {
      offerEventToQueue(timestamps[i], values[i], path);
    }
    return values;
  }

  @Override
  public Double fire(long timestamp, Double value, PartialPath path) throws Exception {
    offerEventToQueue(timestamp, value, path);
    return value;
  }

  @Override
  public double[] fire(long[] timestamps, double[] values, PartialPath path) throws Exception {
    for (int i = 0; i < timestamps.length; i++) {
      offerEventToQueue(timestamps[i], values[i], path);
    }
    return values;
  }

  @Override
  public Boolean fire(long timestamp, Boolean value, PartialPath path) throws Exception {
    offerEventToQueue(timestamp, value, path);
    return value;
  }

  @Override
  public boolean[] fire(long[] timestamps, boolean[] values, PartialPath path) throws Exception {
    for (int i = 0; i < timestamps.length; i++) {
      offerEventToQueue(timestamps[i], values[i], path);
    }
    return values;
  }

  @Override
  public Binary fire(long timestamp, Binary value, PartialPath path) throws Exception {
    offerEventToQueue(timestamp, value, path);
    return value;
  }

  @Override
  public Binary[] fire(long[] timestamps, Binary[] values, PartialPath path) throws Exception {
    for (int i = 0; i < timestamps.length; i++) {
      offerEventToQueue(timestamps[i], values[i], path);
    }
    return values;
  }
}
