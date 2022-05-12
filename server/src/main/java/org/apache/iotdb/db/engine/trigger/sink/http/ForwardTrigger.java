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

import java.io.IOException;
import java.util.HashMap;

public class ForwardTrigger implements Trigger {

  private final HTTPHandler forwardManagerHandler = new HTTPHandler();
  private HTTPConfiguration forwardManagerConfiguration;
  private ForwardQueue<Event> queue;
  private final HashMap<String, String> labels = new HashMap<>();

  @Override
  public void onCreate(TriggerAttributes attributes) throws Exception {
    String endpoint = attributes.getString("endpoint");
    String protocol = attributes.getString("protocol");
    boolean stopForwardingIfException = Boolean.parseBoolean(attributes.getString("stopForwardingIfException"));

    // TODO instantiate the corresponding handler (HTTP or MQTT) according to the protocol.
    forwardManagerConfiguration = new HTTPConfiguration(endpoint, protocol, stopForwardingIfException);
    queue = new ForwardQueue<>(forwardManagerHandler);
    forwardManagerHandler.open(forwardManagerConfiguration);
  }

  @Override
  public void onDrop() throws IOException {
    forwardManagerHandler.close();
  }

  @Override
  public void onStart() {
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
    labels.put("fullPath", path.toString());
    HTTPEvent httpEvent = new HTTPEvent(path.toString(), labels);
    queue.offer(httpEvent);
    return value;
  }

  @Override
  public double[] fire(long[] timestamps, double[] values, PartialPath path) throws Exception {
    // 需要合并
    for (double value : values) {
      labels.put("value", String.valueOf(value));
      labels.put("severity", "warning");
      labels.put("fullPath", path.toString());
      HTTPEvent httpEvent = new HTTPEvent(path.toString(), labels);
      queue.offer(httpEvent);
    }
    return values;
  }
}
