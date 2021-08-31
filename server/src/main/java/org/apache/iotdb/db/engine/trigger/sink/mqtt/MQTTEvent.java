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

import org.apache.iotdb.db.engine.trigger.sink.api.Event;

import org.fusesource.mqtt.client.QoS;

public class MQTTEvent implements Event {

  private final String topic;
  private final QoS qos;
  private final boolean retain;

  private final long timestamp;
  private final Object[] values;

  public MQTTEvent(String topic, QoS qos, boolean retain, long timestamp, Object... values) {
    this.topic = topic;
    this.qos = qos;
    this.retain = retain;
    this.timestamp = timestamp;
    this.values = values;
  }

  public String getTopic() {
    return topic;
  }

  public QoS getQoS() {
    return qos;
  }

  public boolean retain() {
    return retain;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public Object[] getValues() {
    return values;
  }
}
