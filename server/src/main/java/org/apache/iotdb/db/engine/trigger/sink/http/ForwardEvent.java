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
import org.apache.iotdb.db.engine.trigger.sink.api.Event;

import com.google.gson.Gson;

import java.util.Map;

public class ForwardEvent implements Event {
  private final String topic;
  private final long timestamp;
  private final Object value;
  private final PartialPath fullPath;
  private Map<String, String> labels;

  public ForwardEvent(String topic, long timestamp, Object value, PartialPath fullPath) {
    this.topic = topic;
    this.timestamp = timestamp;
    this.value = value;
    this.fullPath = fullPath;
  }

  public ForwardEvent(
      String topic,
      long timestamp,
      Object value,
      PartialPath fullPath,
      Map<String, String> labels) {
    this(topic, timestamp, value, fullPath);
    this.labels = labels;
  }

  public String toJsonString() {
    Gson gson = new Gson();
    return gson.toJson(this);
  }

  public String getTopic() {
    return topic;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public Object getValue() {
    return value;
  }

  public PartialPath getFullPath() {
    return fullPath;
  }

  public Map<String, String> getLabels() {
    return labels;
  }
}
