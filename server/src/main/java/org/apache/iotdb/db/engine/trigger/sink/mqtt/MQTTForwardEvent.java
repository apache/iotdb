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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.trigger.sink.api.Event;

import com.google.gson.Gson;

public class MQTTForwardEvent implements Event {

  private final long timestamp;
  private final Object value;
  private final PartialPath fullPath;
  private final String payloadFormatter;

  public MQTTForwardEvent(
      long timestamp, Object value, PartialPath fullPath, String payloadFormatter) {
    this.timestamp = timestamp;
    this.value = value;
    this.fullPath = fullPath;
    this.payloadFormatter = payloadFormatter;
  }

  public String toJsonString() {
    Gson gson = new Gson();
    return gson.toJson(toFormatterString());
  }

  private String toFormatterString() {
    return payloadFormatter
        .replaceAll("`timestamp`", String.valueOf(timestamp))
        .replaceAll("`value`", String.valueOf(value))
        .replaceAll("`device`", fullPath.getDevice())
        .replaceAll("`measurement`", fullPath.getMeasurement())
        .replaceAll("`fullPath`", fullPath.getFullPath());
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
}
