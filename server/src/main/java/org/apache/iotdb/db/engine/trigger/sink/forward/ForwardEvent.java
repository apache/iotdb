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

package org.apache.iotdb.db.engine.trigger.sink.forward;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.trigger.sink.api.Event;
import org.apache.iotdb.tsfile.utils.Binary;

public class ForwardEvent implements Event {
  private final long timestamp;
  private final Object value;
  private final PartialPath fullPath;

  private static final String PAYLOAD_FORMATTER =
      "{\"device\":\"%s\",\"measurement\":\"%s\",\"timestamp\":%d,\"value\":%s}";

  public static final String PAYLOADS_FORMATTER_REGEX =
      "\\[(\\{\"device\":\".*\",\"measurement\":\".*\",\"timestamp\":\\d*,\"value\":.*},)*"
          + "(\\{\"device\":\".*\",\"measurement\":\".*\",\"timestamp\":\\d*,\"value\":.*})]";

  public ForwardEvent(long timestamp, Object value, PartialPath fullPath) {
    this.timestamp = timestamp;
    this.value = value;
    this.fullPath = fullPath;
  }

  public String toJsonString() {
    return String.format(
        PAYLOAD_FORMATTER,
        fullPath.getDevice(),
        fullPath.getMeasurement(),
        timestamp,
        objectToJson(value));
  }

  private static String objectToJson(Object object) {
    return (object instanceof String || object instanceof Binary)
        ? ('\"' + object.toString() + '\"')
        : object.toString();
  }

  public long getTimestamp() {
    return timestamp;
  }

  public Object getValue() {
    return value;
  }

  @Override
  public PartialPath getFullPath() {
    return fullPath;
  }
}
