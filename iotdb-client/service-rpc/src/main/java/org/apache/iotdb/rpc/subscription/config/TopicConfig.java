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

package org.apache.iotdb.rpc.subscription.config;

import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TopicConfig extends PipeParameters {

  public TopicConfig() {
    super(Collections.emptyMap());
  }

  public TopicConfig(final Map<String, String> attributes) {
    super(attributes);
  }

  /////////////////////////////// de/ser ///////////////////////////////

  public void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(attributes, stream);
  }

  public static TopicConfig deserialize(final ByteBuffer buffer) {
    return new TopicConfig(ReadWriteIOUtils.readMap(buffer));
  }

  /////////////////////////////// utilities ///////////////////////////////

  public Map<String, String> getAttributesWithPathOrPattern() {
    if (attributes.containsKey(TopicConstant.PATTERN_KEY)) {
      return Collections.singletonMap(
          TopicConstant.PATTERN_KEY, attributes.get(TopicConstant.PATTERN_KEY));
    }

    return Collections.singletonMap(
        TopicConstant.PATH_KEY,
        attributes.getOrDefault(TopicConstant.PATH_KEY, TopicConstant.PATH_DEFAULT_VALUE));
  }

  public Map<String, String> getAttributesWithTimeRange(final long creationTime) {
    final Map<String, String> attributesWithTimeRange = new HashMap<>();

    // parse start time
    final String startTime =
        attributes.getOrDefault(TopicConstant.START_TIME_KEY, String.valueOf(Long.MIN_VALUE));
    if (TopicConstant.NOW_TIME_VALUE.equals(startTime)) {
      attributesWithTimeRange.put(TopicConstant.START_TIME_KEY, String.valueOf(creationTime));
    } else {
      attributesWithTimeRange.put(TopicConstant.START_TIME_KEY, startTime);
    }

    // parse end time
    final String endTime =
        attributes.getOrDefault(TopicConstant.END_TIME_KEY, String.valueOf(Long.MAX_VALUE));
    if (TopicConstant.NOW_TIME_VALUE.equals(endTime)) {
      attributesWithTimeRange.put(TopicConstant.END_TIME_KEY, String.valueOf(creationTime));
    } else {
      attributesWithTimeRange.put(TopicConstant.END_TIME_KEY, endTime);
    }

    // enable loose range when using tsfile format
    if (TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE.equals(
        attributes.getOrDefault(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_DEFAULT_VALUE))) {
      attributesWithTimeRange.put("history.loose-range", "time");
      attributesWithTimeRange.put("realtime.loose-range", "time");
    }

    return attributesWithTimeRange;
  }

  public Map<String, String> getAttributesWithRealtimeMode() {
    return TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE.equals(
            attributes.getOrDefault(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_DEFAULT_VALUE))
        ? Collections.singletonMap("realtime.mode", "batch")
        : Collections.singletonMap("realtime.mode", "hybrid");
  }

  public Map<String, String> getAttributesWithProcessorPrefix() {
    final Map<String, String> attributesWithProcessorPrefix = new HashMap<>();
    attributes.forEach(
        (key, value) -> {
          if (key.toLowerCase().startsWith("processor")) {
            attributesWithProcessorPrefix.put(key, value);
          }
        });
    return attributesWithProcessorPrefix;
  }
}
