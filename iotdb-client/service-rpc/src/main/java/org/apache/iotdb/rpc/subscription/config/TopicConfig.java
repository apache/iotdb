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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.rpc.subscription.config.TopicConstant.MODE_LIVE_VALUE;
import static org.apache.iotdb.rpc.subscription.config.TopicConstant.MODE_SNAPSHOT_VALUE;

public class TopicConfig extends PipeParameters {

  public TopicConfig() {
    super(Collections.emptyMap());
  }

  public TopicConfig(final Map<String, String> attributes) {
    super(attributes);
  }

  private static final Map<String, String> REALTIME_BATCH_MODE_CONFIG =
      Collections.singletonMap("realtime.mode", "batch");
  private static final Map<String, String> REALTIME_STREAM_MODE_CONFIG =
      Collections.singletonMap("realtime.mode", "stream");

  private static final Map<String, String> SINK_TABLET_FORMAT_CONFIG =
      Collections.singletonMap("format", "tablet");

  private static final Map<String, String> SNAPSHOT_MODE_CONFIG =
      Collections.singletonMap("mode", MODE_SNAPSHOT_VALUE);
  private static final Map<String, String> LIVE_MODE_CONFIG =
      Collections.singletonMap("mode", MODE_LIVE_VALUE);

  private static final Set<String> LOOSE_RANGE_KEY_SET;

  static {
    final Set<String> set = new HashSet<>(2);
    set.add("history.loose-range");
    set.add("realtime.loose-range");
    LOOSE_RANGE_KEY_SET = Collections.unmodifiableSet(set);
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

  public Map<String, String> getAttributesWithTimeRange() {
    final Map<String, String> attributesWithTimeRange = new HashMap<>();

    // there should be no TopicConstant.NOW_TIME_VALUE here
    attributesWithTimeRange.put(
        TopicConstant.START_TIME_KEY,
        attributes.getOrDefault(TopicConstant.START_TIME_KEY, String.valueOf(Long.MIN_VALUE)));
    attributesWithTimeRange.put(
        TopicConstant.END_TIME_KEY,
        attributes.getOrDefault(TopicConstant.END_TIME_KEY, String.valueOf(Long.MAX_VALUE)));

    return attributesWithTimeRange;
  }

  public Map<String, String> getAttributesWithRealtimeMode() {
    return REALTIME_STREAM_MODE_CONFIG; // default to stream (hybrid)
  }

  public Map<String, String> getAttributesWithSourceMode() {
    return MODE_SNAPSHOT_VALUE.equalsIgnoreCase(
            attributes.getOrDefault(TopicConstant.MODE_KEY, TopicConstant.MODE_DEFAULT_VALUE))
        ? SNAPSHOT_MODE_CONFIG
        : LIVE_MODE_CONFIG;
  }

  public Map<String, String> getAttributesWithSourceLooseRange() {
    final String looseRangeValue =
        attributes.getOrDefault(
            TopicConstant.LOOSE_RANGE_KEY, TopicConstant.LOOSE_RANGE_DEFAULT_VALUE);
    return LOOSE_RANGE_KEY_SET.stream()
        .collect(Collectors.toMap(key -> key, key -> looseRangeValue));
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

  public Map<String, String> getAttributesWithSinkFormat() {
    return Collections.emptyMap(); // default to hybrid
  }
}
