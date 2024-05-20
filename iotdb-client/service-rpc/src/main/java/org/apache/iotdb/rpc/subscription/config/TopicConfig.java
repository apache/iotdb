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

  public TopicConfig(Map<String, String> attributes) {
    super(attributes);
  }

  /////////////////////////////// de/ser ///////////////////////////////

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(attributes, stream);
  }

  public static TopicConfig deserialize(ByteBuffer buffer) {
    return new TopicConfig(ReadWriteIOUtils.readMap(buffer));
  }

  /////////////////////////////// utilities ///////////////////////////////

  public Map<String, String> getAttributesWithSourcePathOrPattern() {
    if (attributes.containsKey(TopicConstant.PATTERN_KEY)) {
      return Collections.singletonMap(
          TopicConstant.PATTERN_KEY, attributes.get(TopicConstant.PATTERN_KEY));
    }

    return Collections.singletonMap(
        TopicConstant.PATH_KEY,
        attributes.getOrDefault(TopicConstant.PATH_KEY, TopicConstant.PATH_DEFAULT_VALUE));
  }

  public Map<String, String> getAttributesWithTimeRange(long creationTime) {
    Map<String, String> attributesWithTimeRange = new HashMap<>();
    String startTime =
        attributes.getOrDefault(TopicConstant.START_TIME_KEY, String.valueOf(Long.MIN_VALUE));
    if (TopicConstant.NOW_TIME_VALUE.equals(startTime)) {
      attributesWithTimeRange.put(TopicConstant.START_TIME_KEY, String.valueOf(creationTime));
    } else {
      attributesWithTimeRange.put(TopicConstant.START_TIME_KEY, startTime);
    }
    String endTime =
        attributes.getOrDefault(TopicConstant.END_TIME_KEY, String.valueOf(Long.MAX_VALUE));
    if (TopicConstant.NOW_TIME_VALUE.equals(endTime)) {
      attributesWithTimeRange.put(TopicConstant.END_TIME_KEY, String.valueOf(creationTime));
    } else {
      attributesWithTimeRange.put(TopicConstant.END_TIME_KEY, endTime);
    }
    return attributesWithTimeRange;
  }

  public Map<String, String> getAttributesWithProcessorPrefix() {
    Map<String, String> attributesWithProcessorPrefix = new HashMap<>();
    attributes.forEach(
        (key, value) -> {
          if (key.toLowerCase().startsWith("processor")) {
            attributesWithProcessorPrefix.put(key, value);
          }
        });
    return attributesWithProcessorPrefix;
  }
}
