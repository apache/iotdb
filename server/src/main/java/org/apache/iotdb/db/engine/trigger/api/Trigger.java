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

package org.apache.iotdb.db.engine.trigger.api;

import org.apache.iotdb.tsfile.utils.Binary;

/** User Guide: docs/UserGuide/Operation Manual/Triggers.md */
public interface Trigger {

  @SuppressWarnings("squid:S112")
  default void onCreate(TriggerAttributes attributes) throws Exception {}

  @SuppressWarnings("squid:S112")
  default void onDrop() throws Exception {}

  @SuppressWarnings("squid:S112")
  default void onStart() throws Exception {}

  @SuppressWarnings("squid:S112")
  default void onStop() throws Exception {}

  @SuppressWarnings("squid:S112")
  default Integer fire(long timestamp, Integer value) throws Exception {
    return value;
  }

  default int[] fire(long[] timestamps, int[] values) throws Exception {
    int size = timestamps.length;
    for (int i = 0; i < size; ++i) {
      fire(timestamps[i], values[i]);
    }
    return values;
  }

  @SuppressWarnings("squid:S112")
  default Long fire(long timestamp, Long value) throws Exception {
    return value;
  }

  default long[] fire(long[] timestamps, long[] values) throws Exception {
    int size = timestamps.length;
    for (int i = 0; i < size; ++i) {
      fire(timestamps[i], values[i]);
    }
    return values;
  }

  @SuppressWarnings("squid:S112")
  default Float fire(long timestamp, Float value) throws Exception {
    return value;
  }

  default float[] fire(long[] timestamps, float[] values) throws Exception {
    int size = timestamps.length;
    for (int i = 0; i < size; ++i) {
      fire(timestamps[i], values[i]);
    }
    return values;
  }

  @SuppressWarnings("squid:S112")
  default Double fire(long timestamp, Double value) throws Exception {
    return value;
  }

  default double[] fire(long[] timestamps, double[] values) throws Exception {
    int size = timestamps.length;
    for (int i = 0; i < size; ++i) {
      fire(timestamps[i], values[i]);
    }
    return values;
  }

  @SuppressWarnings("squid:S112")
  default Boolean fire(long timestamp, Boolean value) throws Exception {
    return value;
  }

  default boolean[] fire(long[] timestamps, boolean[] values) throws Exception {
    int size = timestamps.length;
    for (int i = 0; i < size; ++i) {
      fire(timestamps[i], values[i]);
    }
    return values;
  }

  @SuppressWarnings("squid:S112")
  default Binary fire(long timestamp, Binary value) throws Exception {
    return value;
  }

  default Binary[] fire(long[] timestamps, Binary[] values) throws Exception {
    int size = timestamps.length;
    for (int i = 0; i < size; ++i) {
      fire(timestamps[i], values[i]);
    }
    return values;
  }
}
