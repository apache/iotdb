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

package org.apache.iotdb.db.schemaengine.schemaregion.attribute.update;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class UpdateDetailContainer implements UpdateContainer {

  static final long MAP_SIZE = RamUsageEstimator.shallowSizeOfInstance(ConcurrentHashMap.class);
  static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(UpdateClearContainer.class) + MAP_SIZE;

  private final Map<String, Map<String[], Map<String, String>>> updateMap =
      new ConcurrentHashMap<>();

  @Override
  public long updateAttribute(
      final String tableName,
      final String[] deviceId,
      final Map<String, String> updatedAttributes) {
    final AtomicLong result = new AtomicLong(0);
    updateMap.compute(
        tableName,
        (name, value) -> {
          if (Objects.isNull(value)) {
            result.addAndGet(
                RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY
                    + RamUsageEstimator.sizeOf(name)
                    + MAP_SIZE);
            value = new ConcurrentHashMap<>();
          }
          value.compute(
              deviceId,
              (device, attributes) -> {
                if (Objects.isNull(attributes)) {
                  result.addAndGet(
                      RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY
                          + RamUsageEstimator.sizeOf(device)
                          + MAP_SIZE);
                  attributes = new ConcurrentHashMap<>();
                }
                for (final Map.Entry<String, String> updateAttribute :
                    updatedAttributes.entrySet()) {
                  attributes.compute(
                      updateAttribute.getKey(),
                      (k, v) -> {
                        if (Objects.isNull(v)) {
                          result.addAndGet(
                              RamUsageEstimator.sizeOf(k)
                                  + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY);
                        }
                        result.addAndGet(
                            RamUsageEstimator.sizeOf(updateAttribute.getValue())
                                - RamUsageEstimator.sizeOf(v));
                        return updateAttribute.getValue();
                      });
                  if (Objects.isNull(updateAttribute.getValue())) {
                    attributes.put(updateAttribute.getKey(), null);
                  }
                }
                return attributes;
              });
          return value;
        });
    return result.get();
  }

  @Override
  public ByteBuffer getUpdateBuffer() {
    return null;
  }

  @Override
  public Pair<Integer, Boolean> updateSelfByCommitBuffer(final ByteBuffer commitBuffer) {
    return null;
  }

  @Override
  public void serialize(final OutputStream outputstream) {}

  @Override
  public void deserialize(final InputStream fileInputStream) {}

  @Override
  public long ramBytesUsed() {
    // TODO
    return INSTANCE_SIZE;
  }
}
