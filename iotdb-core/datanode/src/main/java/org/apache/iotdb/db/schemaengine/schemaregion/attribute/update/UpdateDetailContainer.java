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
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class UpdateDetailContainer implements UpdateContainer {

  static final long MAP_SIZE = RamUsageEstimator.shallowSizeOfInstance(ConcurrentHashMap.class);
  static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(UpdateClearContainer.class) + MAP_SIZE;

  private final ConcurrentMap<String, ConcurrentMap<String[], ConcurrentMap<String, String>>>
      updateMap = new ConcurrentHashMap<>();

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
  public byte[] getUpdateContent() {
    return null;
  }

  @Override
  public Pair<Integer, Boolean> updateSelfByCommitBuffer(final ByteBuffer commitBuffer) {
    return null;
  }

  @Override
  public void serialize(final OutputStream outputstream) throws IOException {
    ReadWriteIOUtils.write(updateMap.size(), outputstream);
    for (final Map.Entry<String, ConcurrentMap<String[], ConcurrentMap<String, String>>>
        tableEntry : updateMap.entrySet()) {
      ReadWriteIOUtils.write(tableEntry.getKey(), outputstream);
      for (final Map.Entry<String[], ConcurrentMap<String, String>> deviceEntry :
          tableEntry.getValue().entrySet()) {
        ReadWriteIOUtils.write(deviceEntry.getKey().length, outputstream);
        for (final String node : deviceEntry.getKey()) {
          ReadWriteIOUtils.write(node, outputstream);
        }
        ReadWriteIOUtils.write(deviceEntry.getValue(), outputstream);
      }
    }
  }

  @Override
  public void deserialize(final InputStream inputStream) throws IOException {
    final int tableSize = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < tableSize; ++i) {
      final String tableName = ReadWriteIOUtils.readString(inputStream);
      final int deviceSize = ReadWriteIOUtils.readInt(inputStream);
      final ConcurrentMap<String[], ConcurrentMap<String, String>> deviceMap =
          new ConcurrentHashMap<>(deviceSize);
      for (int j = 0; j < deviceSize; ++j) {
        final int nodeSize = ReadWriteIOUtils.readInt(inputStream);
        final String[] deviceId = new String[nodeSize];
        for (int k = 0; k < nodeSize; ++k) {
          deviceId[k] = ReadWriteIOUtils.readString(inputStream);
        }
        deviceMap.put(deviceId, readConcurrentMap(inputStream));
      }
      updateMap.put(tableName, deviceMap);
    }
  }

  private static ConcurrentMap<String, String> readConcurrentMap(final InputStream inputStream)
      throws IOException {
    final int length = ReadWriteIOUtils.readInt(inputStream);
    if (length == -1) {
      return null;
    } else {
      final ConcurrentMap<String, String> map = new ConcurrentHashMap<>(length);
      for (int i = 0; i < length; ++i) {
        map.put(ReadWriteIOUtils.readString(inputStream), ReadWriteIOUtils.readString(inputStream));
      }
      return map;
    }
  }

  @Override
  public long ramBytesUsed() {
    final AtomicLong result = new AtomicLong(INSTANCE_SIZE);

    // Do not use "sizeOfMap" to save time and ensure that the calculated bytes is the same as
    // previous calculation's sum
    updateMap.forEach(
        (name, value) -> {
          result.addAndGet(
              RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY
                  + RamUsageEstimator.sizeOf(name)
                  + MAP_SIZE);
          value.forEach(
              (device, attributes) -> {
                result.addAndGet(
                    RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY
                        + RamUsageEstimator.sizeOf(device)
                        + MAP_SIZE);
                attributes.forEach(
                    (k, v) ->
                        result.addAndGet(
                            RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY
                                + RamUsageEstimator.sizeOf(k)
                                + RamUsageEstimator.sizeOf(v)));
              });
        });
    return result.get();
  }

  UpdateClearContainer degrade() {
    return new UpdateClearContainer(updateMap.keySet());
  }
}
