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

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
public class UpdateDetailContainer implements UpdateContainer {

  static final long MAP_SIZE = RamUsageEstimator.shallowSizeOfInstance(ConcurrentHashMap.class);
  static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(UpdateClearContainer.class) + MAP_SIZE;

  // <@Nonnull TableName, <deviceId(@Nullable deviceNodes), <@Nonnull attributeKey, @Nullable
  // attributeValue>>>
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
  public byte[] getUpdateContent(final @Nonnull AtomicInteger limitBytes) {
    final RewritableByteArrayOutputStream outputStream = new RewritableByteArrayOutputStream();
    try {
      serializeWithLimit(outputStream, limitBytes);
    } catch (final IOException ignored) {
      // ByteArrayOutputStream won't throw IOException
    }
    return outputStream.toByteArray();
  }

  private void serializeWithLimit(
      final RewritableByteArrayOutputStream outputStream, final AtomicInteger limitBytes)
      throws IOException {
    ReadWriteIOUtils.write((byte) 1, outputStream);
    final int mapSizeOffset = outputStream.skipInt();
    int mapEntryCount = 0;
    int newSize;
    for (final Map.Entry<String, ConcurrentMap<String[], ConcurrentMap<String, String>>>
        tableEntry : updateMap.entrySet()) {
      final byte[] tableEntryBytes = tableEntry.getKey().getBytes(TSFileConfig.STRING_CHARSET);
      newSize = 2 * Integer.BYTES + tableEntryBytes.length;
      if (limitBytes.get() < newSize) {
        outputStream.rewrite(mapEntryCount, mapSizeOffset);
        return;
      }
      limitBytes.addAndGet(-newSize);
      ++mapEntryCount;
      outputStream.writeWithLength(tableEntryBytes);
      final int deviceSizeOffset = outputStream.skipInt();
      int deviceEntryCount = 0;
      for (final Map.Entry<String[], ConcurrentMap<String, String>> deviceEntry :
          tableEntry.getValue().entrySet()) {
        final byte[][] deviceIdBytes =
            Arrays.stream(deviceEntry.getKey())
                .map(str -> Objects.nonNull(str) ? str.getBytes(TSFileConfig.STRING_CHARSET) : null)
                .toArray(byte[][]::new);

        newSize =
            (Integer.BYTES * (deviceIdBytes.length + 2))
                + Arrays.stream(deviceIdBytes)
                    .map(bytes -> Objects.nonNull(bytes) ? bytes.length : 0)
                    .reduce(0, Integer::sum);
        if (limitBytes.get() < newSize) {
          outputStream.rewrite(mapEntryCount, mapSizeOffset);
          outputStream.rewrite(deviceEntryCount, deviceSizeOffset);
          return;
        }
        limitBytes.addAndGet(-newSize);
        ++deviceEntryCount;
        ReadWriteIOUtils.write(deviceEntry.getKey().length, outputStream);
        for (final byte[] node : deviceIdBytes) {
          outputStream.writeWithLength(node);
        }
        final int attributeOffset = outputStream.skipInt();
        int attributeCount = 0;
        for (final Map.Entry<String, String> attributeKV : deviceEntry.getValue().entrySet()) {
          final byte[] keyBytes = attributeKV.getKey().getBytes(TSFileConfig.STRING_CHARSET);
          final byte[] valueBytes =
              Objects.nonNull(attributeKV.getValue())
                  ? attributeKV.getValue().getBytes(TSFileConfig.STRING_CHARSET)
                  : null;
          newSize =
              2 * Integer.BYTES
                  + keyBytes.length
                  + (Objects.nonNull(valueBytes) ? valueBytes.length : 0);
          if (limitBytes.get() < newSize) {
            outputStream.rewrite(mapEntryCount, mapSizeOffset);
            outputStream.rewrite(deviceEntryCount, deviceSizeOffset);
            outputStream.rewrite(attributeCount, attributeOffset);
            return;
          }
          limitBytes.addAndGet(-newSize);
          outputStream.writeWithLength(keyBytes);
          outputStream.writeWithLength(valueBytes);
          ++attributeCount;
        }
        outputStream.rewrite(deviceEntry.getValue().size(), attributeOffset);
      }
      outputStream.rewrite(tableEntry.getValue().size(), deviceSizeOffset);
    }
    outputStream.rewrite(updateMap.size(), mapSizeOffset);
  }

  @Override
  public Pair<Long, Boolean> updateSelfByCommitContainer(final UpdateContainer commitContainer) {
    final AtomicLong result = new AtomicLong(0);
    if (commitContainer instanceof UpdateDetailContainer) {
      ((UpdateDetailContainer) commitContainer)
          .updateMap.forEach(
              (table, commitMap) -> {
                if (!this.updateMap.containsKey(table)) {
                  return;
                }
                final ConcurrentMap<String[], ConcurrentMap<String, String>> thisDeviceMap =
                    this.updateMap.get(table);
                commitMap.forEach(
                    (device, attributes) -> {
                      if (!thisDeviceMap.containsKey(device)) {
                        return;
                      }
                      final Map<String, String> thisAttributes = thisDeviceMap.get(device);
                      attributes.forEach(
                          (k, v) -> {
                            if (!thisAttributes.containsKey(k)) {
                              return;
                            }
                            final String thisV = thisAttributes.get(k);
                            if (Objects.equals(thisV, v)) {
                              result.addAndGet(
                                  RamUsageEstimator.sizeOf(k)
                                      + RamUsageEstimator.sizeOf(thisV)
                                      + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY);
                              thisAttributes.remove(k);
                            }
                          });
                      if (thisAttributes.isEmpty()) {
                        result.addAndGet(
                            RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY
                                + RamUsageEstimator.sizeOf(device)
                                + MAP_SIZE);
                        thisDeviceMap.remove(device);
                      }
                    });
                if (thisDeviceMap.isEmpty()) {
                  result.addAndGet(
                      RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY
                          + RamUsageEstimator.sizeOf(table)
                          + MAP_SIZE);
                  this.updateMap.remove(table);
                }
              });
    }
    return new Pair<>(result.get(), updateMap.isEmpty());
  }

  @Override
  public void serialize(final OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write((byte) 1, outputStream);
    ReadWriteIOUtils.write(updateMap.size(), outputStream);
    for (final Map.Entry<String, ConcurrentMap<String[], ConcurrentMap<String, String>>>
        tableEntry : updateMap.entrySet()) {
      ReadWriteIOUtils.write(tableEntry.getKey(), outputStream);
      ReadWriteIOUtils.write(tableEntry.getValue().size(), outputStream);
      for (final Map.Entry<String[], ConcurrentMap<String, String>> deviceEntry :
          tableEntry.getValue().entrySet()) {
        ReadWriteIOUtils.write(deviceEntry.getKey().length, outputStream);
        for (final String node : deviceEntry.getKey()) {
          ReadWriteIOUtils.write(node, outputStream);
        }
        ReadWriteIOUtils.write(deviceEntry.getValue(), outputStream);
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

  UpdateClearContainer degrade() {
    return new UpdateClearContainer(updateMap.keySet());
  }
}
