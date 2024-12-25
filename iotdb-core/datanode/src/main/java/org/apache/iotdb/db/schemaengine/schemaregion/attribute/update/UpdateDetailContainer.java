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

import org.apache.iotdb.db.schemaengine.schemaregion.attribute.DeviceAttributeStore;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

// Note that all the writings to the containers are guarded by the schema region lock.
// The readings are guarded byt the concurrent segment lock, thus there is no need
// to worry about the concurrent safety.
@ThreadSafe
public class UpdateDetailContainer implements UpdateContainer {

  static final long MAP_SIZE = RamUsageEstimator.shallowSizeOfInstance(ConcurrentHashMap.class);
  static final long LIST_SIZE = RamUsageEstimator.shallowSizeOfInstance(ArrayList.class);
  static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(UpdateClearContainer.class) + MAP_SIZE;

  // <@Nonnull TableName, <deviceId(@Nullable deviceNodes), <@Nonnull attributeKey, @Nonnull
  // attributeValue>>>
  // If the attribute value is updated to null, the stored value is Binary.EMPTY_VALUE
  private final ConcurrentMap<String, ConcurrentMap<List<String>, ConcurrentMap<String, Binary>>>
      updateMap = new ConcurrentHashMap<>();

  public ConcurrentMap<String, ConcurrentMap<List<String>, ConcurrentMap<String, Binary>>>
      getUpdateMap() {
    return updateMap;
  }

  @Override
  public long updateAttribute(
      final String tableName,
      final String[] deviceId,
      final Map<String, Binary> updatedAttributes) {
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
              Arrays.asList(deviceId),
              (device, attributes) -> {
                if (Objects.isNull(attributes)) {
                  result.addAndGet(
                      RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY
                          + sizeOfList(device)
                          + MAP_SIZE);
                  attributes = new ConcurrentHashMap<>();
                }
                for (final Map.Entry<String, Binary> updateAttribute :
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
                            sizeOf(updateAttribute.getValue())
                                - (Objects.nonNull(v) ? sizeOf(v) : 0));
                        return updateAttribute.getValue();
                      });
                }
                return attributes;
              });
          return value;
        });
    return result.get();
  }

  @Override
  public byte[] getUpdateContent(
      final @Nonnull AtomicInteger limitBytes, final @Nonnull AtomicBoolean hasRemaining) {
    final RewritableByteArrayOutputStream outputStream = new RewritableByteArrayOutputStream();
    try {
      serializeWithLimit(outputStream, limitBytes, hasRemaining);
    } catch (final IOException ignored) {
      // ByteArrayOutputStream won't throw IOException
    }
    return outputStream.toByteArray();
  }

  @Override
  public long invalidate(final String tableName) {
    final AtomicLong result = new AtomicLong(0);
    handleTableRemoval(tableName, result);
    return result.get();
  }

  // pathNodes.length >= 3
  @Override
  public long invalidate(final @Nonnull String[] pathNodes) {
    final AtomicLong result = new AtomicLong(0);
    updateMap.compute(
        pathNodes[2],
        (name, value) -> {
          if (Objects.isNull(value)) {
            return null;
          }
          value.compute(
              Arrays.asList(pathNodes).subList(3, pathNodes.length),
              (device, attributes) -> {
                if (Objects.nonNull(attributes)) {
                  result.addAndGet(
                      RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY
                          + sizeOfList(device)
                          + sizeOfMapEntries(attributes));
                }
                return null;
              });
          if (value.isEmpty()) {
            result.addAndGet(
                RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY
                    + RamUsageEstimator.sizeOf(name)
                    + MAP_SIZE);
            return null;
          }
          return value;
        });
    return result.get();
  }

  @Override
  public long invalidate(final String tableName, final String attributeName) {
    final AtomicLong result = new AtomicLong(0);
    final long keyEntrySize =
        RamUsageEstimator.sizeOf(attributeName) + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;
    updateMap.compute(
        tableName,
        (name, value) -> {
          if (Objects.isNull(value)) {
            return null;
          }
          for (final Iterator<Map.Entry<List<String>, ConcurrentMap<String, Binary>>> it =
                  value.entrySet().iterator();
              it.hasNext(); ) {
            final Map.Entry<List<String>, ConcurrentMap<String, Binary>> entry = it.next();
            final Binary attributeValue = entry.getValue().remove(attributeName);
            if (Objects.nonNull(attributeValue)) {
              result.addAndGet(keyEntrySize + sizeOf(attributeValue));
            }
            if (entry.getValue().isEmpty()) {
              result.addAndGet(
                  RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY
                      + sizeOfList(entry.getKey())
                      + MAP_SIZE);
              it.remove();
            }
          }
          if (value.isEmpty()) {
            result.addAndGet(
                RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY
                    + RamUsageEstimator.sizeOf(name)
                    + MAP_SIZE);
            return null;
          }
          return value;
        });
    return result.get();
  }

  private void serializeWithLimit(
      final RewritableByteArrayOutputStream outputStream,
      final AtomicInteger limitBytes,
      final AtomicBoolean hasRemaining)
      throws IOException {
    ReadWriteIOUtils.write((byte) 1, outputStream);
    final int mapSizeOffset = outputStream.skipInt();
    int mapEntryCount = 0;
    int newSize;
    for (final Map.Entry<String, ConcurrentMap<List<String>, ConcurrentMap<String, Binary>>>
        tableEntry : updateMap.entrySet()) {
      final byte[] tableEntryBytes = tableEntry.getKey().getBytes(TSFileConfig.STRING_CHARSET);
      newSize = 2 * Integer.BYTES + tableEntryBytes.length;
      if (limitBytes.get() < newSize) {
        outputStream.rewrite(mapEntryCount, mapSizeOffset);
        hasRemaining.set(true);
        return;
      }
      limitBytes.addAndGet(-newSize);
      ++mapEntryCount;
      outputStream.writeWithLength(tableEntryBytes);
      final int deviceSizeOffset = outputStream.skipInt();
      int deviceEntryCount = 0;
      for (final Map.Entry<List<String>, ConcurrentMap<String, Binary>> deviceEntry :
          tableEntry.getValue().entrySet()) {
        final byte[][] deviceIdBytes =
            deviceEntry.getKey().stream()
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
          hasRemaining.set(true);
          return;
        }
        limitBytes.addAndGet(-newSize);
        ++deviceEntryCount;
        ReadWriteIOUtils.write(deviceEntry.getKey().size(), outputStream);
        for (final byte[] node : deviceIdBytes) {
          outputStream.writeWithLength(node);
        }
        final int attributeOffset = outputStream.skipInt();
        int attributeCount = 0;
        for (final Map.Entry<String, Binary> attributeKV : deviceEntry.getValue().entrySet()) {
          final byte[] keyBytes = attributeKV.getKey().getBytes(TSFileConfig.STRING_CHARSET);
          final byte[] valueBytes =
              attributeKV.getValue() != Binary.EMPTY_VALUE
                  ? attributeKV.getValue().getValues()
                  : null;
          newSize =
              2 * Integer.BYTES
                  + keyBytes.length
                  + (Objects.nonNull(valueBytes) ? valueBytes.length : 0);
          if (limitBytes.get() < newSize) {
            outputStream.rewrite(mapEntryCount, mapSizeOffset);
            outputStream.rewrite(deviceEntryCount, deviceSizeOffset);
            outputStream.rewrite(attributeCount, attributeOffset);
            hasRemaining.set(true);
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
                final ConcurrentMap<List<String>, ConcurrentMap<String, Binary>> thisDeviceMap =
                    this.updateMap.get(table);
                commitMap.forEach(
                    (device, attributes) -> {
                      if (!thisDeviceMap.containsKey(device)) {
                        return;
                      }
                      final Map<String, Binary> thisAttributes = thisDeviceMap.get(device);
                      attributes.forEach(
                          (k, v) -> {
                            if (!thisAttributes.containsKey(k)) {
                              return;
                            }
                            final Binary thisV = thisAttributes.get(k);
                            if (Objects.equals(thisV, v)) {
                              result.addAndGet(
                                  RamUsageEstimator.sizeOf(k)
                                      + sizeOf(thisV)
                                      + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY);
                              thisAttributes.remove(k);
                            }
                          });
                      if (thisAttributes.isEmpty()) {
                        result.addAndGet(
                            RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY
                                + sizeOfList(device)
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
    } else {
      ((UpdateClearContainer) commitContainer)
          .getTableNames()
          .forEach(tableName -> handleTableRemoval(tableName, result));
    }
    return new Pair<>(result.get(), updateMap.isEmpty());
  }

  private void handleTableRemoval(final String tableName, final AtomicLong result) {
    final ConcurrentMap<List<String>, ConcurrentMap<String, Binary>> deviceMap =
        updateMap.remove(tableName);
    if (Objects.nonNull(deviceMap)) {
      result.addAndGet(
          deviceMap.size() * (RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY + MAP_SIZE)
              + deviceMap.entrySet().stream()
                  .mapToLong(
                      entry -> sizeOfList(entry.getKey()) + sizeOfMapEntries(entry.getValue()))
                  .reduce(0, Long::sum));
    }
  }

  private static long sizeOfList(final @Nonnull List<String> input) {
    return input.stream().map(RamUsageEstimator::sizeOf).reduce(LIST_SIZE, Long::sum);
  }

  public static long sizeOfMapEntries(final @Nonnull Map<String, Binary> map) {
    return map.size() * RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY
        + map.entrySet().stream()
            .mapToLong(
                innerEntry ->
                    RamUsageEstimator.sizeOf(innerEntry.getKey()) + sizeOf(innerEntry.getValue()))
            .reduce(0, Long::sum);
  }

  public static long sizeOf(final Binary value) {
    return value == Binary.EMPTY_VALUE ? 0 : value.ramBytesUsed();
  }

  @Override
  public void serialize(final OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write((byte) 1, outputStream);
    ReadWriteIOUtils.write(updateMap.size(), outputStream);
    for (final Map.Entry<String, ConcurrentMap<List<String>, ConcurrentMap<String, Binary>>>
        tableEntry : updateMap.entrySet()) {
      ReadWriteIOUtils.write(tableEntry.getKey(), outputStream);
      ReadWriteIOUtils.write(tableEntry.getValue().size(), outputStream);
      for (final Map.Entry<List<String>, ConcurrentMap<String, Binary>> deviceEntry :
          tableEntry.getValue().entrySet()) {
        ReadWriteIOUtils.write(deviceEntry.getKey().size(), outputStream);
        for (final String node : deviceEntry.getKey()) {
          ReadWriteIOUtils.write(node, outputStream);
        }
        DeviceAttributeStore.write(deviceEntry.getValue(), outputStream);
      }
    }
  }

  @Override
  public void deserialize(final InputStream inputStream) throws IOException {
    final int tableSize = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < tableSize; ++i) {
      final String tableName = ReadWriteIOUtils.readString(inputStream);
      final int deviceSize = ReadWriteIOUtils.readInt(inputStream);
      final ConcurrentMap<List<String>, ConcurrentMap<String, Binary>> deviceMap =
          new ConcurrentHashMap<>(deviceSize);
      for (int j = 0; j < deviceSize; ++j) {
        final int nodeSize = ReadWriteIOUtils.readInt(inputStream);
        final List<String> deviceId = new ArrayList<>(nodeSize);
        for (int k = 0; k < nodeSize; ++k) {
          deviceId.add(ReadWriteIOUtils.readString(inputStream));
        }
        deviceMap.put(
            deviceId,
            (ConcurrentMap<String, Binary>) DeviceAttributeStore.readMap(inputStream, true));
      }
      updateMap.put(tableName, deviceMap);
    }
  }

  UpdateClearContainer degrade() {
    return new UpdateClearContainer(updateMap.keySet());
  }
}
