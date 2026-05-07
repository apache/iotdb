/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.utils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class TableDeviceLastValueCollector {

  private final long memoryBudgetInBytes;
  private Map<IDeviceID, Map<String, TimeValuePair>> deviceLastValues;
  private long memoryCostInBytes;

  private TableDeviceLastValueCollector(
      final long memoryBudgetInBytes,
      final Map<IDeviceID, Map<String, TimeValuePair>> deviceLastValues,
      final long memoryCostInBytes) {
    this.memoryBudgetInBytes = memoryBudgetInBytes;
    this.deviceLastValues = deviceLastValues;
    this.memoryCostInBytes = memoryCostInBytes;
  }

  public static TableDeviceLastValueCollector create(final long memoryBudgetInBytes) {
    return new TableDeviceLastValueCollector(memoryBudgetInBytes, new HashMap<>(), 0);
  }

  public static TableDeviceLastValueCollector restore(
      final long memoryBudgetInBytes,
      final Map<IDeviceID, List<Pair<String, TimeValuePair>>> deviceLastValues) {
    if (Objects.isNull(deviceLastValues)) {
      return new TableDeviceLastValueCollector(memoryBudgetInBytes, null, 0);
    }

    final Map<IDeviceID, Map<String, TimeValuePair>> restoredDeviceLastValues =
        restoreDeviceLastValues(deviceLastValues);
    return new TableDeviceLastValueCollector(
        memoryBudgetInBytes,
        restoredDeviceLastValues,
        calculateDeviceLastValuesMemoryCost(restoredDeviceLastValues));
  }

  public void update(
      final IDeviceID deviceId, final Iterable<? extends IChunkMetadata> chunkMetadataList) {
    if (Objects.isNull(chunkMetadataList) || Objects.isNull(deviceLastValues)) {
      return;
    }

    for (final IChunkMetadata chunkMetadata : chunkMetadataList) {
      update(deviceId, chunkMetadata);
      if (Objects.isNull(deviceLastValues)) {
        return;
      }
    }
  }

  public void update(final IDeviceID deviceId, final IChunkMetadata chunkMetadata) {
    if (Objects.isNull(chunkMetadata) || Objects.isNull(deviceLastValues)) {
      return;
    }

    updateMeasurementLastValue(
        deviceId,
        chunkMetadata.getMeasurementUid(),
        chunkMetadata.getEndTime(),
        buildLastValuePair(chunkMetadata));
  }

  public void update(
      final IDeviceID deviceId, final List<TimeseriesMetadata> timeseriesMetadataList) {
    if (Objects.isNull(timeseriesMetadataList) || Objects.isNull(deviceLastValues)) {
      return;
    }

    for (final TimeseriesMetadata timeseriesMetadata : timeseriesMetadataList) {
      if (Objects.isNull(timeseriesMetadata)
          || Objects.isNull(timeseriesMetadata.getStatistics())) {
        continue;
      }

      updateMeasurementLastValue(
          deviceId,
          timeseriesMetadata.getMeasurementId(),
          timeseriesMetadata.getStatistics().getEndTime(),
          buildLastValuePair(timeseriesMetadata));
      if (Objects.isNull(deviceLastValues)) {
        return;
      }
    }
  }

  public Map<IDeviceID, List<Pair<String, TimeValuePair>>> toTsFileResourceLastValues() {
    if (Objects.isNull(deviceLastValues)) {
      return null;
    }

    final Map<IDeviceID, List<Pair<String, TimeValuePair>>> finalDeviceLastValues =
        new HashMap<>(deviceLastValues.size());
    for (final Map.Entry<IDeviceID, Map<String, TimeValuePair>> entry :
        deviceLastValues.entrySet()) {
      final List<Pair<String, TimeValuePair>> lastValues = new ArrayList<>(entry.getValue().size());
      for (final Map.Entry<String, TimeValuePair> lastValueEntry : entry.getValue().entrySet()) {
        lastValues.add(new Pair<>(lastValueEntry.getKey(), lastValueEntry.getValue()));
      }
      finalDeviceLastValues.put(entry.getKey(), lastValues);
    }
    return finalDeviceLastValues;
  }

  private void updateMeasurementLastValue(
      final IDeviceID deviceId,
      final String measurement,
      final long endTime,
      final TimeValuePair newPair) {
    final Map<String, TimeValuePair> deviceMap = getOrCreateDeviceMap(deviceId);
    final int previousSize = deviceMap.size();
    final TimeValuePair oldPair = deviceMap.get(measurement);
    if (Objects.nonNull(oldPair) && oldPair.getTimestamp() > endTime) {
      return;
    }

    if (Objects.nonNull(oldPair)) {
      memoryCostInBytes -= oldPair.getSize();
    }
    if (Objects.nonNull(newPair)) {
      deviceMap.put(measurement, newPair);
      memoryCostInBytes += newPair.getSize();
    } else {
      deviceMap.remove(measurement);
    }
    memoryCostInBytes +=
        (long) (deviceMap.size() - previousSize) * RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;
    if (memoryCostInBytes > memoryBudgetInBytes) {
      deviceLastValues = null;
      memoryCostInBytes = 0;
    }
  }

  private Map<String, TimeValuePair> getOrCreateDeviceMap(final IDeviceID deviceId) {
    return deviceLastValues.computeIfAbsent(
        deviceId,
        id -> {
          final Map<String, TimeValuePair> deviceMap = new HashMap<>();
          memoryCostInBytes += RamUsageEstimator.shallowSizeOf(deviceMap);
          memoryCostInBytes += id.ramBytesUsed();
          return deviceMap;
        });
  }

  private static TimeValuePair buildLastValuePair(final IChunkMetadata chunkMetadata) {
    return Objects.isNull(chunkMetadata.getStatistics())
        ? null
        : buildLastValuePair(
            chunkMetadata.getDataType(),
            chunkMetadata.getStatistics().getLastValue(),
            chunkMetadata.getEndTime());
  }

  private static TimeValuePair buildLastValuePair(final TimeseriesMetadata timeseriesMetadata) {
    return buildLastValuePair(
        timeseriesMetadata.getTsDataType(),
        timeseriesMetadata.getStatistics().getLastValue(),
        timeseriesMetadata.getStatistics().getEndTime());
  }

  private static TimeValuePair buildLastValuePair(
      final TSDataType dataType, final Object value, final long endTime) {
    if (dataType == TSDataType.BLOB) {
      return null;
    }

    final TsPrimitiveType lastValue =
        TsPrimitiveType.getByType(
            dataType == TSDataType.VECTOR ? TSDataType.INT64 : dataType, value);
    return new TimeValuePair(endTime, lastValue);
  }

  private static Map<IDeviceID, Map<String, TimeValuePair>> restoreDeviceLastValues(
      final Map<IDeviceID, List<Pair<String, TimeValuePair>>> deviceLastValues) {
    final Map<IDeviceID, Map<String, TimeValuePair>> restoredDeviceLastValues =
        new HashMap<>(deviceLastValues.size());
    for (final Map.Entry<IDeviceID, List<Pair<String, TimeValuePair>>> entry :
        deviceLastValues.entrySet()) {
      final Map<String, TimeValuePair> restoredLastValues = new HashMap<>(entry.getValue().size());
      for (final Pair<String, TimeValuePair> lastValueEntry : entry.getValue()) {
        restoredLastValues.put(lastValueEntry.getLeft(), lastValueEntry.getRight());
      }
      restoredDeviceLastValues.put(entry.getKey(), restoredLastValues);
    }
    return restoredDeviceLastValues;
  }

  private static long calculateDeviceLastValuesMemoryCost(
      final Map<IDeviceID, Map<String, TimeValuePair>> deviceLastValues) {
    long lastValuesMemCost = 0;
    for (final Map.Entry<IDeviceID, Map<String, TimeValuePair>> entry :
        deviceLastValues.entrySet()) {
      lastValuesMemCost += entry.getKey().ramBytesUsed();
      lastValuesMemCost += RamUsageEstimator.shallowSizeOf(entry.getValue());
      lastValuesMemCost += RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;
      for (final Map.Entry<String, TimeValuePair> lastValueEntry : entry.getValue().entrySet()) {
        lastValuesMemCost += RamUsageEstimator.sizeOf(lastValueEntry.getKey());
        lastValuesMemCost +=
            Objects.nonNull(lastValueEntry.getValue())
                ? lastValueEntry.getValue().getSize()
                : RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        lastValuesMemCost += RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;
      }
    }
    return lastValuesMemCost;
  }
}
