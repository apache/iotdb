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

package org.apache.iotdb.db.service.metrics.file;

import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import org.apache.tsfile.utils.Pair;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ObjectFileMetrics implements IMetricSet {
  private static final String OBJECT = "object";
  private static final String FILE_GLOBAL_COUNT = "object_file_global_count";
  private static final String FILE_GLOBAL_SIZE = "object_file_global_size";

  private final AtomicReference<AbstractMetricService> metricService = new AtomicReference<>();
  private final AtomicBoolean hasRemainData = new AtomicBoolean(false);
  // database -> regionId -> object file num
  private final Map<String, Map<String, Pair<Integer, Gauge>>> dataRegionObjectFileNumMap =
      new ConcurrentHashMap<>();
  // database -> regionId -> object file size
  private final Map<String, Map<String, Pair<Long, Gauge>>> dataRegionObjectFileSizeMap =
      new ConcurrentHashMap<>();

  @Override
  public void bindTo(AbstractMetricService metricService) {
    this.metricService.set(metricService);
    checkIfThereRemainingData();
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    // do nothing here
  }

  public void increaseObjectFileNum(String database, String regionId, int num) {
    updateObjectFileNumMap(database, regionId, num);
  }

  public void decreaseObjectFileNum(String database, String regionId, int num) {
    updateObjectFileNumMap(database, regionId, -num);
  }

  public void increaseObjectFileSize(String database, String regionId, long size) {
    updateObjectFileSizeMap(database, regionId, size);
  }

  public void decreaseObjectFileSize(String database, String regionId, long size) {
    updateObjectFileSizeMap(database, regionId, -size);
  }

  public void deleteRegion(String database, String regionId) {
    deleteRegionFromMap(dataRegionObjectFileNumMap, database, regionId);
    deleteRegionFromMap(dataRegionObjectFileSizeMap, database, regionId);
    deleteObjectFileNumGauge(database, regionId);
    deleteObjectFileSizeGauge(database, regionId);
  }

  private <T> void deleteRegionFromMap(
      Map<String, Map<String, T>> map, String database, String regionId) {
    map.computeIfPresent(
        database,
        (k, v) -> {
          v.remove(regionId);
          return v.isEmpty() ? null : v;
        });
  }

  private void updateObjectFileNumMap(String database, String regionId, int value) {
    Map<String, Pair<Integer, Gauge>> innerMap =
        dataRegionObjectFileNumMap.computeIfAbsent(database, k -> new ConcurrentHashMap<>());
    innerMap.compute(
        regionId,
        (k, v) -> {
          if (v == null) {
            v = new Pair<>(value, null);
          } else {
            v.setLeft(v.getLeft() + value);
          }
          if (v.getRight() == null) {
            if (metricService.get() != null) {
              v.setRight(getOrCreateObjectFileNumGauge(database, regionId));
            } else {
              hasRemainData.set(true);
            }
          }
          if (v.getRight() != null) {
            v.getRight().set(v.getLeft());
          }
          return v;
        });
  }

  private void updateObjectFileSizeMap(String database, String regionId, long value) {
    Map<String, Pair<Long, Gauge>> innerMap =
        dataRegionObjectFileSizeMap.computeIfAbsent(database, k -> new ConcurrentHashMap<>());
    innerMap.compute(
        regionId,
        (k, v) -> {
          if (v == null) {
            v = new Pair<>(value, null);
          } else {
            v.setLeft(v.getLeft() + value);
          }
          if (v.getRight() == null) {
            if (metricService.get() != null) {
              v.setRight(getOrCreateObjectFileSizeGauge(database, regionId));
            } else {
              hasRemainData.set(true);
            }
          }
          if (v.getRight() != null) {
            v.getRight().set(v.getLeft());
          }
          return v;
        });
  }

  public Gauge getOrCreateObjectFileNumGauge(String database, String regionId) {
    return metricService
        .get()
        .getOrCreateGauge(
            FILE_GLOBAL_COUNT,
            MetricLevel.CORE,
            Tag.NAME.toString(),
            OBJECT,
            Tag.DATABASE.toString(),
            database,
            Tag.REGION.toString(),
            regionId);
  }

  public void deleteObjectFileNumGauge(String database, String regionId) {
    Optional.ofNullable(this.metricService.get())
        .ifPresent(
            service ->
                service.remove(
                    MetricType.GAUGE,
                    FILE_GLOBAL_COUNT,
                    Tag.NAME.toString(),
                    OBJECT,
                    Tag.DATABASE.toString(),
                    database,
                    Tag.REGION.toString(),
                    regionId));
  }

  public Gauge getOrCreateObjectFileSizeGauge(String database, String regionId) {
    return metricService
        .get()
        .getOrCreateGauge(
            FILE_GLOBAL_SIZE,
            MetricLevel.CORE,
            Tag.NAME.toString(),
            OBJECT,
            Tag.DATABASE.toString(),
            database,
            Tag.REGION.toString(),
            regionId);
  }

  public void deleteObjectFileSizeGauge(String database, String regionId) {
    Optional.ofNullable(this.metricService.get())
        .ifPresent(
            service ->
                service.remove(
                    MetricType.GAUGE,
                    FILE_GLOBAL_SIZE,
                    Tag.NAME.toString(),
                    OBJECT,
                    Tag.DATABASE.toString(),
                    database,
                    Tag.REGION.toString(),
                    regionId));
  }

  private void checkIfThereRemainingData() {
    if (hasRemainData.get()) {
      synchronized (this) {
        if (hasRemainData.get()) {
          hasRemainData.set(false);
          updateRemainData();
        }
      }
    }
  }

  private synchronized void updateRemainData() {
    for (Map.Entry<String, Map<String, Pair<Integer, Gauge>>> entry :
        dataRegionObjectFileNumMap.entrySet()) {
      for (Map.Entry<String, Pair<Integer, Gauge>> innerEntry : entry.getValue().entrySet()) {
        updateObjectFileNumMap(entry.getKey(), innerEntry.getKey(), 0);
      }
    }
    for (Map.Entry<String, Map<String, Pair<Long, Gauge>>> entry :
        dataRegionObjectFileSizeMap.entrySet()) {
      for (Map.Entry<String, Pair<Long, Gauge>> innerEntry : entry.getValue().entrySet()) {
        updateObjectFileSizeMap(entry.getKey(), innerEntry.getKey(), 0);
      }
    }
  }
}
