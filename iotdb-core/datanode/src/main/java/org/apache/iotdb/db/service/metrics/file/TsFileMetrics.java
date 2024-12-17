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
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class TsFileMetrics implements IMetricSet {
  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileMetrics.class);
  private final AtomicReference<AbstractMetricService> metricService = new AtomicReference<>();
  private final AtomicBoolean hasRemainData = new AtomicBoolean(false);
  private static final String SEQUENCE = "seq";
  private static final String UNSEQUENCE = "unseq";
  private static final String LEVEL = "level";
  private static final String FILE_GLOBAL_COUNT = "file_global_count";
  private static final String FILE_GLOBAL_SIZE = "file_global_size";
  private static final String FILE_LEVEL_COUNT = "file_level_count";
  private static final String FILE_LEVEL_SIZE = "file_level_size";

  // database -> regionId -> sequence file size
  private final Map<String, Map<String, Pair<Long, Gauge>>> seqFileSizeMap =
      new ConcurrentHashMap<>();
  // database -> regionId -> unsequence file size
  private final Map<String, Map<String, Pair<Long, Gauge>>> unseqFileSizeMap =
      new ConcurrentHashMap<>();
  // database -> regionId -> sequence file count
  private final Map<String, Map<String, Pair<Integer, Gauge>>> seqFileCountMap =
      new ConcurrentHashMap<>();
  // database -> regionId -> unsequence file count
  private final Map<String, Map<String, Pair<Integer, Gauge>>> unseqFileCountMap =
      new ConcurrentHashMap<>();

  // level -> sequence file count
  private final Map<Integer, Pair<Integer, Gauge>> seqLevelTsFileCountMap =
      new ConcurrentHashMap<>();
  // level -> unsequence file count
  private final Map<Integer, Pair<Integer, Gauge>> unseqLevelTsFileCountMap =
      new ConcurrentHashMap<>();
  // level -> sequence file size
  private final Map<Integer, Pair<Long, Gauge>> seqLevelTsFileSizeMap = new ConcurrentHashMap<>();
  // level -> unsequence file size
  private final Map<Integer, Pair<Long, Gauge>> unseqLevelTsFileSizeMap = new ConcurrentHashMap<>();

  @Override
  public void bindTo(AbstractMetricService metricService) {
    this.metricService.set(metricService);
    checkIfThereRemainingData();
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    // do nothing here
  }

  // region external update tsfile related metrics
  public void addTsFile(String database, String regionId, long size, boolean seq, String name) {
    updateGlobalTsFileCountAndSize(database, regionId, 1, size, seq);
    try {
      TsFileNameGenerator.TsFileName tsFileName = TsFileNameGenerator.getTsFileName(name);
      int level = tsFileName.getInnerCompactionCnt();
      updateLevelTsFileCountAndSize(size, 1, seq, level);
    } catch (IOException e) {
      LOGGER.warn("Unexpected error occurred when getting tsfile name", e);
    }
  }

  public void deleteFile(boolean seq, List<TsFileResource> tsFileResourceList) {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      String name = tsFileResource.getTsFile().getName();
      long size = tsFileResource.getTsFileSize();
      updateGlobalTsFileCountAndSize(
          tsFileResource.getDatabaseName(), tsFileResource.getDataRegionId(), -1, -size, seq);
      try {
        TsFileNameGenerator.TsFileName tsFileName = TsFileNameGenerator.getTsFileName(name);
        int level = tsFileName.getInnerCompactionCnt();
        updateLevelTsFileCountAndSize(-size, -1, seq, level);
      } catch (IOException e) {
        LOGGER.warn("Unexpected error occurred when getting tsfile name", e);
      }
    }
  }

  public void deleteRegion(String database, String regionId) {
    Arrays.asList(seqFileCountMap, unseqFileCountMap)
        .forEach(map -> deleteRegionFromMap(map, database, regionId));
    Arrays.asList(seqFileSizeMap, unseqFileSizeMap)
        .forEach(map -> deleteRegionFromMap(map, database, regionId));
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

  // endregion

  // region update global tsfile value map and gauge map
  private void updateGlobalTsFileCountAndSize(
      String database, String regionId, int countDelta, long sizeDelta, boolean seq) {
    updateGlobalTsFileCountMap(
        (seq ? seqFileCountMap : unseqFileCountMap),
        (seq ? SEQUENCE : UNSEQUENCE),
        database,
        regionId,
        countDelta);
    updateGlobalTsFileSizeMap(
        (seq ? seqFileSizeMap : unseqFileSizeMap),
        (seq ? SEQUENCE : UNSEQUENCE),
        database,
        regionId,
        sizeDelta);
  }

  private void updateGlobalTsFileCountMap(
      Map<String, Map<String, Pair<Integer, Gauge>>> map,
      String orderStr,
      String database,
      String regionId,
      int value) {
    Map<String, Pair<Integer, Gauge>> innerMap =
        map.computeIfAbsent(database, k -> new ConcurrentHashMap<>());
    innerMap.compute(
        regionId,
        (k, v) -> {
          // add pair if regionId not exists, and update value
          if (v == null) {
            v = new Pair<>(value, null);
          } else {
            v.setLeft(v.getLeft() + value);
          }
          // try to add gauge if gauge not exists
          if (v.getRight() == null) {
            if (metricService.get() != null) {
              v.setRight(getOrCreateGlobalTsFileCountGauge(orderStr, database, regionId));
            } else {
              hasRemainData.set(true);
            }
          }
          // try to update gauge
          if (v.getRight() != null) {
            v.getRight().set(v.getLeft());
          }
          return v;
        });
  }

  public Gauge getOrCreateGlobalTsFileCountGauge(
      String orderStr, String database, String regionId) {
    return metricService
        .get()
        .getOrCreateGauge(
            FILE_GLOBAL_COUNT,
            MetricLevel.CORE,
            Tag.NAME.toString(),
            orderStr,
            Tag.DATABASE.toString(),
            database,
            Tag.REGION.toString(),
            regionId);
  }

  private void updateGlobalTsFileSizeMap(
      Map<String, Map<String, Pair<Long, Gauge>>> map,
      String orderStr,
      String database,
      String regionId,
      long value) {
    Map<String, Pair<Long, Gauge>> innerMap =
        map.computeIfAbsent(database, k -> new ConcurrentHashMap<>());
    innerMap.compute(
        regionId,
        (k, v) -> {
          // add pair if regionId not exists, and update value
          if (v == null) {
            v = new Pair<>(value, null);
          } else {
            v.setLeft(v.getLeft() + value);
          }
          // try to add gauge if gauge not exists
          if (v.getRight() == null) {
            if (metricService.get() != null) {
              v.setRight(getOrCreateGlobalTsFileSizeGauge(orderStr, database, regionId));
            } else {
              hasRemainData.set(true);
            }
          }
          // try to update gauge
          if (v.getRight() != null) {
            v.getRight().set(v.getLeft());
          }
          return v;
        });
  }

  public Gauge getOrCreateGlobalTsFileSizeGauge(String orderStr, String database, String regionId) {
    return metricService
        .get()
        .getOrCreateGauge(
            FILE_GLOBAL_SIZE,
            MetricLevel.CORE,
            Tag.NAME.toString(),
            orderStr,
            Tag.DATABASE.toString(),
            database,
            Tag.REGION.toString(),
            regionId);
  }

  // endregion

  // region update level tsfile value map and gauge map

  private void updateLevelTsFileCountAndSize(
      long sizeDelta, int countDelta, boolean seq, int level) {
    updateLevelTsFileCountMap(
        seq ? seqLevelTsFileCountMap : unseqLevelTsFileCountMap,
        seq ? SEQUENCE : UNSEQUENCE,
        level,
        countDelta);
    updateLevelTsFileSizeMap(
        seq ? seqLevelTsFileSizeMap : unseqLevelTsFileSizeMap,
        seq ? SEQUENCE : UNSEQUENCE,
        level,
        sizeDelta);
  }

  private void updateLevelTsFileCountMap(
      Map<Integer, Pair<Integer, Gauge>> map, String orderStr, int level, int value) {
    map.compute(
        level,
        (k, v) -> {
          // add pair if level not exists, and update value
          if (v == null) {
            v = new Pair<>(value, null);
          } else {
            v.setLeft(v.getLeft() + value);
          }
          // try to add gauge if gauge not exists
          if (v.getRight() == null) {
            if (metricService.get() != null) {
              v.setRight(getOrCreateLevelTsFileCountGauge(orderStr, level));
            } else {
              hasRemainData.set(true);
            }
          }
          // try to update gauge
          if (v.getRight() != null) {
            v.getRight().set(v.getLeft());
          }
          return v;
        });
  }

  public Gauge getOrCreateLevelTsFileCountGauge(String orderStr, int level) {
    return metricService
        .get()
        .getOrCreateGauge(
            FILE_LEVEL_COUNT,
            MetricLevel.CORE,
            Tag.NAME.toString(),
            orderStr,
            LEVEL,
            String.valueOf(level));
  }

  private void updateLevelTsFileSizeMap(
      Map<Integer, Pair<Long, Gauge>> map, String orderStr, int level, long value) {
    map.compute(
        level,
        (k, v) -> {
          // add pair if level not exists, and update value
          if (v == null) {
            v = new Pair<>(value, null);
          } else {
            v.setLeft(v.getLeft() + value);
          }
          // try to add gauge if gauge not exists
          if (v.getRight() == null) {
            if (metricService.get() != null) {
              v.setRight(getOrCreateLevelTsFileSizeGauge(orderStr, level));
            } else {
              hasRemainData.set(true);
            }
          }
          // try to update gauge
          if (v.getRight() != null) {
            v.getRight().set(v.getLeft());
          }
          return v;
        });
  }

  public Gauge getOrCreateLevelTsFileSizeGauge(String orderStr, int level) {
    return metricService
        .get()
        .getOrCreateGauge(
            FILE_LEVEL_SIZE,
            MetricLevel.CORE,
            Tag.NAME.toString(),
            orderStr,
            LEVEL,
            String.valueOf(level));
  }

  // endregion

  // region check remain data
  private void checkIfThereRemainingData() {
    if (hasRemainData.get()) {
      synchronized (this) {
        if (hasRemainData.get()) {
          hasRemainData.set(false);
          updateRemainData(true);
          updateRemainData(false);
        }
      }
    }
  }

  private synchronized void updateRemainData(boolean seq) {
    for (Map.Entry<String, Map<String, Pair<Integer, Gauge>>> entry :
        (seq ? seqFileCountMap : unseqFileCountMap).entrySet()) {
      for (Map.Entry<String, Pair<Integer, Gauge>> innerEntry : entry.getValue().entrySet()) {
        updateGlobalTsFileCountMap(
            (seq ? seqFileCountMap : unseqFileCountMap),
            (seq ? SEQUENCE : UNSEQUENCE),
            entry.getKey(),
            innerEntry.getKey(),
            0);
      }
    }
    for (Map.Entry<String, Map<String, Pair<Long, Gauge>>> entry :
        (seq ? seqFileSizeMap : unseqFileSizeMap).entrySet()) {
      for (Map.Entry<String, Pair<Long, Gauge>> innerEntry : entry.getValue().entrySet()) {
        updateGlobalTsFileSizeMap(
            (seq ? seqFileSizeMap : unseqFileSizeMap),
            (seq ? SEQUENCE : UNSEQUENCE),
            entry.getKey(),
            innerEntry.getKey(),
            0);
      }
    }
    for (Map.Entry<Integer, Pair<Integer, Gauge>> entry :
        (seq ? seqLevelTsFileCountMap : unseqLevelTsFileCountMap).entrySet()) {
      updateLevelTsFileCountMap(
          seq ? seqLevelTsFileCountMap : unseqLevelTsFileCountMap,
          seq ? SEQUENCE : UNSEQUENCE,
          entry.getKey(),
          0);
    }
    for (Map.Entry<Integer, Pair<Long, Gauge>> entry :
        (seq ? seqLevelTsFileSizeMap : unseqLevelTsFileSizeMap).entrySet()) {
      updateLevelTsFileSizeMap(
          seq ? seqLevelTsFileSizeMap : unseqLevelTsFileSizeMap,
          seq ? SEQUENCE : UNSEQUENCE,
          entry.getKey(),
          0);
    }
  }

  // endregion

  public Map<Integer, Long> getRegionSizeMap() {
    Map<Integer, Long> regionSizeMap = new HashMap<>();
    for (Map<String, Pair<Long, Gauge>> map : seqFileSizeMap.values()) {
      for (Map.Entry<String, Pair<Long, Gauge>> regionSizeEntry : map.entrySet()) {
        Integer regionId = Integer.parseInt(regionSizeEntry.getKey());
        regionSizeMap.put(regionId, regionSizeEntry.getValue().getLeft());
      }
    }
    for (Map<String, Pair<Long, Gauge>> map : unseqFileSizeMap.values()) {
      for (Map.Entry<String, Pair<Long, Gauge>> regionSizeEntry : map.entrySet()) {
        Integer regionId = Integer.parseInt(regionSizeEntry.getKey());
        regionSizeMap.merge(regionId, regionSizeEntry.getValue().getLeft(), Long::sum);
      }
    }
    return regionSizeMap;
  }

  @TestOnly
  public long getFileCount(boolean seq) {
    long fileCount = 0;
    for (Map.Entry<String, Map<String, Pair<Integer, Gauge>>> entry :
        (seq ? seqFileCountMap : unseqFileCountMap).entrySet()) {
      for (Map.Entry<String, Pair<Integer, Gauge>> regionEntry : entry.getValue().entrySet()) {
        fileCount += regionEntry.getValue().getLeft();
      }
    }
    return fileCount;
  }
}
