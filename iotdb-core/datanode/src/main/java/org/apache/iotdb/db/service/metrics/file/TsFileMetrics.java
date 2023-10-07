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

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BinaryOperator;

public class TsFileMetrics implements IMetricSet {
  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileMetrics.class);
  private AbstractMetricService metricService = null;
  private AtomicBoolean hasRemainData = new AtomicBoolean(false);
  private static final String SEQUENCE = "sequence";
  private static final String UNSEQUENCE = "unsequence";
  private static final String LEVEL = "level";
  private static final String FILE_LEVEL_COUNT = "file_level_count";
  private static final String FILE_LEVEL_SIZE = "file_level_size";

  // database -> regionId -> sequence file size
  private final Map<String, Map<String, Long>> seqFileSizeMap = new ConcurrentHashMap<>();
  // database -> regionId -> unsequence file size
  private final Map<String, Map<String, Long>> unseqFileSizeMap = new ConcurrentHashMap<>();
  // database -> regionId -> sequence file count
  private final Map<String, Map<String, Integer>> seqFileCountMap = new ConcurrentHashMap<>();
  // database -> regionId -> unsequence file count
  private final Map<String, Map<String, Integer>> unseqFileCountMap = new ConcurrentHashMap<>();
  // database -> regionId -> sequence file size gauge
  private final Map<String, Map<String, Gauge>> seqFileSizeGaugeMap = new ConcurrentHashMap<>();
  // database -> regionId -> unsequence file size gauge
  private final Map<String, Map<String, Gauge>> unseqFileSizeGaugeMap = new ConcurrentHashMap<>();
  // database -> regionId -> sequence file count gauge
  private final Map<String, Map<String, Gauge>> seqFileCountGaugeMap = new ConcurrentHashMap<>();
  // database -> regionId -> unsequence file count gauge
  private final Map<String, Map<String, Gauge>> unseqFileCountGaugeMap = new ConcurrentHashMap<>();

  // level -> sequence file count
  private final Map<Integer, Integer> seqLevelTsFileCountMap = new ConcurrentHashMap<>();
  // level -> unsequence file count
  private final Map<Integer, Integer> unseqLevelTsFileCountMap = new ConcurrentHashMap<>();
  // level -> sequence file size
  private final Map<Integer, Long> seqLevelTsFileSizeMap = new ConcurrentHashMap<>();
  // level -> unsequence file size
  private final Map<Integer, Long> unseqLevelTsFileSizeMap = new ConcurrentHashMap<>();
  // level -> sequence file count gauge
  private final Map<Integer, Gauge> seqLevelCountGaugeMap = new ConcurrentHashMap<>();
  // level -> sequence file size gauge
  private final Map<Integer, Gauge> seqLevelSizeGaugeMap = new ConcurrentHashMap<>();
  // level -> unsequence file count gauge
  private final Map<Integer, Gauge> unseqLevelCountGaugeMap = new ConcurrentHashMap<>();
  // level -> unsequence file size gauge
  private final Map<Integer, Gauge> unseqLevelSizeGaugeMap = new ConcurrentHashMap<>();

  @Override
  public void bindTo(AbstractMetricService metricService) {
    this.metricService = metricService;
    checkIfThereRemainingData();
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    this.metricService = null;
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
    Arrays.asList(
            seqFileCountGaugeMap,
            unseqFileCountGaugeMap,
            seqFileSizeGaugeMap,
            unseqFileSizeGaugeMap)
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
    long fileCount =
        updateGlobalTsFileValueMapAndGet(
            (seq ? seqFileCountMap : unseqFileCountMap),
            database,
            regionId,
            countDelta,
            Integer::sum);
    long fileSize =
        updateGlobalTsFileValueMapAndGet(
            (seq ? seqFileSizeMap : unseqFileSizeMap), database, regionId, sizeDelta, Long::sum);
    if (metricService != null) {
      updateGlobalTsFileGaugeMap(
          database,
          regionId,
          fileCount,
          (seq ? seqFileCountGaugeMap : unseqFileCountGaugeMap),
          (seq ? SEQUENCE : UNSEQUENCE),
          Metric.FILE_COUNT.toString());
      updateGlobalTsFileGaugeMap(
          database,
          regionId,
          fileSize,
          (seq ? seqFileSizeGaugeMap : unseqFileSizeGaugeMap),
          (seq ? SEQUENCE : UNSEQUENCE),
          Metric.FILE_SIZE.toString());
      checkIfThereRemainingData();
    } else {
      // the metric service has not been set yet
      hasRemainData.set(true);
    }
  }

  private <T> T updateGlobalTsFileValueMapAndGet(
      Map<String, Map<String, T>> map,
      String database,
      String regionId,
      T value,
      BinaryOperator<T> mergeFunction) {
    Map<String, T> innerMap = map.computeIfAbsent(database, k -> new ConcurrentHashMap<>());
    return innerMap.merge(regionId, value, mergeFunction);
  }

  private void updateGlobalTsFileGaugeMap(
      String database,
      String regionId,
      long value,
      Map<String, Map<String, Gauge>> gaugeMap,
      String orderStr,
      String gaugeName) {
    gaugeMap.compute(
        database,
        (k, v) -> {
          if (v == null) {
            v = new HashMap<>();
          }
          if (!v.containsKey(regionId)) {
            v.put(
                regionId,
                metricService.getOrCreateGauge(
                    gaugeName,
                    MetricLevel.CORE,
                    Tag.NAME.toString(),
                    orderStr,
                    Tag.DATABASE.toString(),
                    database,
                    Tag.REGION.toString(),
                    regionId));
          }
          v.get(regionId).set(value);
          return v;
        });
  }

  // endregion

  // region update level tsfile value map and gauge map

  private void updateLevelTsFileCountAndSize(
      long sizeDelta, int countDelta, boolean seq, int level) {
    int count =
        (seq ? seqLevelTsFileCountMap : unseqLevelTsFileCountMap)
            .compute(level, (k, v) -> v == null ? countDelta : v + countDelta);
    long size =
        (seq ? seqLevelTsFileSizeMap : unseqLevelTsFileSizeMap)
            .compute(level, (k, v) -> v == null ? sizeDelta : v + sizeDelta);
    if (metricService != null) {
      updateLevelTsFileGaugeMap(
          level,
          count,
          seq ? seqLevelCountGaugeMap : unseqLevelCountGaugeMap,
          seq ? SEQUENCE : UNSEQUENCE,
          FILE_LEVEL_COUNT);
      updateLevelTsFileGaugeMap(
          level,
          size,
          seq ? seqLevelSizeGaugeMap : unseqLevelSizeGaugeMap,
          seq ? SEQUENCE : UNSEQUENCE,
          FILE_LEVEL_SIZE);
      checkIfThereRemainingData();
    } else {
      // the metric service has not been set yet
      hasRemainData.set(true);
    }
  }

  private void updateLevelTsFileGaugeMap(
      int level, long value, Map<Integer, Gauge> gaugeMap, String orderStr, String gaugeName) {
    gaugeMap
        .computeIfAbsent(
            level,
            l ->
                metricService.getOrCreateGauge(
                    gaugeName,
                    MetricLevel.CORE,
                    Tag.NAME.toString(),
                    orderStr,
                    LEVEL,
                    String.valueOf(level)))
        .set(value);
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

  private void updateRemainData(boolean seq) {
    for (Map.Entry<String, Map<String, Integer>> entry :
        (seq ? seqFileCountMap : unseqFileCountMap).entrySet()) {
      for (Map.Entry<String, Integer> innerEntry : entry.getValue().entrySet()) {
        updateGlobalTsFileGaugeMap(
            entry.getKey(),
            innerEntry.getKey(),
            innerEntry.getValue(),
            seq ? seqFileCountGaugeMap : unseqFileCountGaugeMap,
            seq ? SEQUENCE : UNSEQUENCE,
            Metric.FILE_COUNT.toString());
      }
    }
    for (Map.Entry<String, Map<String, Long>> entry :
        (seq ? seqFileSizeMap : unseqFileSizeMap).entrySet()) {
      for (Map.Entry<String, Long> innerEntry : entry.getValue().entrySet()) {
        updateGlobalTsFileGaugeMap(
            entry.getKey(),
            innerEntry.getKey(),
            innerEntry.getValue(),
            seq ? seqFileSizeGaugeMap : unseqFileSizeGaugeMap,
            seq ? SEQUENCE : UNSEQUENCE,
            Metric.FILE_SIZE.toString());
      }
    }
    for (Map.Entry<Integer, Integer> entry :
        (seq ? seqLevelTsFileCountMap : unseqLevelTsFileCountMap).entrySet()) {
      updateLevelTsFileGaugeMap(
          entry.getKey(),
          entry.getValue(),
          seq ? seqLevelCountGaugeMap : unseqLevelCountGaugeMap,
          seq ? SEQUENCE : UNSEQUENCE,
          FILE_LEVEL_COUNT);
    }
    for (Map.Entry<Integer, Long> entry :
        (seq ? seqLevelTsFileSizeMap : unseqLevelTsFileSizeMap).entrySet()) {
      updateLevelTsFileGaugeMap(
          entry.getKey(),
          entry.getValue(),
          seq ? seqLevelSizeGaugeMap : unseqLevelSizeGaugeMap,
          seq ? SEQUENCE : UNSEQUENCE,
          FILE_LEVEL_SIZE);
    }
  }

  // endregion

  @TestOnly
  public long getFileCount(boolean seq) {
    long fileCount = 0;
    for (Map.Entry<String, Map<String, Integer>> entry :
        (seq ? seqFileCountMap : unseqFileCountMap).entrySet()) {
      for (Map.Entry<String, Integer> regionEntry : entry.getValue().entrySet()) {
        fileCount += regionEntry.getValue();
      }
    }
    return fileCount;
  }
}
