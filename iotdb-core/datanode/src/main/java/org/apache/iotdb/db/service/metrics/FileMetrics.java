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

package org.apache.iotdb.db.service.metrics;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.MetricConstant;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.metrics.utils.SystemType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("java:S6548") // do not warn about singleton class
public class FileMetrics implements IMetricSet {
  private static final Logger log = LoggerFactory.getLogger(FileMetrics.class);
  private static final MetricConfig METRIC_CONFIG =
      MetricConfigDescriptor.getInstance().getMetricConfig();
  private static final WALManager WAL_MANAGER = WALManager.getInstance();
  private final Runtime runtime = Runtime.getRuntime();
  private String[] getOpenFileNumberCommand;

  private AbstractMetricService metricService = null;
  private static final String FILE_LEVEL_COUNT = "file_level_count";
  private static final String FILE_LEVEL_SIZE = "file_level_size";
  private static final String SEQUENCE = "sequence";
  private static final String UNSEQUENCE = "unsequence";
  private static final String LEVEL = "level";
  // database -> regionId -> sequence file size
  private final Map<String, Map<String, Long>> seqFileSizeMap = new ConcurrentHashMap<>();
  // database -> regionId -> unsequence file size
  private final Map<String, Map<String, Long>> unseqFileSizeMap = new ConcurrentHashMap<>();
  // database -> regionId -> sequence file number
  private final Map<String, Map<String, Integer>> seqFileNumMap = new ConcurrentHashMap<>();
  // database -> regionId -> unsequence file number
  private final Map<String, Map<String, Integer>> unseqFileNumMap = new ConcurrentHashMap<>();
  // database -> regionId -> sequence file size gauge
  private final Map<String, Map<String, Gauge>> seqFileSizeGaugeMap = new ConcurrentHashMap<>();
  // database -> regionId -> unsequence file size gauge
  private final Map<String, Map<String, Gauge>> unseqFileSizeGaugeMap = new ConcurrentHashMap<>();
  // database -> regionId -> sequence file number gauge
  private final Map<String, Map<String, Gauge>> seqFileNumGaugeMap = new ConcurrentHashMap<>();
  // database -> regionId -> unsequence file number gauge
  private final Map<String, Map<String, Gauge>> unseqFileNumGaugeMap = new ConcurrentHashMap<>();

  private final AtomicInteger modFileNum = new AtomicInteger(0);
  private final AtomicLong modFileSize = new AtomicLong(0);
  // level -> database -> regionId -> sequence file number
  private final Map<Integer, Map<String, Map<String, Integer>>> seqLevelTsFileCountMap =
      new ConcurrentHashMap<>();
  // level -> database -> regionId -> unsequence file number
  private final Map<Integer, Map<String, Map<String, Integer>>> unseqLevelTsFileCountMap =
      new ConcurrentHashMap<>();
  // level -> database -> regionId -> sequence file size
  private final Map<Integer, Map<String, Map<String, Long>>> seqLevelTsFileSizeMap =
      new ConcurrentHashMap<>();
  // level -> database -> regionId -> unsequence file size
  private final Map<Integer, Map<String, Map<String, Long>>> unseqLevelTsFileSizeMap =
      new ConcurrentHashMap<>();
  // level -> database -> regionId -> sequence file number gauge
  private final Map<Integer, Map<String, Map<String, Gauge>>> seqLevelCountGaugeMap =
      new ConcurrentHashMap<>();
  // level -> database -> regionId -> sequence file size gauge
  private final Map<Integer, Map<String, Map<String, Gauge>>> seqLevelSizeGaugeMap =
      new ConcurrentHashMap<>();
  // level -> database -> regionId -> unsequence file number gauge
  private final Map<Integer, Map<String, Map<String, Gauge>>> unseqLevelCountGaugeMap =
      new ConcurrentHashMap<>();
  // level -> database -> regionId -> unsequence file size gauge
  private final Map<Integer, Map<String, Map<String, Gauge>>> unseqLevelSizeGaugeMap =
      new ConcurrentHashMap<>();
  private long lastUpdateTime = 0;

  // compaction temporal files
  private final AtomicLong innerSeqCompactionTempFileSize = new AtomicLong(0);
  private final AtomicLong innerUnseqCompactionTempFileSize = new AtomicLong(0);
  private final AtomicLong crossCompactionTempFileSize = new AtomicLong(0);
  private final AtomicInteger innerSeqCompactionTempFileNum = new AtomicInteger(0);
  private final AtomicInteger innerUnseqCompactionTempFileNum = new AtomicInteger(0);
  private final AtomicInteger crossCompactionTempFileNum = new AtomicInteger(0);

  @SuppressWarnings("squid:S1075")
  private String fileHandlerCntPathInLinux = "/proc/%s/fd";

  private FileMetrics() {
    fileHandlerCntPathInLinux = String.format(fileHandlerCntPathInLinux, METRIC_CONFIG.getPid());
  }

  public static FileMetrics getInstance() {
    return FileMetricsInstanceHolder.INSTANCE;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    this.metricService = metricService;
    bindTsFileMetrics(metricService);
    bindWalFileMetrics(metricService);
    bindCompactionFileMetrics(metricService);
    bindSystemRelatedMetrics(metricService);
  }

  private void bindTsFileMetrics(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.CORE,
        this,
        FileMetrics::getModFileSize,
        Tag.NAME.toString(),
        "mods");
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.CORE,
        this,
        FileMetrics::getModFileNum,
        Tag.NAME.toString(),
        "mods");
  }

  private void bindWalFileMetrics(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.CORE,
        WAL_MANAGER,
        WALManager::getTotalDiskUsage,
        Tag.NAME.toString(),
        "wal");
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.CORE,
        WAL_MANAGER,
        WALManager::getTotalFileNum,
        Tag.NAME.toString(),
        "wal");
  }

  private void bindCompactionFileMetrics(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.CORE,
        this,
        o -> o.getInnerCompactionTempFileSize(true),
        Tag.NAME.toString(),
        "inner-seq-temp");
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.CORE,
        this,
        o -> o.getInnerCompactionTempFileSize(false),
        Tag.NAME.toString(),
        "inner-unseq-temp");
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.CORE,
        this,
        FileMetrics::getCrossCompactionTempFileSize,
        Tag.NAME.toString(),
        "cross-temp");
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.CORE,
        this,
        o -> o.getInnerCompactionTempFileNum(true),
        Tag.NAME.toString(),
        "inner-seq-temp");
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.CORE,
        this,
        o -> o.getInnerCompactionTempFileNum(false),
        Tag.NAME.toString(),
        "inner-unseq-temp");
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.CORE,
        this,
        FileMetrics::getCrossCompactionTempFileNum,
        Tag.NAME.toString(),
        "cross-temp");
  }

  private void bindSystemRelatedMetrics(AbstractMetricService metricService) {
    if ((METRIC_CONFIG.getSystemType() == SystemType.LINUX
            || METRIC_CONFIG.getSystemType() == SystemType.MAC)
        && METRIC_CONFIG.getPid().length() != 0) {
      this.getOpenFileNumberCommand =
          new String[] {
            "/bin/sh", "-c", String.format("lsof -p %s | wc -l", METRIC_CONFIG.getPid())
          };
      metricService.createAutoGauge(
          Metric.FILE_COUNT.toString(),
          MetricLevel.IMPORTANT,
          this,
          FileMetrics::getOpenFileHandlersNumber,
          Tag.NAME.toString(),
          "open_file_handlers");
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    unbindTsFileMetrics(metricService);
    unbindWalMetrics(metricService);
    unbindCompactionMetrics(metricService);
    unbindSystemRelatedMetrics(metricService);
  }

  private void unbindTsFileMetrics(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_SIZE.toString(), Tag.NAME.toString(), "seq");
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_SIZE.toString(), Tag.NAME.toString(), "unseq");
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_SIZE.toString(), Tag.NAME.toString(), "mods");
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_COUNT.toString(), Tag.NAME.toString(), "seq");
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_COUNT.toString(), Tag.NAME.toString(), "unseq");
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_COUNT.toString(), Tag.NAME.toString(), "mods");
  }

  private void unbindWalMetrics(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_SIZE.toString(), Tag.NAME.toString(), "wal");
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_COUNT.toString(), Tag.NAME.toString(), "wal");
  }

  private void unbindCompactionMetrics(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_COUNT.toString(), Tag.NAME.toString(), "inner-seq-temp");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.FILE_COUNT.toString(),
        Tag.NAME.toString(),
        "inner-unseq-temp");
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_COUNT.toString(), Tag.NAME.toString(), "cross-temp");
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_SIZE.toString(), Tag.NAME.toString(), "inner-seq-temp");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.FILE_SIZE.toString(),
        Tag.NAME.toString(),
        "inner-unseq-temp");
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_SIZE.toString(), Tag.NAME.toString(), "cross-temp");
  }

  private void unbindSystemRelatedMetrics(AbstractMetricService metricService) {
    if ((METRIC_CONFIG.getSystemType() == SystemType.LINUX
            || METRIC_CONFIG.getSystemType() == SystemType.MAC)
        && METRIC_CONFIG.getPid().length() != 0) {
      metricService.remove(
          MetricType.AUTO_GAUGE,
          Metric.FILE_COUNT.toString(),
          Tag.NAME.toString(),
          "open_file_handlers");
    }
  }

  private long getOpenFileHandlersNumber() {
    long fdCount = 0;
    try {
      if (METRIC_CONFIG.getSystemType() == SystemType.LINUX) {
        // count the fd in the system directory instead of
        // calling runtime.exec() which could be much slower
        File fdDir = new File(fileHandlerCntPathInLinux);
        if (fdDir.exists()) {
          File[] fds = fdDir.listFiles();
          fdCount = fds == null ? 0 : fds.length;
        }
      } else if ((METRIC_CONFIG.getSystemType() == SystemType.MAC)
          && METRIC_CONFIG.getPid().length() != 0) {
        Process process = runtime.exec(getOpenFileNumberCommand);
        StringBuilder result = new StringBuilder();
        try (BufferedReader input =
            new BufferedReader(new InputStreamReader(process.getInputStream()))) {
          String line;
          while ((line = input.readLine()) != null) {
            result.append(line);
          }
        }
        fdCount = Long.parseLong(result.toString().trim());
      }
    } catch (IOException e) {
      log.warn("Failed to get open file number, because ", e);
    }
    return fdCount;
  }

  // following are update functions for tsfile metrics
  public void addFile(String database, String regionId, long size, boolean seq, String name) {
    updateGlobalCountAndSize(database, regionId, size, 1, seq);
    try {
      TsFileNameGenerator.TsFileName tsFileName = TsFileNameGenerator.getTsFileName(name);
      int level = tsFileName.getInnerCompactionCnt();
      updateLevelCountAndSize(database, regionId, size, 1, seq, level);
    } catch (IOException e) {
      log.warn("Unexpected error occurred when getting tsfile name", e);
    }
  }

  private void updateGlobalCountAndSize(
      String database, String regionId, long sizeDelta, int countDelta, boolean seq) {
    long fileSize =
        (seq ? seqFileSizeMap : unseqFileSizeMap)
            .compute(
                database,
                (k, v) -> {
                  long size = 0;
                  if (v == null) {
                    v = new HashMap<>();
                  } else if (v.containsKey(regionId)) {
                    size = v.get(regionId);
                  }
                  v.put(regionId, size + sizeDelta);
                  return v;
                })
            .get(regionId);
    long fileNum =
        (seq ? seqFileNumMap : unseqFileNumMap)
            .compute(
                database,
                (k, v) -> {
                  int count = 0;
                  if (v == null) {
                    v = new HashMap<>();
                  } else if (v.containsKey(regionId)) {
                    count = v.get(regionId);
                  }
                  v.put(regionId, count + countDelta);
                  return v;
                })
            .get(regionId);
    if (metricService != null) {
      updateGlobalGauge(
          database,
          regionId,
          fileSize,
          (seq ? seqFileSizeGaugeMap : unseqFileSizeGaugeMap),
          (seq ? SEQUENCE : UNSEQUENCE),
          Metric.FILE_SIZE.toString());
      updateGlobalGauge(
          database,
          regionId,
          fileNum,
          (seq ? seqFileNumGaugeMap : unseqFileNumGaugeMap),
          (seq ? SEQUENCE : UNSEQUENCE),
          Metric.FILE_COUNT.toString());
    }
  }

  private void updateGlobalGauge(
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

  private void updateLevelCountAndSize(
      String database, String regionId, long sizeDelta, int countDelta, boolean seq, int level) {
    int count =
        (seq ? seqLevelTsFileCountMap : unseqLevelTsFileCountMap)
            .compute(
                level,
                (k, v) -> {
                  if (v == null) {
                    v = new HashMap<>();
                  }
                  v.compute(
                      database,
                      (dk, dv) -> {
                        if (dv == null) {
                          dv = new HashMap<>();
                        }
                        dv.compute(
                            regionId, (rdk, rdv) -> rdv == null ? countDelta : rdv + countDelta);
                        return dv;
                      });
                  return v;
                })
            .get(database)
            .get(regionId);
    long totalSize =
        (seq ? seqLevelTsFileSizeMap : unseqLevelTsFileSizeMap)
            .compute(
                level,
                (k, v) -> {
                  if (v == null) {
                    v = new HashMap<>();
                  }
                  v.compute(
                      database,
                      (dk, dv) -> {
                        if (dv == null) {
                          dv = new HashMap<>();
                        }
                        dv.compute(
                            regionId, (rdk, rdv) -> rdv == null ? sizeDelta : rdv + sizeDelta);
                        return dv;
                      });
                  return v;
                })
            .get(database)
            .get(regionId);
    if (metricService != null) {
      updateLevelGauge(
          level,
          database,
          regionId,
          count,
          seq ? seqLevelCountGaugeMap : unseqLevelCountGaugeMap,
          seq ? SEQUENCE : UNSEQUENCE,
          FILE_LEVEL_COUNT);
      updateLevelGauge(
          level,
          database,
          regionId,
          totalSize,
          seq ? seqLevelSizeGaugeMap : unseqLevelSizeGaugeMap,
          seq ? SEQUENCE : UNSEQUENCE,
          FILE_LEVEL_SIZE);
    }
  }

  private void updateLevelGauge(
      int level,
      String database,
      String regionId,
      long value,
      Map<Integer, Map<String, Map<String, Gauge>>> gaugeMap,
      String orderStr,
      String gaugeName) {
    gaugeMap
        .compute(
            level,
            (k, v) -> {
              if (v == null) {
                v = new HashMap<>();
              }
              if (!v.containsKey(database)) {
                v.put(database, new HashMap<>());
              }
              if (!v.get(database).containsKey(regionId)) {
                v.get(database)
                    .put(
                        regionId,
                        metricService.getOrCreateGauge(
                            gaugeName,
                            MetricLevel.CORE,
                            Tag.NAME.toString(),
                            orderStr,
                            Tag.DATABASE.toString(),
                            database,
                            Tag.REGION.toString(),
                            regionId,
                            LEVEL,
                            String.valueOf(level)));
              }
              return v;
            })
        .get(database)
        .get(regionId)
        .set(value);
  }

  public void deleteFile(boolean seq, List<TsFileResource> tsFileResourceList) {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      String name = tsFileResource.getTsFile().getName();
      long size = tsFileResource.getTsFileSize();
      updateGlobalCountAndSize(
          tsFileResource.getDatabaseName(), tsFileResource.getDataRegionId(), -size, -1, seq);
      try {
        TsFileNameGenerator.TsFileName tsFileName = TsFileNameGenerator.getTsFileName(name);
        int level = tsFileName.getInnerCompactionCnt();
        updateLevelCountAndSize(
            tsFileResource.getDatabaseName(), tsFileResource.getDataRegionId(), -size,
            -1,
            seq,
            level
        );
      } catch (IOException e) {
        log.warn("Unexpected error occurred when getting tsfile name", e);
      }
    }
  }

  public long getFileNum(boolean seq) {
    long fileNum = 0;
    for (Map.Entry<String, Map<String, Integer>> entry :
        (seq ? seqFileNumMap : unseqFileNumMap).entrySet()) {
      for (Map.Entry<String, Integer> regionEntry : entry.getValue().entrySet()) {
        fileNum += regionEntry.getValue();
      }
    }
    return fileNum;
  }

  public int getModFileNum() {
    return modFileNum.get();
  }

  public long getModFileSize() {
    return modFileSize.get();
  }

  public void increaseModFileNum(int num) {
    modFileNum.addAndGet(num);
  }

  public void decreaseModFileNum(int num) {
    modFileNum.addAndGet(-num);
  }

  public void increaseModFileSize(long size) {
    modFileSize.addAndGet(size);
  }

  public void decreaseModFileSize(long size) {
    modFileSize.addAndGet(-size);
  }

  public long getInnerCompactionTempFileSize(boolean seq) {
    updateCompactionTempSize();
    return seq ? innerSeqCompactionTempFileSize.get() : innerUnseqCompactionTempFileSize.get();
  }

  private synchronized void updateCompactionTempSize() {
    if (System.currentTimeMillis() - lastUpdateTime <= MetricConstant.UPDATE_INTERVAL) {
      return;
    }
    lastUpdateTime = System.currentTimeMillis();

    innerSeqCompactionTempFileSize.set(0);
    innerSeqCompactionTempFileNum.set(0);
    innerUnseqCompactionTempFileSize.set(0);
    innerUnseqCompactionTempFileNum.set(0);
    crossCompactionTempFileSize.set(0);
    crossCompactionTempFileNum.set(0);

    List<AbstractCompactionTask> runningTasks =
        CompactionTaskManager.getInstance().getRunningCompactionTaskList();
    for (AbstractCompactionTask task : runningTasks) {
      CompactionTaskSummary summary = task.getSummary();
      if (task instanceof InnerSpaceCompactionTask) {
        if (task.isInnerSeqTask()) {
          innerSeqCompactionTempFileSize.addAndGet(summary.getTemporalFileSize());
          innerSeqCompactionTempFileNum.addAndGet(1);
        } else {
          innerUnseqCompactionTempFileSize.addAndGet(summary.getTemporalFileSize());
          innerUnseqCompactionTempFileNum.addAndGet(1);
        }
      } else {
        crossCompactionTempFileSize.addAndGet(summary.getTemporalFileSize());
        crossCompactionTempFileNum.addAndGet(summary.getTemporalFileNum());
      }
    }
  }

  public long getCrossCompactionTempFileSize() {
    updateCompactionTempSize();
    return crossCompactionTempFileSize.get();
  }

  public long getInnerCompactionTempFileNum(boolean seq) {
    updateCompactionTempSize();
    return seq ? innerSeqCompactionTempFileNum.get() : innerUnseqCompactionTempFileNum.get();
  }

  public long getCrossCompactionTempFileNum() {
    updateCompactionTempSize();
    return crossCompactionTempFileNum.get();
  }

  private static class FileMetricsInstanceHolder {
    private static final FileMetrics INSTANCE = new FileMetrics();

    private FileMetricsInstanceHolder() {}
  }
}
