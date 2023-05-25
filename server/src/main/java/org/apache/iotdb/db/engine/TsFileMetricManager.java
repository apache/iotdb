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

package org.apache.iotdb.db.engine;

import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.engine.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** This class collect the number and size of tsfile, and send it to the {@link FileMetrics} */
public class TsFileMetricManager {
  private static final Logger log = LoggerFactory.getLogger(TsFileMetricManager.class);
  private static final TsFileMetricManager INSTANCE = new TsFileMetricManager();
  private static final String FILE_LEVEL_COUNT = "file_level_count";
  private static final String SEQUENCE = "sequence";
  private static final String UNSEQUENCE = "unsequence";
  private static final String LEVEL = "level";
  private final AtomicLong seqFileSize = new AtomicLong(0);
  private final AtomicLong unseqFileSize = new AtomicLong(0);
  private final AtomicInteger seqFileNum = new AtomicInteger(0);
  private final AtomicInteger unseqFileNum = new AtomicInteger(0);

  private final AtomicInteger modFileNum = new AtomicInteger(0);

  private final AtomicLong modFileSize = new AtomicLong(0);
  private final Map<Integer, Integer> seqLevelTsFileCountMap = new HashMap<>();
  private final Map<Integer, Integer> unseqLevelTsFileCountMap = new HashMap<>();
  private long lastUpdateTime = 0;
  private static final long UPDATE_INTERVAL = 10_000L;

  // compaction temporal files
  private final AtomicLong innerSeqCompactionTempFileSize = new AtomicLong(0);
  private final AtomicLong innerUnseqCompactionTempFileSize = new AtomicLong(0);
  private final AtomicLong crossCompactionTempFileSize = new AtomicLong(0);
  private final AtomicInteger innerSeqCompactionTempFileNum = new AtomicInteger(0);
  private final AtomicInteger innerUnseqCompactionTempFileNum = new AtomicInteger(0);
  private final AtomicInteger crossCompactionTempFileNum = new AtomicInteger(0);
  private AbstractMetricService metricService;

  private TsFileMetricManager() {}

  public static TsFileMetricManager getInstance() {
    return INSTANCE;
  }

  public void setMetricService(AbstractMetricService metricService) {
    this.metricService = metricService;
  }

  public void addFile(long size, boolean seq, String name) {
    if (seq) {
      seqFileSize.getAndAdd(size);
      seqFileNum.incrementAndGet();
    } else {
      unseqFileSize.getAndAdd(size);
      unseqFileNum.incrementAndGet();
    }
    try {
      TsFileNameGenerator.TsFileName tsFileName = TsFileNameGenerator.getTsFileName(name);
      int level = tsFileName.getInnerCompactionCnt();
      int count = -1;
      if (seq) {
        count = seqLevelTsFileCountMap.compute(level, (k, v) -> v == null ? 1 : v + 1);
      } else {
        count = unseqLevelTsFileCountMap.compute(level, (k, v) -> v == null ? 1 : v + 1);
      }
      if (metricService != null) {
        metricService
            .getOrCreateGauge(
                FILE_LEVEL_COUNT,
                MetricLevel.CORE,
                Tag.TYPE.toString(),
                seq ? SEQUENCE : UNSEQUENCE,
                LEVEL,
                String.valueOf(level))
            .set(count);
      }
    } catch (IOException e) {
      log.error("Unexpected error occurred when getting tsfile name", e);
    }
  }

  public void deleteFile(long size, boolean seq, int num, List<String> names) {
    if (seq) {
      seqFileSize.getAndAdd(-size);
      seqFileNum.getAndAdd(-num);
    } else {
      unseqFileSize.getAndAdd(-size);
      unseqFileNum.getAndAdd(-num);
    }
    for (String name : names) {
      int level = -1;
      int count = -1;
      try {
        TsFileNameGenerator.TsFileName tsFileName = TsFileNameGenerator.getTsFileName(name);
        level = tsFileName.getInnerCompactionCnt();
        count =
            seq
                ? seqLevelTsFileCountMap.compute(level, (k, v) -> v == null ? 0 : v - 1)
                : unseqLevelTsFileCountMap.compute(level, (k, v) -> v == null ? 0 : v - 1);
      } catch (IOException e) {
        log.error("Unexpected error occurred when getting tsfile name", e);
      }
      if (metricService != null && level != -1 && count != -1) {
        metricService
            .getOrCreateGauge(
                FILE_LEVEL_COUNT,
                MetricLevel.CORE,
                Tag.TYPE.toString(),
                seq ? SEQUENCE : UNSEQUENCE,
                LEVEL,
                String.valueOf(level))
            .set(count);
      }
    }
  }

  public long getFileSize(boolean seq) {
    return seq ? seqFileSize.get() : unseqFileSize.get();
  }

  public long getFileNum(boolean seq) {
    return seq ? seqFileNum.get() : unseqFileNum.get();
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
    if (System.currentTimeMillis() - lastUpdateTime <= UPDATE_INTERVAL) {
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
}
