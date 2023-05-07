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
package org.apache.iotdb.db.service.metrics.recorder;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.engine.compaction.constant.CompactionTaskStatus;
import org.apache.iotdb.db.engine.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.engine.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.schedule.constant.CompactionType;
import org.apache.iotdb.db.engine.compaction.schedule.constant.ProcessChunkType;
import org.apache.iotdb.metrics.utils.MetricLevel;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CompactionMetricsManager {
  private static final CompactionMetricsManager INSTANCE = new CompactionMetricsManager();
  private long lastUpdateTime = 0L;
  private static final long UPDATE_INTERVAL = 10_000L;
  private final AtomicInteger waitingSeqInnerCompactionTaskNum = new AtomicInteger(0);
  private final AtomicInteger waitingUnseqInnerCompactionTaskNum = new AtomicInteger(0);
  private final AtomicInteger waitingCrossCompactionTaskNum = new AtomicInteger(0);
  private final AtomicInteger runningSeqInnerCompactionTaskNum = new AtomicInteger(0);
  private final AtomicInteger runningUnseqInnerCompactionTaskNum = new AtomicInteger(0);
  private final AtomicInteger runningCrossCompactionTaskNum = new AtomicInteger(0);
  private final AtomicInteger finishSeqInnerCompactionTaskNum = new AtomicInteger(0);
  private final AtomicInteger finishUnseqInnerCompactionTaskNum = new AtomicInteger(0);
  private final AtomicInteger finishCrossCompactionTaskNum = new AtomicInteger(0);

  private CompactionMetricsManager() {}

  public static CompactionMetricsManager getInstance() {
    return INSTANCE;
  }

  public void recordWriteInfo(
      CompactionType compactionType,
      ProcessChunkType processChunkType,
      boolean aligned,
      long byteNum) {
    MetricService.getInstance()
        .count(
            byteNum / 1024L,
            Metric.DATA_WRITTEN.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "compaction_" + compactionType.toString(),
            Tag.TYPE.toString(),
            (aligned ? "ALIGNED" : "NOT_ALIGNED") + "_" + processChunkType.toString());
    MetricService.getInstance()
        .count(
            byteNum / 1024L,
            Metric.DATA_WRITTEN.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "compaction",
            Tag.TYPE.toString(),
            "total");
  }

  public void recordReadInfo(long byteNum) {
    MetricService.getInstance()
        .count(
            byteNum,
            Metric.DATA_READ.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "compaction");
  }

  public void updateSummary(CompactionTaskSummary summary) {
    MetricService.getInstance()
        .count(
            summary.getProcessPointNum(),
            "Compacted_Point_Num",
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "compaction");
    MetricService.getInstance()
        .count(
            summary.getProcessChunkNum(),
            "Compacted_Chunk_Num",
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "compaction");
    MetricService.getInstance()
        .count(
            summary.getDirectlyFlushChunkNum(),
            "Directly_Flush_Chunk_Num",
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            "compaction");
    MetricService.getInstance()
        .count(
            summary.getDeserializeChunkCount(),
            "Deserialized_Chunk_Num",
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            "compaction");
    MetricService.getInstance()
        .count(
            summary.getMergedChunkNum(),
            "Merged_Chunk_Num",
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            "compaction");
  }

  public void reportTaskFinishOrAbort(boolean isCrossTask, boolean isSeq, long timeCost) {
    if (isCrossTask) {
      finishCrossCompactionTaskNum.incrementAndGet();
      MetricService.getInstance()
          .timer(
              timeCost,
              TimeUnit.MILLISECONDS,
              Metric.COST_TASK.toString(),
              MetricLevel.CORE,
              Tag.NAME.toString(),
              "cross_compaction");
    } else if (isSeq) {
      finishSeqInnerCompactionTaskNum.incrementAndGet();
      MetricService.getInstance()
          .timer(
              timeCost,
              TimeUnit.MILLISECONDS,
              Metric.COST_TASK.toString(),
              MetricLevel.CORE,
              Tag.NAME.toString(),
              "inner_seq_compaction");
    } else {
      finishUnseqInnerCompactionTaskNum.incrementAndGet();
      MetricService.getInstance()
          .timer(
              timeCost,
              TimeUnit.MILLISECONDS,
              Metric.COST_TASK.toString(),
              MetricLevel.CORE,
              Tag.NAME.toString(),
              "inner_unseq_compaction");
    }
  }

  public int getWaitingSeqInnerCompactionTaskNum() {
    return waitingSeqInnerCompactionTaskNum.get();
  }

  public int getWaitingUnseqInnerCompactionTaskNum() {
    updateCompactionTaskInfo();
    return waitingUnseqInnerCompactionTaskNum.get();
  }

  public int getWaitingCrossCompactionTaskNum() {
    updateCompactionTaskInfo();
    return waitingCrossCompactionTaskNum.get();
  }

  public int getRunningSeqInnerCompactionTaskNum() {
    updateCompactionTaskInfo();
    return runningSeqInnerCompactionTaskNum.get();
  }

  public int getRunningUnseqInnerCompactionTaskNum() {
    updateCompactionTaskInfo();
    return runningUnseqInnerCompactionTaskNum.get();
  }

  public int getRunningCrossCompactionTaskNum() {
    updateCompactionTaskInfo();
    return runningCrossCompactionTaskNum.get();
  }

  public int getFinishSeqInnerCompactionTaskNum() {
    updateCompactionTaskInfo();
    return finishSeqInnerCompactionTaskNum.get();
  }

  public int getFinishUnseqInnerCompactionTaskNum() {
    updateCompactionTaskInfo();
    return finishUnseqInnerCompactionTaskNum.get();
  }

  public int getFinishCrossCompactionTaskNum() {
    updateCompactionTaskInfo();
    return finishCrossCompactionTaskNum.get();
  }

  private void updateCompactionTaskInfo() {
    if (System.currentTimeMillis() - lastUpdateTime < UPDATE_INTERVAL) {
      return;
    }
    lastUpdateTime = System.currentTimeMillis();
    Map<CompactionTaskType, Map<CompactionTaskStatus, Integer>> compactionTaskStatisticMap =
        CompactionTaskManager.getInstance().getCompactionTaskStatistic();
    this.waitingSeqInnerCompactionTaskNum.set(
        compactionTaskStatisticMap
            .getOrDefault(CompactionTaskType.INNER_SEQ, Collections.emptyMap())
            .getOrDefault(CompactionTaskStatus.Waiting, 0));
    this.waitingUnseqInnerCompactionTaskNum.set(
        compactionTaskStatisticMap
            .getOrDefault(CompactionTaskType.INNER_UNSEQ, Collections.emptyMap())
            .getOrDefault(CompactionTaskStatus.Waiting, 0));
    this.waitingCrossCompactionTaskNum.set(
        compactionTaskStatisticMap
            .getOrDefault(CompactionTaskType.CROSS, Collections.emptyMap())
            .getOrDefault(CompactionTaskStatus.Waiting, 0));
    this.runningSeqInnerCompactionTaskNum.set(
        compactionTaskStatisticMap
            .getOrDefault(CompactionTaskType.INNER_SEQ, Collections.emptyMap())
            .getOrDefault(CompactionTaskStatus.Running, 0));
    this.runningUnseqInnerCompactionTaskNum.set(
        compactionTaskStatisticMap
            .getOrDefault(CompactionTaskType.INNER_UNSEQ, Collections.emptyMap())
            .getOrDefault(CompactionTaskStatus.Running, 0));
    this.runningCrossCompactionTaskNum.set(
        compactionTaskStatisticMap
            .getOrDefault(CompactionTaskType.CROSS, Collections.emptyMap())
            .getOrDefault(CompactionTaskStatus.Running, 0));
  }
}
