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
import org.apache.iotdb.db.engine.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.schedule.constant.CompactionType;
import org.apache.iotdb.db.engine.compaction.schedule.constant.ProcessChunkType;
import org.apache.iotdb.metrics.utils.MetricLevel;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CompactionMetricsManager {
  private static final CompactionMetricsManager INSTANCE = new CompactionMetricsManager();
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

  public void reportAddTaskToWaitingQueue(boolean isCrossTask, boolean isSeq) {
    if (isCrossTask) {
      waitingCrossCompactionTaskNum.incrementAndGet();
    } else if (isSeq) {
      waitingSeqInnerCompactionTaskNum.incrementAndGet();
    } else {
      waitingUnseqInnerCompactionTaskNum.incrementAndGet();
    }
  }

  public void reportPollTaskFromWaitingQueue(boolean isCrossTask, boolean isSeq) {
    if (isCrossTask) {
      waitingCrossCompactionTaskNum.decrementAndGet();
    } else if (isSeq) {
      waitingSeqInnerCompactionTaskNum.decrementAndGet();
    } else {
      waitingUnseqInnerCompactionTaskNum.decrementAndGet();
    }
  }

  public void reportTaskStartRunning(boolean isCrossTask, boolean isSeq) {
    if (isCrossTask) {
      runningCrossCompactionTaskNum.incrementAndGet();
    } else if (isSeq) {
      runningSeqInnerCompactionTaskNum.incrementAndGet();
    } else {
      runningUnseqInnerCompactionTaskNum.incrementAndGet();
    }
  }

  public void reportTaskFinishOrAbort(boolean isCrossTask, boolean isSeq, long timeCost) {
    if (isCrossTask) {
      runningCrossCompactionTaskNum.decrementAndGet();
      finishCrossCompactionTaskNum.incrementAndGet();
      MetricService.getInstance()
          .timer(
              timeCost,
              TimeUnit.MILLISECONDS,
              Metric.COST_TASK.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              "cross_compaction");
    } else if (isSeq) {
      runningSeqInnerCompactionTaskNum.decrementAndGet();
      finishSeqInnerCompactionTaskNum.incrementAndGet();
      MetricService.getInstance()
          .timer(
              timeCost,
              TimeUnit.MILLISECONDS,
              Metric.COST_TASK.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              "inner_seq_compaction");
    } else {
      runningUnseqInnerCompactionTaskNum.decrementAndGet();
      finishUnseqInnerCompactionTaskNum.incrementAndGet();
      MetricService.getInstance()
          .timer(
              timeCost,
              TimeUnit.MILLISECONDS,
              Metric.COST_TASK.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              "inner_unseq_compaction");
    }
  }

  public int getWaitingSeqInnerCompactionTaskNum() {
    return waitingSeqInnerCompactionTaskNum.get();
  }

  public int getWaitingUnseqInnerCompactionTaskNum() {
    return waitingUnseqInnerCompactionTaskNum.get();
  }

  public int getWaitingCrossCompactionTaskNum() {
    return waitingCrossCompactionTaskNum.get();
  }

  public int getRunningSeqInnerCompactionTaskNum() {
    return runningSeqInnerCompactionTaskNum.get();
  }

  public int getRunningUnseqInnerCompactionTaskNum() {
    return runningUnseqInnerCompactionTaskNum.get();
  }

  public int getRunningCrossCompactionTaskNum() {
    return runningCrossCompactionTaskNum.get();
  }

  public int getFinishSeqInnerCompactionTaskNum() {
    return finishSeqInnerCompactionTaskNum.get();
  }

  public int getFinishUnseqInnerCompactionTaskNum() {
    return finishUnseqInnerCompactionTaskNum.get();
  }

  public int getFinishCrossCompactionTaskNum() {
    return finishCrossCompactionTaskNum.get();
  }
}
