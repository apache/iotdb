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
import org.apache.iotdb.db.engine.compaction.constant.CompactionTaskStatus;
import org.apache.iotdb.db.engine.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.engine.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.schedule.constant.CompactionType;
import org.apache.iotdb.db.engine.compaction.schedule.constant.ProcessChunkType;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CompactionMetrics implements IMetricSet {
  private static final List<String> TYPES = Arrays.asList("aligned", "not_aligned");
  private static final CompactionMetrics INSTANCE = new CompactionMetrics();
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
  // compaction type -> Counter[ Not-Aligned, Aligned]
  private final Map<String, Counter[]> writeCounters = new ConcurrentHashMap<>();
  private final Map<String, Counter[]> readCounters = new ConcurrentHashMap<>();

  private CompactionMetrics() {
    for (String type : TYPES) {
      readCounters.put(
          type,
          new Counter[] {
            DoNothingMetricManager.DO_NOTHING_COUNTER, DoNothingMetricManager.DO_NOTHING_COUNTER
          });
      writeCounters.put(
          type,
          new Counter[] {
            DoNothingMetricManager.DO_NOTHING_COUNTER, DoNothingMetricManager.DO_NOTHING_COUNTER
          });
    }
  }

  // region compaction write info
  private Map<String, Map<CompactionType, Map<ProcessChunkType, Counter>>> writeInfoCounterMap =
      new ConcurrentHashMap<>();
  private Counter totalCompactionWriteInfoCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;

  private void bindWriteInfo(AbstractMetricService metricService) {
    for (CompactionType compactionType : CompactionType.values()) {
      writeCounters.put(
          compactionType.toString(),
          new Counter[] {
            metricService.getOrCreateCounter(
                Metric.DATA_WRITTEN.toString(),
                MetricLevel.IMPORTANT,
                Tag.TYPE.toString(),
                compactionType.toString(),
                Tag.NAME.toString(),
                "not_aligned"),
            metricService.getOrCreateCounter(
                Metric.DATA_WRITTEN.toString(),
                MetricLevel.IMPORTANT,
                Tag.TYPE.toString(),
                compactionType.toString(),
                Tag.NAME.toString(),
                "aligned")
          });
    }
    totalCompactionWriteInfoCounter =
        metricService.getOrCreateCounter(
            Metric.DATA_WRITTEN.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "compaction",
            Tag.TYPE.toString(),
            "total");
  }

  private void unbindWriteInfo(AbstractMetricService metricService) {
    for (CompactionType compactionType : CompactionType.values()) {
      metricService.remove(
          MetricType.COUNTER,
          Metric.DATA_WRITTEN.toString(),
          Tag.TYPE.toString(),
          compactionType.toString(),
          Tag.NAME.toString(),
          "not_aligned");
      metricService.remove(
          MetricType.COUNTER,
          Metric.DATA_WRITTEN.toString(),
          Tag.TYPE.toString(),
          compactionType.toString(),
          Tag.NAME.toString(),
          "aligned");
    }
    metricService.remove(
        MetricType.COUNTER,
        Metric.DATA_WRITTEN.toString(),
        Tag.NAME.toString(),
        "compaction",
        Tag.TYPE.toString(),
        "total");
  }

  public void recordWriteInfo(CompactionType compactionType, boolean aligned, long byteNum) {
    Counter[] counters = writeCounters.get(compactionType.toString());
    counters[aligned ? 1 : 0].inc(byteNum);
    totalCompactionWriteInfoCounter.inc(byteNum);
  }

  // endregion

  // region compaction read info
  private Counter totalCompactionReadInfoCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;

  private void bindReadInfo(AbstractMetricService metricService) {
    for (CompactionType compactionType : CompactionType.values()) {
      readCounters.put(
          compactionType.toString(),
          new Counter[] {
            metricService.getOrCreateCounter(
                Metric.DATA_READ.toString(),
                MetricLevel.IMPORTANT,
                Tag.TYPE.toString(),
                compactionType.toString(),
                Tag.NAME.toString(),
                "not_aligned"),
            metricService.getOrCreateCounter(
                Metric.DATA_READ.toString(),
                MetricLevel.IMPORTANT,
                Tag.TYPE.toString(),
                compactionType.toString(),
                Tag.NAME.toString(),
                "aligned")
          });
    }
    totalCompactionReadInfoCounter =
        metricService.getOrCreateCounter(
            Metric.DATA_READ.toString(), MetricLevel.IMPORTANT, Tag.NAME.toString(), "compaction");
  }

  private void unbindReadInfo(AbstractMetricService metricService) {
    for (CompactionType compactionType : CompactionType.values()) {
      metricService.remove(
          MetricType.COUNTER,
          Metric.DATA_READ.toString(),
          Tag.TYPE.toString(),
          compactionType.toString(),
          Tag.NAME.toString(),
          "not_aligned");
      metricService.remove(
          MetricType.COUNTER,
          Metric.DATA_READ.toString(),
          Tag.TYPE.toString(),
          compactionType.toString(),
          Tag.NAME.toString(),
          "aligned");
    }
    metricService.remove(
        MetricType.COUNTER, Metric.DATA_READ.toString(), Tag.NAME.toString(), "compaction");
  }

  public void recordReadInfo(CompactionType compactionType, boolean aligned, long byteNum) {
    Counter[] counters = readCounters.get(compactionType.toString());
    counters[aligned ? 1 : 0].inc(byteNum);
    totalCompactionReadInfoCounter.inc(byteNum);
  }
  // endregion

  // region compaction summary info
  private Counter totalCompactedPointCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter totalCompactedChunkCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter totalDirectlyFlushChunkCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter totalDeserializedChunkCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter totalMergedChunkCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;

  private void bindPerformanceInfo(AbstractMetricService metricService) {
    totalCompactedPointCounter =
        metricService.getOrCreateCounter(
            "compacted_point_num", MetricLevel.IMPORTANT, Tag.NAME.toString(), "compaction");
    totalCompactedChunkCounter =
        metricService.getOrCreateCounter(
            "compacted_chunk_num", MetricLevel.IMPORTANT, Tag.NAME.toString(), "compaction");
    totalDirectlyFlushChunkCounter =
        metricService.getOrCreateCounter(
            "directly_flush_chunk_num", MetricLevel.NORMAL, Tag.NAME.toString(), "compaction");
    totalDeserializedChunkCounter =
        metricService.getOrCreateCounter(
            "deserialized_chunk_num", MetricLevel.NORMAL, Tag.NAME.toString(), "compaction");
    totalMergedChunkCounter =
        metricService.getOrCreateCounter(
            "merged_chunk_num", MetricLevel.NORMAL, Tag.NAME.toString(), "compaction");
  }

  private void unbindPerformanceInfo(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.COUNTER, "compacted_point_num", Tag.NAME.toString(), "compaction");
    metricService.remove(
        MetricType.COUNTER, "compacted_chunk_num", Tag.NAME.toString(), "compaction");
    metricService.remove(
        MetricType.COUNTER, "directly_flush_chunk_num", Tag.NAME.toString(), "compaction");
    metricService.remove(
        MetricType.COUNTER, "deserialized_chunk_num", Tag.NAME.toString(), "compaction");
    metricService.remove(MetricType.COUNTER, "merged_chunk_num", Tag.NAME.toString(), "compaction");
  }

  public void recordSummaryInfo(CompactionTaskSummary summary) {
    totalCompactedPointCounter.inc(summary.getProcessPointNum());
    totalCompactedChunkCounter.inc(summary.getProcessChunkNum());
    totalDirectlyFlushChunkCounter.inc(summary.getDirectlyFlushChunkNum());
    totalDeserializedChunkCounter.inc(summary.getDeserializeChunkCount());
    totalMergedChunkCounter.inc(summary.getMergedChunkNum());
  }
  // endregion

  // region task info
  private Timer seqCompactionCostTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer unSeqCompactionCostTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer crossCompactionCostTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  public void recordTaskFinishOrAbort(boolean isCrossTask, boolean isSeq, long timeCost) {
    if (isCrossTask) {
      finishCrossCompactionTaskNum.incrementAndGet();
      crossCompactionCostTimer.update(timeCost, TimeUnit.MILLISECONDS);
    } else if (isSeq) {
      finishSeqInnerCompactionTaskNum.incrementAndGet();
      seqCompactionCostTimer.update(timeCost, TimeUnit.MILLISECONDS);
    } else {
      finishUnseqInnerCompactionTaskNum.incrementAndGet();
      unSeqCompactionCostTimer.update(timeCost, TimeUnit.MILLISECONDS);
    }
  }

  private void bindTaskInfo(AbstractMetricService metricService) {
    seqCompactionCostTimer =
        metricService.getOrCreateTimer(
            Metric.COST_TASK.toString(),
            MetricLevel.CORE,
            Tag.NAME.toString(),
            "inner_seq_compaction");
    unSeqCompactionCostTimer =
        metricService.getOrCreateTimer(
            Metric.COST_TASK.toString(),
            MetricLevel.CORE,
            Tag.NAME.toString(),
            "inner_unseq_compaction");
    crossCompactionCostTimer =
        metricService.getOrCreateTimer(
            Metric.COST_TASK.toString(), MetricLevel.CORE, Tag.NAME.toString(), "cross_compaction");
    metricService.createAutoGauge(
        Metric.QUEUE.toString(),
        MetricLevel.IMPORTANT,
        this,
        (metrics) -> {
          updateCompactionTaskInfo();
          return waitingCrossCompactionTaskNum.get();
        },
        Tag.NAME.toString(),
        "compaction_cross",
        Tag.STATUS.toString(),
        "waiting");
    metricService.createAutoGauge(
        Metric.QUEUE.toString(),
        MetricLevel.IMPORTANT,
        this,
        (metrics) -> {
          updateCompactionTaskInfo();
          return waitingSeqInnerCompactionTaskNum.get();
        },
        Tag.NAME.toString(),
        "compaction_inner_seq",
        Tag.STATUS.toString(),
        "waiting");
    metricService.createAutoGauge(
        Metric.QUEUE.toString(),
        MetricLevel.IMPORTANT,
        this,
        (metrics) -> {
          updateCompactionTaskInfo();
          return waitingUnseqInnerCompactionTaskNum.get();
        },
        Tag.NAME.toString(),
        "compaction_inner_unseq",
        Tag.STATUS.toString(),
        "waiting");
    metricService.createAutoGauge(
        Metric.QUEUE.toString(),
        MetricLevel.IMPORTANT,
        this,
        (metrics) -> {
          updateCompactionTaskInfo();
          return runningCrossCompactionTaskNum.get();
        },
        Tag.NAME.toString(),
        "compaction_cross",
        Tag.STATUS.toString(),
        "running");
    metricService.createAutoGauge(
        Metric.QUEUE.toString(),
        MetricLevel.IMPORTANT,
        this,
        (metrics) -> {
          updateCompactionTaskInfo();
          return runningSeqInnerCompactionTaskNum.get();
        },
        Tag.NAME.toString(),
        "compaction_inner_seq",
        Tag.STATUS.toString(),
        "running");
    metricService.createAutoGauge(
        Metric.QUEUE.toString(),
        MetricLevel.IMPORTANT,
        this,
        (metrics) -> {
          updateCompactionTaskInfo();
          return runningUnseqInnerCompactionTaskNum.get();
        },
        Tag.NAME.toString(),
        "compaction_inner_unseq",
        Tag.STATUS.toString(),
        "running");
    metricService.createAutoGauge(
        Metric.COMPACTION_TASK_COUNT.toString(),
        MetricLevel.IMPORTANT,
        this,
        (metrics) -> {
          updateCompactionTaskInfo();
          return finishSeqInnerCompactionTaskNum.get();
        },
        Tag.NAME.toString(),
        "inner_seq");
    metricService.createAutoGauge(
        Metric.COMPACTION_TASK_COUNT.toString(),
        MetricLevel.IMPORTANT,
        this,
        (metrics) -> {
          updateCompactionTaskInfo();
          return finishUnseqInnerCompactionTaskNum.get();
        },
        Tag.NAME.toString(),
        "inner_unseq");
    metricService.createAutoGauge(
        Metric.COMPACTION_TASK_COUNT.toString(),
        MetricLevel.IMPORTANT,
        this,
        (metrics) -> {
          updateCompactionTaskInfo();
          return finishCrossCompactionTaskNum.get();
        },
        Tag.NAME.toString(),
        "cross");
  }

  private void unbindTaskInfo(AbstractMetricService metricService) {
    seqCompactionCostTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    unSeqCompactionCostTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    crossCompactionCostTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    metricService.remove(
        MetricType.TIMER, Metric.COST_TASK.toString(), Tag.NAME.toString(), "inner_seq_compaction");
    metricService.remove(
        MetricType.TIMER,
        Metric.COST_TASK.toString(),
        Tag.NAME.toString(),
        "inner_unseq_compaction");
    metricService.remove(
        MetricType.TIMER, Metric.COST_TASK.toString(), Tag.NAME.toString(), "cross_compaction");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.QUEUE.toString(),
        Tag.NAME.toString(),
        "compaction_cross",
        Tag.STATUS.toString(),
        "waiting");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.QUEUE.toString(),
        Tag.NAME.toString(),
        "compaction_inner_seq",
        Tag.STATUS.toString(),
        "waiting");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.QUEUE.toString(),
        Tag.NAME.toString(),
        "compaction_inner_unseq",
        Tag.STATUS.toString(),
        "waiting");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.QUEUE.toString(),
        Tag.NAME.toString(),
        "compaction_cross",
        Tag.STATUS.toString(),
        "running");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.QUEUE.toString(),
        Tag.NAME.toString(),
        "compaction_inner_seq",
        Tag.STATUS.toString(),
        "running");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.QUEUE.toString(),
        Tag.NAME.toString(),
        "compaction_inner_unseq",
        Tag.STATUS.toString(),
        "running");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.COMPACTION_TASK_COUNT.toString(),
        Tag.NAME.toString(),
        "inner_seq");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.COMPACTION_TASK_COUNT.toString(),
        Tag.NAME.toString(),
        "inner_unseq");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.COMPACTION_TASK_COUNT.toString(),
        Tag.NAME.toString(),
        "cross");
  }

  // endregion

  @Override
  public void bindTo(AbstractMetricService metricService) {
    bindTaskInfo(metricService);
    bindWriteInfo(metricService);
    bindReadInfo(metricService);
    bindPerformanceInfo(metricService);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    unbindTaskInfo(metricService);
    unbindWriteInfo(metricService);
    unbindReadInfo(metricService);
    unbindPerformanceInfo(metricService);
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
            .getOrDefault(CompactionTaskStatus.WAITING, 0));
    this.waitingUnseqInnerCompactionTaskNum.set(
        compactionTaskStatisticMap
            .getOrDefault(CompactionTaskType.INNER_UNSEQ, Collections.emptyMap())
            .getOrDefault(CompactionTaskStatus.WAITING, 0));
    this.waitingCrossCompactionTaskNum.set(
        compactionTaskStatisticMap
            .getOrDefault(CompactionTaskType.CROSS, Collections.emptyMap())
            .getOrDefault(CompactionTaskStatus.WAITING, 0));
    this.runningSeqInnerCompactionTaskNum.set(
        compactionTaskStatisticMap
            .getOrDefault(CompactionTaskType.INNER_SEQ, Collections.emptyMap())
            .getOrDefault(CompactionTaskStatus.RUNNING, 0));
    this.runningUnseqInnerCompactionTaskNum.set(
        compactionTaskStatisticMap
            .getOrDefault(CompactionTaskType.INNER_UNSEQ, Collections.emptyMap())
            .getOrDefault(CompactionTaskStatus.RUNNING, 0));
    this.runningCrossCompactionTaskNum.set(
        compactionTaskStatisticMap
            .getOrDefault(CompactionTaskType.CROSS, Collections.emptyMap())
            .getOrDefault(CompactionTaskStatus.RUNNING, 0));
  }

  public static CompactionMetrics getInstance() {
    return INSTANCE;
  }
}
