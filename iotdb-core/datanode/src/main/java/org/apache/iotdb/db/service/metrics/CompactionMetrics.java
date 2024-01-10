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
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskStatus;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionIoDataType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionType;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
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
  private static final String NOT_ALIGNED = "not_aligned";
  private static final String ALIGNED = "aligned";
  private static final String METADATA = "metadata";
  private static final List<String> TYPES = Arrays.asList(ALIGNED, NOT_ALIGNED);
  private static final CompactionMetrics INSTANCE = new CompactionMetrics();
  private long lastUpdateTime = 0L;
  private static final long UPDATE_INTERVAL = 10_000L;
  private final AtomicInteger waitingSeqInnerCompactionTaskNum = new AtomicInteger(0);
  private final AtomicInteger waitingUnseqInnerCompactionTaskNum = new AtomicInteger(0);
  private final AtomicInteger waitingCrossCompactionTaskNum = new AtomicInteger(0);
  private final AtomicInteger waitingInsertionCompactionTaskNum = new AtomicInteger(0);
  private final AtomicInteger runningSeqInnerCompactionTaskNum = new AtomicInteger(0);
  private final AtomicInteger runningUnseqInnerCompactionTaskNum = new AtomicInteger(0);
  private final AtomicInteger runningCrossCompactionTaskNum = new AtomicInteger(0);
  private final AtomicInteger runningInsertionCompactionTaskNum = new AtomicInteger(0);
  private final AtomicInteger finishSeqInnerCompactionTaskNum = new AtomicInteger(0);
  private final AtomicInteger finishUnseqInnerCompactionTaskNum = new AtomicInteger(0);
  private final AtomicInteger finishCrossCompactionTaskNum = new AtomicInteger(0);
  private final AtomicInteger finishInsertionCompactionTaskNum = new AtomicInteger(0);
  // compaction type -> Counter[ Not-Aligned, Aligned, Metadata]
  private final Map<String, Counter[]> writeCounters = new ConcurrentHashMap<>();
  private final Map<String, Counter[]> readCounters = new ConcurrentHashMap<>();

  private CompactionMetrics() {
    for (String type : TYPES) {
      readCounters.put(
          type,
          new Counter[] {
            DoNothingMetricManager.DO_NOTHING_COUNTER,
            DoNothingMetricManager.DO_NOTHING_COUNTER,
            DoNothingMetricManager.DO_NOTHING_COUNTER
          });
      writeCounters.put(
          type,
          new Counter[] {
            DoNothingMetricManager.DO_NOTHING_COUNTER,
            DoNothingMetricManager.DO_NOTHING_COUNTER,
            DoNothingMetricManager.DO_NOTHING_COUNTER
          });
    }
  }

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
                NOT_ALIGNED),
            metricService.getOrCreateCounter(
                Metric.DATA_WRITTEN.toString(),
                MetricLevel.IMPORTANT,
                Tag.TYPE.toString(),
                compactionType.toString(),
                Tag.NAME.toString(),
                ALIGNED),
            metricService.getOrCreateCounter(
                Metric.DATA_WRITTEN.toString(),
                MetricLevel.IMPORTANT,
                Tag.TYPE.toString(),
                compactionType.toString(),
                Tag.NAME.toString(),
                METADATA)
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
          NOT_ALIGNED);
      metricService.remove(
          MetricType.COUNTER,
          Metric.DATA_WRITTEN.toString(),
          Tag.TYPE.toString(),
          compactionType.toString(),
          Tag.NAME.toString(),
          ALIGNED);
      metricService.remove(
          MetricType.COUNTER,
          Metric.DATA_WRITTEN.toString(),
          Tag.TYPE.toString(),
          compactionType.toString(),
          Tag.NAME.toString(),
          METADATA);
    }
    metricService.remove(
        MetricType.COUNTER,
        Metric.DATA_WRITTEN.toString(),
        Tag.NAME.toString(),
        "compaction",
        Tag.TYPE.toString(),
        "total");
  }

  public void recordWriteInfo(
      CompactionType compactionType, CompactionIoDataType dataType, long byteNum) {
    Counter[] counters = writeCounters.get(compactionType.toString());
    if (counters != null) {
      counters[dataType.getValue()].inc(byteNum);
    }
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
                NOT_ALIGNED),
            metricService.getOrCreateCounter(
                Metric.DATA_READ.toString(),
                MetricLevel.IMPORTANT,
                Tag.TYPE.toString(),
                compactionType.toString(),
                Tag.NAME.toString(),
                ALIGNED),
            metricService.getOrCreateCounter(
                Metric.DATA_READ.toString(),
                MetricLevel.IMPORTANT,
                Tag.TYPE.toString(),
                compactionType.toString(),
                Tag.NAME.toString(),
                METADATA)
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
          NOT_ALIGNED);
      metricService.remove(
          MetricType.COUNTER,
          Metric.DATA_READ.toString(),
          Tag.TYPE.toString(),
          compactionType.toString(),
          Tag.NAME.toString(),
          ALIGNED);
      metricService.remove(
          MetricType.COUNTER,
          Metric.DATA_READ.toString(),
          Tag.TYPE.toString(),
          compactionType.toString(),
          Tag.NAME.toString(),
          METADATA);
    }
    metricService.remove(
        MetricType.COUNTER, Metric.DATA_READ.toString(), Tag.NAME.toString(), "compaction");
  }

  public void recordReadInfo(
      CompactionType compactionType, CompactionIoDataType dataType, long byteNum) {
    Counter[] counters = readCounters.get(compactionType.toString());
    if (counters != null) {
      counters[dataType.getValue()].inc(byteNum);
    }
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
  private Timer insertionCompactionCostTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  public void recordTaskFinishOrAbort(CompactionTaskType type, long timeCost) {
    switch (type) {
      case CROSS:
        finishCrossCompactionTaskNum.incrementAndGet();
        crossCompactionCostTimer.update(timeCost, TimeUnit.MILLISECONDS);
        break;
      case INSERTION:
        finishInsertionCompactionTaskNum.incrementAndGet();
        insertionCompactionCostTimer.update(timeCost, TimeUnit.MILLISECONDS);
        break;
      case INNER_SEQ:
        finishSeqInnerCompactionTaskNum.incrementAndGet();
        seqCompactionCostTimer.update(timeCost, TimeUnit.MILLISECONDS);
        break;
      case INNER_UNSEQ:
        finishUnseqInnerCompactionTaskNum.incrementAndGet();
        unSeqCompactionCostTimer.update(timeCost, TimeUnit.MILLISECONDS);
        break;
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
    insertionCompactionCostTimer =
        metricService.getOrCreateTimer(
            Metric.COST_TASK.toString(),
            MetricLevel.CORE,
            Tag.NAME.toString(),
            "insertion_compaction");
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
          return waitingInsertionCompactionTaskNum.get();
        },
        Tag.NAME.toString(),
        "compaction_insertion",
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
          return runningInsertionCompactionTaskNum.get();
        },
        Tag.NAME.toString(),
        "compaction_insertion",
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
        metrics -> {
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
    metricService.createAutoGauge(
        Metric.COMPACTION_TASK_COUNT.toString(),
        MetricLevel.IMPORTANT,
        this,
        (metrics) -> {
          updateCompactionTaskInfo();
          return finishInsertionCompactionTaskNum.get();
        },
        Tag.NAME.toString(),
        "insertion");
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
        MetricType.TIMER, Metric.COST_TASK.toString(), Tag.NAME.toString(), "insertion_compaction");
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
        "compaction_insertion",
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
        "compaction_insertion",
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
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.COMPACTION_TASK_COUNT.toString(),
        Tag.NAME.toString(),
        "insertion");
  }

  // endregion

  // region compaction task memory
  private Histogram seqInnerSpaceCompactionTaskMemory = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram unseqInnerSpaceCompactionTaskMemory =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram crossSpaceCompactionTaskMemory = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;

  public void updateCompactionMemoryMetrics(CompactionTaskType taskType, long memory) {
    switch (taskType) {
      case INNER_SEQ:
        seqInnerSpaceCompactionTaskMemory.update(memory);
        break;
      case INNER_UNSEQ:
        unseqInnerSpaceCompactionTaskMemory.update(memory);
        break;
      case CROSS:
        crossSpaceCompactionTaskMemory.update(memory);
        break;
      case INSERTION:
      default:
        break;
    }
  }

  private void bindCompactionTaskMemory(AbstractMetricService metricService) {
    seqInnerSpaceCompactionTaskMemory =
        metricService.getOrCreateHistogram(
            Metric.COMPACTION_TASK_MEMORY.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "seq");
    unseqInnerSpaceCompactionTaskMemory =
        metricService.getOrCreateHistogram(
            Metric.COMPACTION_TASK_MEMORY.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "unseq");
    crossSpaceCompactionTaskMemory =
        metricService.getOrCreateHistogram(
            Metric.COMPACTION_TASK_MEMORY.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "cross");
    metricService.createAutoGauge(
        Metric.COMPACTION_TASK_MEMORY_DISTRIBUTION.toString(),
        MetricLevel.IMPORTANT,
        this,
        metrics -> SystemInfo.getInstance().getCompactionMemoryCost().get(),
        Tag.NAME.toString(),
        "total_usage");
    metricService.createAutoGauge(
        Metric.COMPACTION_TASK_MEMORY_DISTRIBUTION.toString(),
        MetricLevel.IMPORTANT,
        this,
        metrics -> SystemInfo.getInstance().getSeqInnerSpaceCompactionMemoryCost().get(),
        Tag.NAME.toString(),
        "seq");
    metricService.createAutoGauge(
        Metric.COMPACTION_TASK_MEMORY_DISTRIBUTION.toString(),
        MetricLevel.IMPORTANT,
        this,
        metrics -> SystemInfo.getInstance().getUnseqInnerSpaceCompactionMemoryCost().get(),
        Tag.NAME.toString(),
        "unseq");
    metricService.createAutoGauge(
        Metric.COMPACTION_TASK_MEMORY_DISTRIBUTION.toString(),
        MetricLevel.IMPORTANT,
        this,
        metrics -> SystemInfo.getInstance().getCrossSpaceCompactionMemoryCost().get(),
        Tag.NAME.toString(),
        "cross");
  }

  private void unbindCompactionTaskMemory(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.HISTOGRAM, Metric.COMPACTION_TASK_MEMORY.toString(), Tag.NAME.toString(), "seq");
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.COMPACTION_TASK_MEMORY.toString(),
        Tag.NAME.toString(),
        "unseq");
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.COMPACTION_TASK_MEMORY.toString(),
        Tag.NAME.toString(),
        "cross");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.COMPACTION_TASK_MEMORY_DISTRIBUTION.toString(),
        Tag.NAME.toString(),
        "total");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.COMPACTION_TASK_MEMORY_DISTRIBUTION.toString(),
        Tag.NAME.toString(),
        "cross");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.COMPACTION_TASK_MEMORY_DISTRIBUTION.toString(),
        Tag.NAME.toString(),
        "seq");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.COMPACTION_TASK_MEMORY_DISTRIBUTION.toString(),
        Tag.NAME.toString(),
        "unseq");
  }

  // endregion

  // region compaction task selection
  private Gauge seqInnerSpaceCompactionTaskSelectedNum = DoNothingMetricManager.DO_NOTHING_GAUGE;
  private Gauge unseqInnerSpaceCompactionTaskSelectedNum = DoNothingMetricManager.DO_NOTHING_GAUGE;
  private Gauge crossInnerSpaceCompactionTaskSelectedNum = DoNothingMetricManager.DO_NOTHING_GAUGE;
  private Gauge insertionCrossSpaceCompactionTaskSelectedNum =
      DoNothingMetricManager.DO_NOTHING_GAUGE;

  private Histogram seqSpaceCompactionTaskSelectionTimeCost =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram unseqSpaceCompactionTaskSelectionTimeCost =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram crossSpaceCompactionTaskSelectionTimeCost =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram insertionCrossSpaceCompactionTaskSelectionTimeCost =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;

  private Histogram seqInnerSpaceCompactionTaskSelectedFileNum =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram unseqInnerSpaceCompactionTaskSelectedFileNum =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram crossSpaceCompactionTaskSelectedFileNum =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;

  public void updateCompactionTaskSelectionNum(CompactionScheduleSummary summary) {
    seqInnerSpaceCompactionTaskSelectedNum.set(summary.getSubmitSeqInnerSpaceCompactionTaskNum());
    unseqInnerSpaceCompactionTaskSelectedNum.set(
        summary.getSubmitUnseqInnerSpaceCompactionTaskNum());
    crossInnerSpaceCompactionTaskSelectedNum.set(summary.getSubmitCrossSpaceCompactionTaskNum());
    insertionCrossSpaceCompactionTaskSelectedNum.set(
        summary.getSubmitInsertionCrossSpaceCompactionTaskNum());
  }

  public void updateCompactionTaskSelectionTimeCost(CompactionTaskType taskType, long time) {
    switch (taskType) {
      case INNER_SEQ:
        seqSpaceCompactionTaskSelectionTimeCost.update(time);
        break;
      case INNER_UNSEQ:
        unseqSpaceCompactionTaskSelectionTimeCost.update(time);
        break;
      case CROSS:
        crossSpaceCompactionTaskSelectionTimeCost.update(time);
        break;
      case INSERTION:
        insertionCrossSpaceCompactionTaskSelectionTimeCost.update(time);
        break;
      default:
        break;
    }
  }

  public void updateCompactionTaskSelectedFileNum(
      CompactionTaskType taskType, int selectedFileNum) {
    switch (taskType) {
      case INNER_SEQ:
        seqInnerSpaceCompactionTaskSelectedFileNum.update(selectedFileNum);
        break;
      case INNER_UNSEQ:
        unseqInnerSpaceCompactionTaskSelectedFileNum.update(selectedFileNum);
        break;
      case CROSS:
        crossSpaceCompactionTaskSelectedFileNum.update(selectedFileNum);
        break;
      case INSERTION:
      default:
        break;
    }
  }

  private void bindCompactionTaskSelection(AbstractMetricService metricService) {
    seqInnerSpaceCompactionTaskSelectedNum =
        metricService.getOrCreateGauge(
            Metric.COMPACTION_TASK_SELECTION.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "seq");
    unseqInnerSpaceCompactionTaskSelectedNum =
        metricService.getOrCreateGauge(
            Metric.COMPACTION_TASK_SELECTION.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "unseq");
    crossInnerSpaceCompactionTaskSelectedNum =
        metricService.getOrCreateGauge(
            Metric.COMPACTION_TASK_SELECTION.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "cross");
    insertionCrossSpaceCompactionTaskSelectedNum =
        metricService.getOrCreateGauge(
            Metric.COMPACTION_TASK_SELECTION.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "insertion");
    seqSpaceCompactionTaskSelectionTimeCost =
        metricService.getOrCreateHistogram(
            Metric.COMPACTION_TASK_SELECTION_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "seq");
    unseqSpaceCompactionTaskSelectionTimeCost =
        metricService.getOrCreateHistogram(
            Metric.COMPACTION_TASK_SELECTION_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "unseq");
    crossSpaceCompactionTaskSelectionTimeCost =
        metricService.getOrCreateHistogram(
            Metric.COMPACTION_TASK_SELECTION_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "cross");
    insertionCrossSpaceCompactionTaskSelectionTimeCost =
        metricService.getOrCreateHistogram(
            Metric.COMPACTION_TASK_SELECTION_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "insertion");
    seqInnerSpaceCompactionTaskSelectedFileNum =
        metricService.getOrCreateHistogram(
            Metric.COMPACTION_TASK_SELECTED_FILE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "seq");
    unseqInnerSpaceCompactionTaskSelectedFileNum =
        metricService.getOrCreateHistogram(
            Metric.COMPACTION_TASK_SELECTED_FILE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "unseq");
    crossSpaceCompactionTaskSelectedFileNum =
        metricService.getOrCreateHistogram(
            Metric.COMPACTION_TASK_SELECTED_FILE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "cross");
  }

  private void unbindCompactionTaskSelection(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.GAUGE, Metric.COMPACTION_TASK_SELECTION.toString(), Tag.NAME.toString(), "seq");
    metricService.remove(
        MetricType.GAUGE,
        Metric.COMPACTION_TASK_SELECTION.toString(),
        Tag.NAME.toString(),
        "unseq");
    metricService.remove(
        MetricType.GAUGE,
        Metric.COMPACTION_TASK_SELECTION.toString(),
        Tag.NAME.toString(),
        "cross");
    metricService.remove(
        MetricType.GAUGE,
        Metric.COMPACTION_TASK_SELECTION.toString(),
        Tag.NAME.toString(),
        "insertion");
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.COMPACTION_TASK_SELECTION_COST.toString(),
        Tag.NAME.toString(),
        "seq");
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.COMPACTION_TASK_SELECTION_COST.toString(),
        Tag.NAME.toString(),
        "unseq");
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.COMPACTION_TASK_SELECTION_COST.toString(),
        Tag.NAME.toString(),
        "cross");
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.COMPACTION_TASK_SELECTION_COST.toString(),
        Tag.NAME.toString(),
        "insertion");
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.COMPACTION_TASK_SELECTED_FILE.toString(),
        Tag.NAME.toString(),
        "seq");
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.COMPACTION_TASK_SELECTED_FILE.toString(),
        Tag.NAME.toString(),
        "unseq");
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.COMPACTION_TASK_SELECTED_FILE.toString(),
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
    bindCompactionTaskMemory(metricService);
    bindCompactionTaskSelection(metricService);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    unbindTaskInfo(metricService);
    unbindWriteInfo(metricService);
    unbindReadInfo(metricService);
    unbindPerformanceInfo(metricService);
    unbindCompactionTaskMemory(metricService);
    unbindCompactionTaskSelection(metricService);
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
    this.waitingInsertionCompactionTaskNum.set(
        compactionTaskStatisticMap
            .getOrDefault(CompactionTaskType.INSERTION, Collections.emptyMap())
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
    this.runningInsertionCompactionTaskNum.set(
        compactionTaskStatisticMap
            .getOrDefault(CompactionTaskType.INSERTION, Collections.emptyMap())
            .getOrDefault(CompactionTaskStatus.RUNNING, 0));
  }

  public static CompactionMetrics getInstance() {
    return INSTANCE;
  }
}
