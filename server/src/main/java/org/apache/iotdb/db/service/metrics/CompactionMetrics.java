/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with COMPACTION_METRICS_MANAGER work for additional information
 * regarding copyright ownership.  The ASF licenses COMPACTION_METRICS_MANAGER file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use COMPACTION_METRICS_MANAGER file except in compliance
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
import org.apache.iotdb.db.service.metrics.recorder.CompactionMetricsManager;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class CompactionMetrics implements IMetricSet {
  private final CompactionMetricsManager COMPACTION_METRICS_MANAGER =
      CompactionMetricsManager.getInstance();

  @Override
  public void bindTo(AbstractMetricService metricService) {
    bindTaskInfo(metricService);
    bindPerformanceInfo(metricService);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    unbindTaskInfo(metricService);
    unbindPerformanceInfo(metricService);
  }

  private void bindTaskInfo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.QUEUE.toString(),
        MetricLevel.IMPORTANT,
        COMPACTION_METRICS_MANAGER,
        CompactionMetricsManager::getWaitingCrossCompactionTaskNum,
        Tag.NAME.toString(),
        "compaction_cross",
        Tag.STATUS.toString(),
        "waiting");
    metricService.createAutoGauge(
        Metric.QUEUE.toString(),
        MetricLevel.IMPORTANT,
        COMPACTION_METRICS_MANAGER,
        CompactionMetricsManager::getWaitingSeqInnerCompactionTaskNum,
        Tag.NAME.toString(),
        "compaction_inner_seq",
        Tag.STATUS.toString(),
        "waiting");
    metricService.createAutoGauge(
        Metric.QUEUE.toString(),
        MetricLevel.IMPORTANT,
        COMPACTION_METRICS_MANAGER,
        CompactionMetricsManager::getWaitingUnseqInnerCompactionTaskNum,
        Tag.NAME.toString(),
        "compaction_inner_unseq",
        Tag.STATUS.toString(),
        "waiting");
    metricService.createAutoGauge(
        Metric.QUEUE.toString(),
        MetricLevel.IMPORTANT,
        COMPACTION_METRICS_MANAGER,
        CompactionMetricsManager::getRunningCrossCompactionTaskNum,
        Tag.NAME.toString(),
        "compaction_cross",
        Tag.STATUS.toString(),
        "running");
    metricService.createAutoGauge(
        Metric.QUEUE.toString(),
        MetricLevel.IMPORTANT,
        COMPACTION_METRICS_MANAGER,
        CompactionMetricsManager::getRunningSeqInnerCompactionTaskNum,
        Tag.NAME.toString(),
        "compaction_inner_seq",
        Tag.STATUS.toString(),
        "running");
    metricService.createAutoGauge(
        Metric.QUEUE.toString(),
        MetricLevel.IMPORTANT,
        COMPACTION_METRICS_MANAGER,
        CompactionMetricsManager::getRunningUnseqInnerCompactionTaskNum,
        Tag.NAME.toString(),
        "compaction_inner_unseq",
        Tag.STATUS.toString(),
        "running");
    metricService.createAutoGauge(
        Metric.COMPACTION_TASK_COUNT.toString(),
        MetricLevel.IMPORTANT,
        COMPACTION_METRICS_MANAGER,
        CompactionMetricsManager::getFinishSeqInnerCompactionTaskNum,
        Tag.NAME.toString(),
        "inner_seq");
    metricService.createAutoGauge(
        Metric.COMPACTION_TASK_COUNT.toString(),
        MetricLevel.IMPORTANT,
        COMPACTION_METRICS_MANAGER,
        CompactionMetricsManager::getFinishUnseqInnerCompactionTaskNum,
        Tag.NAME.toString(),
        "inner_unseq");
    metricService.createAutoGauge(
        Metric.COMPACTION_TASK_COUNT.toString(),
        MetricLevel.IMPORTANT,
        COMPACTION_METRICS_MANAGER,
        CompactionMetricsManager::getFinishCrossCompactionTaskNum,
        Tag.NAME.toString(),
        "cross");
    metricService.getOrCreateTimer(
        Metric.COST_TASK.toString(),
        MetricLevel.IMPORTANT,
        Tag.NAME.toString(),
        "inner_seq_compaction");
    metricService.getOrCreateTimer(
        Metric.COST_TASK.toString(),
        MetricLevel.IMPORTANT,
        Tag.NAME.toString(),
        "inner_unseq_compaction");
    metricService.getOrCreateTimer(
        Metric.COST_TASK.toString(),
        MetricLevel.IMPORTANT,
        Tag.NAME.toString(),
        "cross_compaction");
  }

  private void unbindTaskInfo(AbstractMetricService metricService) {
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
    metricService.remove(
        MetricType.TIMER, Metric.COST_TASK.toString(), Tag.NAME.toString(), "inner_seq_compaction");
    metricService.remove(
        MetricType.TIMER,
        Metric.COST_TASK.toString(),
        Tag.NAME.toString(),
        "inner_unseq_compaction");
    metricService.remove(
        MetricType.TIMER, Metric.COST_TASK.toString(), Tag.NAME.toString(), "cross_compaction");
  }

  private void bindPerformanceInfo(AbstractMetricService metricService) {
    metricService.getOrCreateCounter(
        "Compacted_Point_Num", MetricLevel.IMPORTANT, Tag.NAME.toString(), "compaction");
    metricService.getOrCreateCounter(
        "Compacted_Chunk_Num", MetricLevel.IMPORTANT, Tag.NAME.toString(), "compaction");
    metricService.getOrCreateCounter(
        "Directly_Flush_Chunk_Num", MetricLevel.NORMAL, Tag.NAME.toString(), "compaction");
    metricService.getOrCreateCounter(
        "Deserialized_Chunk_Num", MetricLevel.NORMAL, Tag.NAME.toString(), "compaction");
    metricService.getOrCreateCounter(
        "Merged_Chunk_Num", MetricLevel.NORMAL, Tag.NAME.toString(), "compaction");
  }

  private void unbindPerformanceInfo(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.COUNTER, "Compacted_Point_Num", Tag.NAME.toString(), "compaction");
    metricService.remove(
        MetricType.COUNTER, "Compacted_Chunk_Num", Tag.NAME.toString(), "compaction");
    metricService.remove(
        MetricType.COUNTER, "Directly_Flush_Chunk_Num", Tag.NAME.toString(), "compaction");
    metricService.remove(
        MetricType.COUNTER, "Deserialized_Chunk_Num", Tag.NAME.toString(), "compaction");
    metricService.remove(MetricType.COUNTER, "Merged_Chunk_Num", Tag.NAME.toString(), "compaction");
  }
}
