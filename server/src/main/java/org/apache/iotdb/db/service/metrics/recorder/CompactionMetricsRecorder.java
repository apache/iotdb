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

import org.apache.iotdb.db.engine.compaction.constant.CompactionTaskStatus;
import org.apache.iotdb.db.engine.compaction.constant.CompactionType;
import org.apache.iotdb.db.engine.compaction.constant.ProcessChunkType;
import org.apache.iotdb.db.engine.compaction.cross.CrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.inner.InnerSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.service.metrics.MetricsService;
import org.apache.iotdb.db.service.metrics.enums.Metric;
import org.apache.iotdb.db.service.metrics.enums.Tag;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.MetricLevel;

import java.util.concurrent.TimeUnit;

public class CompactionMetricsRecorder {

  private static final boolean ENABLE_METRIC =
      MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric();

  public static void recordWriteInfo(
      CompactionType compactionType,
      ProcessChunkType processChunkType,
      boolean aligned,
      long byteNum) {
    if (!ENABLE_METRIC) {
      return;
    }
    MetricsService.getInstance()
        .getMetricManager()
        .count(
            byteNum / 1024L,
            Metric.DATA_WRITTEN.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "compaction",
            Tag.NAME.toString(),
            compactionType.toString(),
            Tag.TYPE.toString(),
            aligned ? "ALIGNED" : "NOT_ALIGNED",
            Tag.TYPE.toString(),
            processChunkType.toString());
    MetricsService.getInstance()
        .getMetricManager()
        .count(
            byteNum / 1024L,
            Metric.DATA_WRITTEN.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "compaction",
            Tag.TYPE.toString(),
            "total");
  }

  public static void recordReadInfo(long byteNum) {
    if (!ENABLE_METRIC) {
      return;
    }
    MetricsService.getInstance()
        .getMetricManager()
        .count(
            byteNum,
            Metric.DATA_READ.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "compaction");
  }

  public static void recordTaskInfo(
      AbstractCompactionTask task, CompactionTaskStatus status, int size) {
    if (!ENABLE_METRIC) {
      return;
    }
    String taskType = "unknown";
    boolean isInnerTask = false;
    if (task instanceof InnerSpaceCompactionTask) {
      isInnerTask = true;
      taskType = "inner";
    } else if (task instanceof CrossSpaceCompactionTask) {
      taskType = "cross";
    }

    switch (status) {
      case ADD_TO_QUEUE:
      case POLL_FROM_QUEUE:
        MetricsService.getInstance()
            .getMetricManager()
            .getOrCreateGauge(
                Metric.QUEUE.toString(),
                MetricLevel.IMPORTANT,
                Tag.NAME.toString(),
                "compaction_" + taskType,
                Tag.STATUS.toString(),
                "waiting")
            .set(size);
        break;
      case READY_TO_EXECUTE:
        MetricsService.getInstance()
            .getMetricManager()
            .getOrCreateGauge(
                Metric.QUEUE.toString(),
                MetricLevel.IMPORTANT,
                Tag.NAME.toString(),
                "compaction_" + taskType,
                Tag.STATUS.toString(),
                "running")
            .set(size);
        break;
      case FINISHED:
        MetricsService.getInstance()
            .getMetricManager()
            .getOrCreateGauge(
                Metric.QUEUE.toString(),
                MetricLevel.IMPORTANT,
                Tag.NAME.toString(),
                "compaction_" + taskType,
                Tag.STATUS.toString(),
                "running")
            .set(size);
        MetricsService.getInstance()
            .getMetricManager()
            .timer(
                task.getTimeCost(),
                TimeUnit.MILLISECONDS,
                Metric.COST_TASK.toString(),
                MetricLevel.IMPORTANT,
                Tag.NAME.toString(),
                isInnerTask ? "inner_compaction" : "cross_compaction");
        if (isInnerTask) {
          MetricsService.getInstance()
              .getMetricManager()
              .count(
                  1,
                  Metric.COMPACTION_TASK_COUNT.toString(),
                  MetricLevel.IMPORTANT,
                  Tag.NAME.toString(),
                  "inner_compaction",
                  Tag.TYPE.toString(),
                  ((InnerSpaceCompactionTask) task).isSequence() ? "sequence" : "unsequence");
        } else {
          MetricsService.getInstance()
              .getMetricManager()
              .count(
                  1,
                  Metric.COMPACTION_TASK_COUNT.toString(),
                  MetricLevel.IMPORTANT,
                  Tag.NAME.toString(),
                  "cross_compaction",
                  Tag.TYPE.toString(),
                  "cross");
        }
        break;
      default:
        // do nothing
        break;
    }
  }
}
