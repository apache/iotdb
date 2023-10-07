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
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.MetricConstant;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CompactionFileMetrics implements IMetricSet {
  // compaction temporal files
  private final AtomicLong innerSeqCompactionTempFileSize = new AtomicLong(0);
  private final AtomicLong innerUnseqCompactionTempFileSize = new AtomicLong(0);
  private final AtomicLong crossCompactionTempFileSize = new AtomicLong(0);
  private final AtomicInteger innerSeqCompactionTempFileNum = new AtomicInteger(0);
  private final AtomicInteger innerUnseqCompactionTempFileNum = new AtomicInteger(0);
  private final AtomicInteger crossCompactionTempFileNum = new AtomicInteger(0);

  private long lastUpdateTime = 0;

  @Override
  public void bindTo(AbstractMetricService metricService) {
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
        CompactionFileMetrics::getCrossCompactionTempFileSize,
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
        CompactionFileMetrics::getCrossCompactionTempFileNum,
        Tag.NAME.toString(),
        "cross-temp");
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
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
}
