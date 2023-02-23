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

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.service.metrics.WritingMetrics;
import org.apache.iotdb.db.wal.checkpoint.CheckpointType;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class WritingMetricsManager {
  public static final WritingMetricsManager INSTANCE = new WritingMetricsManager();

  private WritingMetricsManager() {}

  public void createFlushingMemTableStatusMetrics(DataRegionId dataRegionId) {
    Arrays.asList(
            WritingMetrics.MEM_TABLE_SIZE,
            WritingMetrics.SERIES_NUM,
            WritingMetrics.POINTS_NUM,
            WritingMetrics.AVG_SERIES_POINT_NUM)
        .forEach(
            name ->
                MetricService.getInstance()
                    .getOrCreateHistogram(
                        Metric.FLUSHING_MEM_TABLE_STATUS.toString(),
                        MetricLevel.IMPORTANT,
                        Tag.NAME.toString(),
                        name,
                        Tag.REGION.toString(),
                        dataRegionId.toString()));
  }

  public void removeFlushingMemTableStatusMetrics(DataRegionId dataRegionId) {
    Arrays.asList(
            WritingMetrics.MEM_TABLE_SIZE,
            WritingMetrics.SERIES_NUM,
            WritingMetrics.POINTS_NUM,
            WritingMetrics.AVG_SERIES_POINT_NUM)
        .forEach(
            name ->
                MetricService.getInstance()
                    .remove(
                        MetricType.HISTOGRAM,
                        Metric.FLUSHING_MEM_TABLE_STATUS.toString(),
                        Tag.NAME.toString(),
                        name,
                        Tag.REGION.toString(),
                        dataRegionId.toString()));
  }

  public void recordFlushingMemTableStatus(
      String storageGroup, long memSize, long seriesNum, long totalPointsNum, long avgSeriesNum) {
    int idx = storageGroup.lastIndexOf('-');
    if (idx == -1) {
      return;
    }
    String dataRegionIdStr = storageGroup.substring(idx + 1);
    DataRegionId dataRegionId = new DataRegionId(Integer.parseInt(dataRegionIdStr));

    MetricService.getInstance()
        .histogram(
            memSize,
            Metric.FLUSHING_MEM_TABLE_STATUS.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            WritingMetrics.MEM_TABLE_SIZE,
            Tag.REGION.toString(),
            dataRegionId.toString());
    MetricService.getInstance()
        .histogram(
            seriesNum,
            Metric.FLUSHING_MEM_TABLE_STATUS.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            WritingMetrics.SERIES_NUM,
            Tag.REGION.toString(),
            dataRegionId.toString());
    MetricService.getInstance()
        .histogram(
            totalPointsNum,
            Metric.FLUSHING_MEM_TABLE_STATUS.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            WritingMetrics.POINTS_NUM,
            Tag.REGION.toString(),
            dataRegionId.toString());
    MetricService.getInstance()
        .histogram(
            avgSeriesNum,
            Metric.FLUSHING_MEM_TABLE_STATUS.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            WritingMetrics.AVG_SERIES_POINT_NUM,
            Tag.REGION.toString(),
            dataRegionId.toString());
  }

  public void recordFlushCost(String stage, long costTimeInMillis) {
    MetricService.getInstance()
        .timer(
            costTimeInMillis,
            TimeUnit.MILLISECONDS,
            Metric.FLUSH_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            stage);
  }

  public void recordFlushSubTaskCost(String subTaskType, long costTimeInMillis) {
    MetricService.getInstance()
        .timer(
            costTimeInMillis,
            TimeUnit.MILLISECONDS,
            Metric.FLUSH_SUB_TASK_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            subTaskType);
  }

  public void recordMakeCheckpointCost(CheckpointType type, long costTimeInNanos) {
    MetricService.getInstance()
        .timer(
            costTimeInNanos,
            TimeUnit.NANOSECONDS,
            Metric.WAL_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            WritingMetrics.MAKE_CHECKPOINT,
            Tag.TYPE.toString(),
            type.toString());
  }

  public void recordSerializeWALInfoEntryCost(long costTimeInNanos) {
    MetricService.getInstance()
        .timer(
            costTimeInNanos,
            TimeUnit.NANOSECONDS,
            Metric.WAL_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            WritingMetrics.SERIALIZE_ONE_INFO_ENTRY,
            Tag.TYPE.toString(),
            WritingMetrics.TYPE_WAL_INFO_ENTRY);
  }

  public void recordSyncWALBufferCost(long costTimeInNanos) {
    MetricService.getInstance()
        .timer(
            costTimeInNanos,
            TimeUnit.NANOSECONDS,
            Metric.WAL_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            WritingMetrics.SYNC_WAL_BUFFER,
            Tag.TYPE.toString(),
            WritingMetrics.TYPE_NULL);
  }

  public static WritingMetricsManager getInstance() {
    return INSTANCE;
  }
}
