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
import org.apache.iotdb.db.wal.WALManager;
import org.apache.iotdb.db.wal.checkpoint.CheckpointType;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Arrays;

public class WritingMetrics implements IMetricSet {
  private static final WALManager WAL_MANAGER = WALManager.getInstance();
  public static final String WAL_NODES_NUM = "wal_nodes_num";
  public static final String MAKE_CHECKPOINT = "make_checkpoint";
  public static final String SERIALIZE_ONE_INFO_ENTRY = "serialize_one_info_entry";
  public static final String TYPE_WAL_INFO_ENTRY = "wal_info_entry";
  public static final String TYPE_NULL = "null";
  public static final String SYNC_WAL_BUFFER = "sync_wal_buffer";
  public static final String FLUSH_STAGE_SORT = "sort";
  public static final String FLUSH_STAGE_ENCODING = "encoding";
  public static final String FLUSH_STAGE_IO = "io";
  public static final String WRITE_PLAN_INDICES = "write_plan_indices";
  public static final String SORT_TASK = "sort_task";
  public static final String ENCODING_TASK = "encoding_task";
  public static final String IO_TASK = "io_task";
  public static final String MEM_TABLE_SIZE = "mem_table_size";
  public static final String POINTS_NUM = "total_points_num";
  public static final String SERIES_NUM = "series_num";
  public static final String AVG_SERIES_POINT_NUM = "avg_series_points_num";

  @Override
  public void bindTo(AbstractMetricService metricService) {
    bindFlushMetrics(metricService);
    bindFlushSubTaskMetrics(metricService);
    bindWALMetrics(metricService);
    bindWALCostMetrics(metricService);
  }

  private void bindFlushMetrics(AbstractMetricService metricService) {
    Arrays.asList(FLUSH_STAGE_SORT, FLUSH_STAGE_ENCODING, FLUSH_STAGE_IO, WRITE_PLAN_INDICES)
        .forEach(
            stage ->
                metricService.getOrCreateTimer(
                    Metric.FLUSH_COST.toString(),
                    MetricLevel.IMPORTANT,
                    Tag.STAGE.toString(),
                    stage));
  }

  private void bindFlushSubTaskMetrics(AbstractMetricService metricService) {
    Arrays.asList(SORT_TASK, ENCODING_TASK, IO_TASK)
        .forEach(
            type ->
                metricService.getOrCreateTimer(
                    Metric.FLUSH_SUB_TASK_COST.toString(),
                    MetricLevel.IMPORTANT,
                    Tag.TYPE.toString(),
                    type));
  }

  private void bindWALMetrics(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.WAL.toString(),
        MetricLevel.IMPORTANT,
        WAL_MANAGER,
        WALManager::getWALNodesNum,
        Tag.NAME.toString(),
        WAL_NODES_NUM);
  }

  private void bindWALCostMetrics(AbstractMetricService metricService) {
    Arrays.asList(
            CheckpointType.GLOBAL_MEMORY_TABLE_INFO.toString(),
            CheckpointType.CREATE_MEMORY_TABLE.toString(),
            CheckpointType.FLUSH_MEMORY_TABLE.toString())
        .forEach(
            type ->
                metricService.getOrCreateTimer(
                    Metric.WAL_COST.toString(),
                    MetricLevel.IMPORTANT,
                    Tag.STAGE.toString(),
                    MAKE_CHECKPOINT,
                    Tag.TYPE.toString(),
                    type));
    metricService.getOrCreateTimer(
        Metric.WAL_COST.toString(),
        MetricLevel.IMPORTANT,
        Tag.STAGE.toString(),
        SERIALIZE_ONE_INFO_ENTRY,
        Tag.TYPE.toString(),
        TYPE_WAL_INFO_ENTRY);
    metricService.getOrCreateTimer(
        Metric.WAL_COST.toString(),
        MetricLevel.IMPORTANT,
        Tag.STAGE.toString(),
        SYNC_WAL_BUFFER,
        Tag.TYPE.toString(),
        TYPE_NULL);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    unbindFlushMetrics(metricService);
    unbindFlushSubTaskMetrics(metricService);
    unbindWALMetrics(metricService);
    unbindWALCostMetrics(metricService);
  }

  private void unbindFlushMetrics(AbstractMetricService metricService) {
    Arrays.asList(FLUSH_STAGE_SORT, FLUSH_STAGE_ENCODING, FLUSH_STAGE_IO, WRITE_PLAN_INDICES)
        .forEach(
            stage ->
                metricService.remove(
                    MetricType.TIMER, Metric.FLUSH_COST.toString(), Tag.STAGE.toString(), stage));
  }

  private void unbindFlushSubTaskMetrics(AbstractMetricService metricService) {
    Arrays.asList(SORT_TASK, ENCODING_TASK, IO_TASK)
        .forEach(
            type ->
                metricService.remove(
                    MetricType.TIMER,
                    Metric.FLUSH_SUB_TASK_COST.toString(),
                    Tag.TYPE.toString(),
                    type));
  }

  private void unbindWALMetrics(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.WAL.toString(), Tag.NAME.toString(), WAL_NODES_NUM);
  }

  private void unbindWALCostMetrics(AbstractMetricService metricService) {
    Arrays.asList(
            CheckpointType.GLOBAL_MEMORY_TABLE_INFO.toString(),
            CheckpointType.CREATE_MEMORY_TABLE.toString(),
            CheckpointType.FLUSH_MEMORY_TABLE.toString())
        .forEach(
            type ->
                metricService.remove(
                    MetricType.TIMER,
                    Metric.WAL_COST.toString(),
                    Tag.STAGE.toString(),
                    MAKE_CHECKPOINT,
                    Tag.TYPE.toString(),
                    type));
    metricService.remove(
        MetricType.TIMER,
        Metric.WAL_COST.toString(),
        Tag.STAGE.toString(),
        SERIALIZE_ONE_INFO_ENTRY,
        Tag.TYPE.toString(),
        TYPE_WAL_INFO_ENTRY);
    metricService.remove(
        MetricType.TIMER,
        Metric.WAL_COST.toString(),
        Tag.STAGE.toString(),
        SYNC_WAL_BUFFER,
        Tag.TYPE.toString(),
        TYPE_NULL);
  }
}
