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

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.flush.FlushManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint.CheckpointType;
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
import java.util.List;

public class WritingMetrics implements IMetricSet {
  private static final WritingMetrics INSTANCE = new WritingMetrics();
  private static final WALManager WAL_MANAGER = WALManager.getInstance();

  private WritingMetrics() {
    // empty constructor
  }

  // region flush overview metrics
  public static final String FLUSH_STAGE_SORT = "sort";
  public static final String FLUSH_STAGE_ENCODING = "encoding";
  public static final String FLUSH_STAGE_IO = "io";
  public static final String WRITE_PLAN_INDICES = "write_plan_indices";
  public static final String PENDING_TASK_NUM = "pending_task_num";
  public static final String PENDING_SUB_TASK_NUM = "pending_sub_task_num";

  private Timer flushStageSortTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer flushStageEncodingTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer flushStageIOTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer writePlanIndicesTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private void bindFlushMetrics(AbstractMetricService metricService) {
    flushStageSortTimer =
        metricService.getOrCreateTimer(
            Metric.FLUSH_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            FLUSH_STAGE_SORT);
    flushStageEncodingTimer =
        metricService.getOrCreateTimer(
            Metric.FLUSH_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            FLUSH_STAGE_ENCODING);
    flushStageIOTimer =
        metricService.getOrCreateTimer(
            Metric.FLUSH_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            FLUSH_STAGE_IO);
    writePlanIndicesTimer =
        metricService.getOrCreateTimer(
            Metric.FLUSH_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            WRITE_PLAN_INDICES);
    metricService.createAutoGauge(
        Metric.PENDING_FLUSH_TASK.toString(),
        MetricLevel.IMPORTANT,
        FlushManager.getInstance(),
        FlushManager::getNumberOfPendingTasks,
        Tag.TYPE.toString(),
        PENDING_TASK_NUM);
    metricService.createAutoGauge(
        Metric.PENDING_FLUSH_TASK.toString(),
        MetricLevel.IMPORTANT,
        FlushManager.getInstance(),
        FlushManager::getNumberOfPendingSubTasks,
        Tag.TYPE.toString(),
        PENDING_SUB_TASK_NUM);
  }

  private void unbindFlushMetrics(AbstractMetricService metricService) {
    flushStageSortTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    flushStageEncodingTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    flushStageIOTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    writePlanIndicesTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    Arrays.asList(FLUSH_STAGE_SORT, FLUSH_STAGE_ENCODING, FLUSH_STAGE_IO, WRITE_PLAN_INDICES)
        .forEach(
            stage ->
                metricService.remove(
                    MetricType.TIMER, Metric.FLUSH_COST.toString(), Tag.STAGE.toString(), stage));
    Arrays.asList(PENDING_TASK_NUM, PENDING_SUB_TASK_NUM)
        .forEach(
            name ->
                metricService.remove(
                    MetricType.AUTO_GAUGE,
                    Metric.PENDING_FLUSH_TASK.toString(),
                    Tag.NAME.toString(),
                    name));
  }

  // endregion

  // region flush subtask metrics
  public static final String SORT_TASK = "sort_task";
  public static final String ENCODING_TASK = "encoding_task";
  public static final String IO_TASK = "io_task";

  private Timer sortTaskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer encodingTaskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer ioTaskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private void bindFlushSubTaskMetrics(AbstractMetricService metricService) {
    sortTaskTimer =
        metricService.getOrCreateTimer(
            Metric.FLUSH_SUB_TASK_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            SORT_TASK);
    encodingTaskTimer =
        metricService.getOrCreateTimer(
            Metric.FLUSH_SUB_TASK_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            ENCODING_TASK);
    ioTaskTimer =
        metricService.getOrCreateTimer(
            Metric.FLUSH_SUB_TASK_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            IO_TASK);
  }

  private void unbindFlushSubTaskMetrics(AbstractMetricService metricService) {
    sortTaskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    encodingTaskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    ioTaskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    Arrays.asList(SORT_TASK, ENCODING_TASK, IO_TASK)
        .forEach(
            type ->
                metricService.remove(
                    MetricType.TIMER,
                    Metric.FLUSH_SUB_TASK_COST.toString(),
                    Tag.TYPE.toString(),
                    type));
  }

  // endregion

  // region wal overview metrics
  public static final String WAL_NODES_NUM = "wal_nodes_num";
  public static final String USED_RATIO = "used_ratio";
  public static final String SERIALIZED_WAL_BUFFER_SIZE_BYTE = "serialized_wal_buffer_size";
  public static final String WROTE_WAL_BUFFER_SIZE_BYTE = "wrote_wal_buffer_size";
  public static final String WAL_COMPRESS_COST_NS = "wal_compress_cost";
  public static final String WAL_UNCOMPRESS_COST_NS = "wal_uncompress_cost";
  public static final String READ_WAL_BUFFER_SIZE_BYTE = "read_wal_buffer_size";
  public static final String READ_WAL_BUFFER_COST_NS = "read_wal_buffer_cost";
  public static final String WRITE_WAL_BUFFER_COST_NS = "write_wal_buffer_cost";
  public static final String ENTRIES_COUNT = "entries_count";

  private Histogram usedRatioHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram entriesCountHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram serializedWALBufferSizeHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram wroteWALBufferSizeHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram walCompressCostHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram walUncompressCostHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram readWALBufferSizeHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram readWALBufferCostHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram writeWALBufferCostHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;

  private void bindWALMetrics(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.WAL_NODE_NUM.toString(),
        MetricLevel.IMPORTANT,
        WAL_MANAGER,
        WALManager::getWALNodesNum,
        Tag.NAME.toString(),
        WAL_NODES_NUM);
    usedRatioHistogram =
        metricService.getOrCreateHistogram(
            Metric.WAL_BUFFER.toString(), MetricLevel.IMPORTANT, Tag.NAME.toString(), USED_RATIO);
    entriesCountHistogram =
        metricService.getOrCreateHistogram(
            Metric.WAL_BUFFER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            ENTRIES_COUNT);

    serializedWALBufferSizeHistogram =
        metricService.getOrCreateHistogram(
            Metric.WAL_BUFFER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            SERIALIZED_WAL_BUFFER_SIZE_BYTE);
    wroteWALBufferSizeHistogram =
        metricService.getOrCreateHistogram(
            Metric.WAL_BUFFER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            WROTE_WAL_BUFFER_SIZE_BYTE);
    walCompressCostHistogram =
        metricService.getOrCreateHistogram(
            Metric.WAL_BUFFER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            WAL_COMPRESS_COST_NS);
    walUncompressCostHistogram =
        metricService.getOrCreateHistogram(
            Metric.WAL_BUFFER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            WAL_UNCOMPRESS_COST_NS);
    readWALBufferSizeHistogram =
        metricService.getOrCreateHistogram(
            Metric.WAL_BUFFER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            READ_WAL_BUFFER_SIZE_BYTE);
    readWALBufferCostHistogram =
        metricService.getOrCreateHistogram(
            Metric.WAL_BUFFER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            READ_WAL_BUFFER_COST_NS);
    writeWALBufferCostHistogram =
        metricService.getOrCreateHistogram(
            Metric.WAL_BUFFER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            WRITE_WAL_BUFFER_COST_NS);
  }

  private void unbindWALMetrics(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.WAL_NODE_NUM.toString(), Tag.NAME.toString(), WAL_NODES_NUM);
    usedRatioHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    entriesCountHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    Arrays.asList(
            USED_RATIO,
            ENTRIES_COUNT,
            SERIALIZED_WAL_BUFFER_SIZE_BYTE,
            WROTE_WAL_BUFFER_SIZE_BYTE,
            WAL_COMPRESS_COST_NS,
            WAL_UNCOMPRESS_COST_NS,
            READ_WAL_BUFFER_SIZE_BYTE,
            READ_WAL_BUFFER_COST_NS,
            WRITE_WAL_BUFFER_COST_NS)
        .forEach(
            name ->
                metricService.remove(
                    MetricType.HISTOGRAM, Metric.WAL_BUFFER.toString(), Tag.NAME.toString(), name));
  }

  // endregion

  // region wal cost metrics
  public static final String MAKE_CHECKPOINT = "make_checkpoint";
  public static final String SERIALIZE_WAL_ENTRY = "serialize_wal_entry";
  public static final String SERIALIZE_WAL_ENTRY_TOTAL = "serialize_wal_entry_total";
  public static final String SYNC_WAL_BUFFER = "sync_wal_buffer";
  public static final String SYNC = "sync";
  public static final String FSYNC = "fsync";
  private Timer globalMemoryTableInfoTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer createMemoryTableTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer flushMemoryTableTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer serializeWalEntryTotalTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer syncTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer fsyncTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private void bindWALCostMetrics(AbstractMetricService metricService) {
    globalMemoryTableInfoTimer =
        metricService.getOrCreateTimer(
            Metric.WAL_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            MAKE_CHECKPOINT,
            Tag.TYPE.toString(),
            CheckpointType.GLOBAL_MEMORY_TABLE_INFO.toString());
    createMemoryTableTimer =
        metricService.getOrCreateTimer(
            Metric.WAL_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            MAKE_CHECKPOINT,
            Tag.TYPE.toString(),
            CheckpointType.CREATE_MEMORY_TABLE.toString());
    flushMemoryTableTimer =
        metricService.getOrCreateTimer(
            Metric.WAL_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            MAKE_CHECKPOINT,
            Tag.TYPE.toString(),
            CheckpointType.FLUSH_MEMORY_TABLE.toString());
    serializeWalEntryTotalTimer =
        metricService.getOrCreateTimer(
            Metric.WAL_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            SERIALIZE_WAL_ENTRY,
            Tag.TYPE.toString(),
            SERIALIZE_WAL_ENTRY_TOTAL);
    syncTimer =
        metricService.getOrCreateTimer(
            Metric.WAL_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            SYNC_WAL_BUFFER,
            Tag.TYPE.toString(),
            SYNC);
    fsyncTimer =
        metricService.getOrCreateTimer(
            Metric.WAL_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            SYNC_WAL_BUFFER,
            Tag.TYPE.toString(),
            FSYNC);
  }

  private void unbindWALCostMetrics(AbstractMetricService metricService) {
    globalMemoryTableInfoTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    createMemoryTableTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    flushMemoryTableTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    serializeWalEntryTotalTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    syncTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    fsyncTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
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
    Arrays.asList(SERIALIZE_WAL_ENTRY_TOTAL)
        .forEach(
            type ->
                metricService.remove(
                    MetricType.TIMER,
                    Metric.WAL_COST.toString(),
                    Tag.STAGE.toString(),
                    SERIALIZE_WAL_ENTRY,
                    Tag.TYPE.toString(),
                    type));
    Arrays.asList(SYNC, FSYNC)
        .forEach(
            type ->
                metricService.remove(
                    MetricType.TIMER,
                    Metric.WAL_COST.toString(),
                    Tag.STAGE.toString(),
                    SYNC_WAL_BUFFER,
                    Tag.TYPE.toString(),
                    type));
  }

  // endregion

  // region manage metrics
  public static final String MEM_TABLE_SIZE = "mem_table_size";
  public static final String POINTS_NUM = "total_points_num";
  public static final String SERIES_NUM = "series_num";
  public static final String AVG_SERIES_POINT_NUM = "avg_series_points_num";
  public static final String COMPRESSION_RATIO = "compression_ratio";
  public static final String EFFECTIVE_RATIO_INFO = "effective_ratio_info";
  public static final String OLDEST_MEM_TABLE_RAM_WHEN_CAUSE_SNAPSHOT =
      "oldest_mem_table_ram_when_cause_snapshot";
  public static final String OLDEST_MEM_TABLE_RAM_WHEN_CAUSE_FLUSH =
      "oldest_mem_table_ram_when_cause_flush";
  public static final String FLUSH_TSFILE_SIZE = "flush_tsfile_size";

  public static final String FLUSH_THRESHOLD = "flush_threshold";
  public static final String REJECT_THRESHOLD = "reject_threshold";

  public static final String TIMED_FLUSH_MEMTABLE_COUNT = "timed_flush_memtable_count";
  public static final String WAL_FLUSH_MEMTABLE_COUNT = "wal_flush_memtable_count";
  public static final String MANUAL_FLUSH_MEMTABLE_COUNT = "manual_flush_memtable_count";
  public static final String MEM_CONTROL_FLUSH_MEMTABLE_COUNT = "mem_control_flush_memtable_count";
  public static final String SERIES_FULL_FLUSH_MEMTABLE = "series_full_flush_memtable";

  private Gauge flushThreholdGauge = DoNothingMetricManager.DO_NOTHING_GAUGE;
  private Gauge rejectThreholdGauge = DoNothingMetricManager.DO_NOTHING_GAUGE;

  private Timer memtableLiveTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private Counter walFlushMemtableCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter timedFlushMemtableCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter seriesFullFlushMemtableCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter manualFlushMemtableCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter memControlFlushMemtableCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;

  public void bindDataRegionMetrics() {
    List<DataRegion> allDataRegions = StorageEngine.getInstance().getAllDataRegions();
    List<DataRegionId> allDataRegionIds = StorageEngine.getInstance().getAllDataRegionIds();
    allDataRegions.forEach(this::createDataRegionMemoryCostMetrics);
    allDataRegionIds.forEach(this::createFlushingMemTableStatusMetrics);
    allDataRegionIds.forEach(this::createActiveMemtableCounterMetrics);
    createActiveTimePartitionCounterMetrics();
    walFlushMemtableCounter = createWalFlushMemTableCounterMetrics();
    timedFlushMemtableCounter = createTimedFlushMemTableCounterMetrics();
    seriesFullFlushMemtableCounter = createSeriesFullFlushMemTableCounterMetrics();
    manualFlushMemtableCounter = createManualFlushMemTableCounterMetrics();
    memControlFlushMemtableCounter = createMemControlFlushMemTableCounterMetrics();

    flushThreholdGauge =
        MetricService.getInstance()
            .getOrCreateGauge(
                Metric.MEMTABLE_THRESHOLD.toString(),
                MetricLevel.IMPORTANT,
                Tag.TYPE.toString(),
                FLUSH_THRESHOLD);
    rejectThreholdGauge =
        MetricService.getInstance()
            .getOrCreateGauge(
                Metric.MEMTABLE_THRESHOLD.toString(),
                MetricLevel.IMPORTANT,
                Tag.TYPE.toString(),
                REJECT_THRESHOLD);

    memtableLiveTimer =
        MetricService.getInstance()
            .getOrCreateTimer(Metric.MEMTABLE_LIVE_DURATION.toString(), MetricLevel.IMPORTANT);
  }

  public void unbindDataRegionMetrics() {
    List<DataRegionId> allDataRegionIds = StorageEngine.getInstance().getAllDataRegionIds();
    allDataRegionIds.forEach(
        dataRegionId -> {
          removeDataRegionMemoryCostMetrics(dataRegionId);
          removeFlushingMemTableStatusMetrics(dataRegionId);
          removeActiveMemtableCounterMetrics(dataRegionId);
        });
    removeActiveTimePartitionCounterMetrics();
    removeSeriesFullFlushMemTableCounterMetrics();
    removeTimedFlushMemTableCounterMetrics();
    removeWalFlushMemTableCounterMetrics();
    removeManualFlushMemTableCounterMetrics();
    removeMemControlFlushMemTableCounterMetrics();
    MetricService.getInstance()
        .remove(
            MetricType.GAUGE,
            Metric.MEMTABLE_THRESHOLD.toString(),
            Tag.TYPE.toString(),
            FLUSH_THRESHOLD);
    MetricService.getInstance()
        .remove(
            MetricType.GAUGE,
            Metric.MEMTABLE_THRESHOLD.toString(),
            Tag.TYPE.toString(),
            REJECT_THRESHOLD);
    MetricService.getInstance().remove(MetricType.TIMER, Metric.MEMTABLE_LIVE_DURATION.toString());
  }

  public void createDataRegionMemoryCostMetrics(DataRegion dataRegion) {
    DataRegionId dataRegionId = new DataRegionId(Integer.parseInt(dataRegion.getDataRegionId()));
    MetricService.getInstance()
        .createAutoGauge(
            Metric.DATA_REGION_MEM_COST.toString(),
            MetricLevel.IMPORTANT,
            dataRegion,
            DataRegion::getMemCost,
            Tag.REGION.toString(),
            dataRegionId.toString());
  }

  public void removeDataRegionMemoryCostMetrics(DataRegionId dataRegionId) {
    MetricService.getInstance()
        .remove(
            MetricType.AUTO_GAUGE,
            Metric.DATA_REGION_MEM_COST.toString(),
            Tag.REGION.toString(),
            dataRegionId.toString());
  }

  public void createWALNodeInfoMetrics(String walNodeId) {
    Arrays.asList(
            EFFECTIVE_RATIO_INFO,
            OLDEST_MEM_TABLE_RAM_WHEN_CAUSE_SNAPSHOT,
            OLDEST_MEM_TABLE_RAM_WHEN_CAUSE_FLUSH)
        .forEach(
            name ->
                MetricService.getInstance()
                    .getOrCreateHistogram(
                        Metric.WAL_NODE_INFO.toString(),
                        MetricLevel.IMPORTANT,
                        Tag.NAME.toString(),
                        name,
                        Tag.TYPE.toString(),
                        walNodeId));
  }

  public void removeWALNodeInfoMetrics(String walNodeId) {
    Arrays.asList(
            EFFECTIVE_RATIO_INFO,
            OLDEST_MEM_TABLE_RAM_WHEN_CAUSE_SNAPSHOT,
            OLDEST_MEM_TABLE_RAM_WHEN_CAUSE_FLUSH)
        .forEach(
            name ->
                MetricService.getInstance()
                    .remove(
                        MetricType.HISTOGRAM,
                        Metric.WAL_NODE_INFO.toString(),
                        Tag.NAME.toString(),
                        name,
                        Tag.TYPE.toString(),
                        walNodeId));
  }

  public void createFlushingMemTableStatusMetrics(DataRegionId dataRegionId) {
    Arrays.asList(
            MEM_TABLE_SIZE,
            SERIES_NUM,
            POINTS_NUM,
            AVG_SERIES_POINT_NUM,
            COMPRESSION_RATIO,
            FLUSH_TSFILE_SIZE)
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

  public Counter createWalFlushMemTableCounterMetrics() {
    return MetricService.getInstance()
        .getOrCreateCounter(
            Metric.FLUSH_MEMTABLE_COUNT.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            WAL_FLUSH_MEMTABLE_COUNT);
  }

  public Counter createTimedFlushMemTableCounterMetrics() {
    return MetricService.getInstance()
        .getOrCreateCounter(
            Metric.FLUSH_MEMTABLE_COUNT.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            TIMED_FLUSH_MEMTABLE_COUNT);
  }

  public Counter createSeriesFullFlushMemTableCounterMetrics() {
    return MetricService.getInstance()
        .getOrCreateCounter(
            Metric.FLUSH_MEMTABLE_COUNT.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            SERIES_FULL_FLUSH_MEMTABLE);
  }

  public Counter createManualFlushMemTableCounterMetrics() {
    return MetricService.getInstance()
        .getOrCreateCounter(
            Metric.FLUSH_MEMTABLE_COUNT.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            MANUAL_FLUSH_MEMTABLE_COUNT);
  }

  public Counter createMemControlFlushMemTableCounterMetrics() {
    return MetricService.getInstance()
        .getOrCreateCounter(
            Metric.FLUSH_MEMTABLE_COUNT.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            MEM_CONTROL_FLUSH_MEMTABLE_COUNT);
  }

  public void createActiveMemtableCounterMetrics(DataRegionId dataRegionId) {
    MetricService.getInstance()
        .getOrCreateCounter(
            Metric.ACTIVE_MEMTABLE_COUNT.toString(),
            MetricLevel.IMPORTANT,
            Tag.REGION.toString(),
            dataRegionId.toString());
  }

  public void createActiveTimePartitionCounterMetrics() {
    MetricService.getInstance()
        .getOrCreateCounter(Metric.ACTIVE_TIME_PARTITION_COUNT.toString(), MetricLevel.IMPORTANT);
  }

  public void removeSeriesFullFlushMemTableCounterMetrics() {
    MetricService.getInstance()
        .remove(
            MetricType.COUNTER,
            Metric.FLUSH_MEMTABLE_COUNT.toString(),
            Tag.TYPE.toString(),
            SERIES_FULL_FLUSH_MEMTABLE);
  }

  public void removeTimedFlushMemTableCounterMetrics() {
    MetricService.getInstance()
        .remove(
            MetricType.COUNTER,
            Metric.FLUSH_MEMTABLE_COUNT.toString(),
            Tag.TYPE.toString(),
            TIMED_FLUSH_MEMTABLE_COUNT);
  }

  public void removeWalFlushMemTableCounterMetrics() {
    MetricService.getInstance()
        .remove(
            MetricType.COUNTER,
            Metric.FLUSH_MEMTABLE_COUNT.toString(),
            Tag.TYPE.toString(),
            WAL_FLUSH_MEMTABLE_COUNT);
  }

  public void removeManualFlushMemTableCounterMetrics() {
    MetricService.getInstance()
        .remove(
            MetricType.COUNTER,
            Metric.FLUSH_MEMTABLE_COUNT.toString(),
            Tag.TYPE.toString(),
            MANUAL_FLUSH_MEMTABLE_COUNT);
  }

  public void removeMemControlFlushMemTableCounterMetrics() {
    MetricService.getInstance()
        .remove(
            MetricType.COUNTER,
            Metric.FLUSH_MEMTABLE_COUNT.toString(),
            Tag.TYPE.toString(),
            MEM_CONTROL_FLUSH_MEMTABLE_COUNT);
  }

  public void removeActiveMemtableCounterMetrics(DataRegionId dataRegionId) {
    MetricService.getInstance()
        .remove(
            MetricType.COUNTER,
            Metric.ACTIVE_MEMTABLE_COUNT.toString(),
            Tag.REGION.toString(),
            dataRegionId.toString());
  }

  public void removeActiveTimePartitionCounterMetrics() {
    MetricService.getInstance()
        .remove(MetricType.COUNTER, Metric.ACTIVE_TIME_PARTITION_COUNT.toString());
  }

  public void removeFlushingMemTableStatusMetrics(DataRegionId dataRegionId) {
    Arrays.asList(
            MEM_TABLE_SIZE,
            SERIES_NUM,
            POINTS_NUM,
            AVG_SERIES_POINT_NUM,
            COMPRESSION_RATIO,
            FLUSH_TSFILE_SIZE)
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

  public void recordWALNodeEffectiveInfoRatio(String walNodeId, double ratio) {
    MetricService.getInstance()
        .histogram(
            (long) (ratio * 100),
            Metric.WAL_NODE_INFO.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            EFFECTIVE_RATIO_INFO,
            Tag.TYPE.toString(),
            walNodeId);
  }

  public void recordMemTableRamWhenCauseSnapshot(String walNodeId, long ram) {
    MetricService.getInstance()
        .histogram(
            ram,
            Metric.WAL_NODE_INFO.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            OLDEST_MEM_TABLE_RAM_WHEN_CAUSE_SNAPSHOT,
            Tag.TYPE.toString(),
            walNodeId);
  }

  public void recordMemTableRamWhenCauseFlush(String walNodeId, long ram) {
    MetricService.getInstance()
        .histogram(
            ram,
            Metric.WAL_NODE_INFO.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            OLDEST_MEM_TABLE_RAM_WHEN_CAUSE_FLUSH,
            Tag.TYPE.toString(),
            walNodeId);
  }

  public void recordTsFileCompressionRatioOfFlushingMemTable(
      String dataRegionId, double compressionRatio) {
    MetricService.getInstance()
        .histogram(
            (long) (compressionRatio * 100),
            Metric.FLUSHING_MEM_TABLE_STATUS.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            COMPRESSION_RATIO,
            Tag.REGION.toString(),
            new DataRegionId(Integer.parseInt(dataRegionId)).toString());
  }

  public void recordFlushingMemTableStatus(
      String storageGroup, long memSize, long seriesNum, long totalPointsNum, long avgSeriesNum) {
    DataRegionId dataRegionId = getDataRegionIdFromStorageGroupStr(storageGroup);
    if (dataRegionId == null) {
      return;
    }

    MetricService.getInstance()
        .histogram(
            memSize,
            Metric.FLUSHING_MEM_TABLE_STATUS.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            MEM_TABLE_SIZE,
            Tag.REGION.toString(),
            dataRegionId.toString());
    MetricService.getInstance()
        .histogram(
            seriesNum,
            Metric.FLUSHING_MEM_TABLE_STATUS.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            SERIES_NUM,
            Tag.REGION.toString(),
            dataRegionId.toString());
    MetricService.getInstance()
        .histogram(
            totalPointsNum,
            Metric.FLUSHING_MEM_TABLE_STATUS.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            POINTS_NUM,
            Tag.REGION.toString(),
            dataRegionId.toString());
    MetricService.getInstance()
        .histogram(
            avgSeriesNum,
            Metric.FLUSHING_MEM_TABLE_STATUS.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            AVG_SERIES_POINT_NUM,
            Tag.REGION.toString(),
            dataRegionId.toString());
  }

  public void recordFlushTsFileSize(String storageGroup, long size) {
    DataRegionId dataRegionId = getDataRegionIdFromStorageGroupStr(storageGroup);
    if (dataRegionId == null) {
      return;
    }
    MetricService.getInstance()
        .histogram(
            size,
            Metric.FLUSHING_MEM_TABLE_STATUS.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            FLUSH_TSFILE_SIZE,
            Tag.REGION.toString(),
            dataRegionId.toString());
  }

  private DataRegionId getDataRegionIdFromStorageGroupStr(String storageGroup) {
    int idx = storageGroup.lastIndexOf('-');
    if (idx == -1) {
      return null;
    }
    String dataRegionIdStr = storageGroup.substring(idx + 1);
    return new DataRegionId(Integer.parseInt(dataRegionIdStr));
  }

  public void recordFlushCost(String stage, long costTimeInMillis) {
    switch (stage) {
      case FLUSH_STAGE_SORT:
        flushStageSortTimer.updateMillis(costTimeInMillis);
        break;
      case FLUSH_STAGE_ENCODING:
        flushStageEncodingTimer.updateMillis(costTimeInMillis);
        break;
      case FLUSH_STAGE_IO:
        flushStageIOTimer.updateMillis(costTimeInMillis);
        break;
      case WRITE_PLAN_INDICES:
        writePlanIndicesTimer.updateMillis(costTimeInMillis);
        break;
      default:
        // do nothing
        break;
    }
  }

  public void recordFlushSubTaskCost(String subTaskType, long costTimeInMillis) {
    switch (subTaskType) {
      case SORT_TASK:
        sortTaskTimer.updateMillis(costTimeInMillis);
        break;
      case ENCODING_TASK:
        encodingTaskTimer.updateMillis(costTimeInMillis);
        break;
      case IO_TASK:
        ioTaskTimer.updateMillis(costTimeInMillis);
        break;
      default:
        // do nothing
        break;
    }
  }

  public void recordMakeCheckpointCost(CheckpointType type, long costTimeInNanos) {
    switch (type) {
      case GLOBAL_MEMORY_TABLE_INFO:
        globalMemoryTableInfoTimer.updateNanos(costTimeInNanos);
        break;
      case CREATE_MEMORY_TABLE:
        createMemoryTableTimer.updateNanos(costTimeInNanos);
        break;
      case FLUSH_MEMORY_TABLE:
        flushMemoryTableTimer.updateNanos(costTimeInNanos);
        break;
      default:
        // do nothing
        break;
    }
  }

  public void recordSerializeWALEntryTotalCost(long costTimeInNanos) {
    serializeWalEntryTotalTimer.updateNanos(costTimeInNanos);
  }

  public void recordCompressWALBufferCost(long costTimeInNanos) {
    walCompressCostHistogram.update(costTimeInNanos);
  }

  public void recordWroteWALBuffer(int serializedSize, int wroteSize, long wroteTimeCostInNanos) {
    serializedWALBufferSizeHistogram.update(serializedSize);
    wroteWALBufferSizeHistogram.update(wroteSize);
    writeWALBufferCostHistogram.update(wroteTimeCostInNanos);
  }

  public void recordWALUncompressCost(long costTimeInNanos) {
    walUncompressCostHistogram.update(costTimeInNanos);
  }

  public void recordWALRead(long size, long costTimeInNanos) {
    readWALBufferSizeHistogram.update(size);
    readWALBufferCostHistogram.update(costTimeInNanos);
  }

  public void recordSyncWALBufferCost(long costTimeInNanos, boolean forceFlag) {
    if (forceFlag) {
      // fsync mode
      fsyncTimer.updateNanos(costTimeInNanos);
    } else {
      // sync mode
      syncTimer.updateNanos(costTimeInNanos);
    }
  }

  public void recordWALBufferUsedRatio(double usedRatio) {
    usedRatioHistogram.update((long) (usedRatio * 100));
  }

  public void recordWALBufferEntriesCount(long count) {
    entriesCountHistogram.update(count);
  }

  public void recordFlushThreshold(double flushThreshold) {
    flushThreholdGauge.set((long) flushThreshold);
  }

  public void recordRejectThreshold(double rejectThreshold) {
    rejectThreholdGauge.set((long) rejectThreshold);
  }

  public void recordMemTableLiveDuration(long durationMillis) {
    memtableLiveTimer.updateMillis(durationMillis);
  }

  public void recordTimedFlushMemTableCount(int number) {
    timedFlushMemtableCounter.inc(number);
  }

  public void recordWalFlushMemTableCount(int number) {
    walFlushMemtableCounter.inc(number);
  }

  public void recordSeriesFullFlushMemTableCount(int number) {
    seriesFullFlushMemtableCounter.inc(number);
  }

  public void recordManualFlushMemTableCount(int number) {
    manualFlushMemtableCounter.inc(number);
  }

  public void recordMemControlFlushMemTableCount(int number) {
    memControlFlushMemtableCounter.inc(number);
  }

  public void recordActiveMemTableCount(String dataRegionId, int number) {
    MetricService.getInstance()
        .count(
            number,
            Metric.ACTIVE_MEMTABLE_COUNT.toString(),
            MetricLevel.IMPORTANT,
            Tag.REGION.toString(),
            dataRegionId);
  }

  public void recordActiveTimePartitionCount(int number) {
    MetricService.getInstance()
        .count(number, Metric.ACTIVE_TIME_PARTITION_COUNT.toString(), MetricLevel.IMPORTANT);
  }

  // endregion

  @Override
  public void bindTo(AbstractMetricService metricService) {
    bindFlushMetrics(metricService);
    bindFlushSubTaskMetrics(metricService);
    bindWALMetrics(metricService);
    bindWALCostMetrics(metricService);
    bindDataRegionMetrics();
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    unbindFlushMetrics(metricService);
    unbindFlushSubTaskMetrics(metricService);
    unbindWALMetrics(metricService);
    unbindWALCostMetrics(metricService);
    unbindDataRegionMetrics();
  }

  public static WritingMetrics getInstance() {
    return INSTANCE;
  }
}
