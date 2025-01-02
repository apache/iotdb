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

package org.apache.iotdb.db.service.metrics.memory;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Arrays;
import java.util.Collections;

public class ThresholdMemoryMetrics implements IMetricSet {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final SystemInfo systemInfo = SystemInfo.getInstance();

  private Gauge totalMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;
  private Gauge storageEngineMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;
  private Gauge writeMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;
  private Gauge memtableMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;
  private Gauge dataNodeDevicePathCacheMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;
  private Gauge bufferedArraysMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;
  private Gauge timePartitionInfoMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;
  private Gauge compactionMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;
  private Gauge queryEngineMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;
  private Gauge schemaEngineMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;
  private Gauge consensusMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;
  private Gauge streamEngineMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;
  private Gauge directBufferMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;

  private static final String TOTAL = "Total";
  private static final String ON_HEAP = "OnHeap";
  private static final String OFF_HEAP = "OffHeap";
  private static final String STORAGE_ENGINE = "StorageEngine";
  private static final String STORAGE_ENGINE_WRITE = "StorageEngine-Write";
  private static final String STORAGE_ENGINE_WRITE_MEMTABLE = "StorageEngine-Write-Memtable";
  private static final String STORAGE_ENGINE_WRITE_MEMTABLE_CACHE =
      "StorageEngine-Write-Memtable-DevicePathCache";
  private static final String STORAGE_ENGINE_WRITE_MEMTABLE_BUFFERED_ARRAYS =
      "StorageEngine-Write-Memtable-BufferedArrays";
  private static final String STORAGE_ENGINE_WRITE_TIME_PARTITION_INFO =
      "StorageEngine-Write-TimePartitionInfo";
  private static final String STORAGE_ENGINE_COMPACTION = "StorageEngine-Compaction";
  private static final String QUERY_ENGINE = "QueryEngine";
  private static final String SCHEMA_ENGINE = "SchemaEngine";
  private static final String CONSENSUS = "Consensus";
  private static final String STREAM_ENGINE = "StreamEngine";
  private static final String DIRECT_BUFFER = "DirectBuffer";
  private static final String[] LEVELS = {"0", "1", "2", "3", "4"};

  @Override
  public void bindTo(AbstractMetricService metricService) {
    totalMemorySize =
        metricService.getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            TOTAL,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[0]);
    totalMemorySize.set(Runtime.getRuntime().maxMemory());
    bindStorageEngineRelatedMemoryMetrics(metricService);
    queryEngineMemorySize =
        metricService.getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            QUERY_ENGINE,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[1]);
    queryEngineMemorySize.set(config.getAllocateMemoryForRead());
    schemaEngineMemorySize =
        metricService.getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            SCHEMA_ENGINE,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[1]);
    schemaEngineMemorySize.set(config.getAllocateMemoryForSchema());
    consensusMemorySize =
        metricService.getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            CONSENSUS,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[1]);
    consensusMemorySize.set(config.getAllocateMemoryForConsensus());
    streamEngineMemorySize =
        metricService.getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            STREAM_ENGINE,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[1]);
    streamEngineMemorySize.set(config.getAllocateMemoryForPipe());
    directBufferMemorySize =
        metricService.getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            DIRECT_BUFFER,
            Tag.TYPE.toString(),
            OFF_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[1]);
    directBufferMemorySize.set(systemInfo.getTotalDirectBufferMemorySizeLimit());
  }

  private void bindStorageEngineRelatedMemoryMetrics(AbstractMetricService metricService) {
    long storageEngineSize = config.getAllocateMemoryForStorageEngine();
    // Total memory size of storage engine
    storageEngineMemorySize =
        metricService.getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            STORAGE_ENGINE,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[1]);
    storageEngineMemorySize.set(storageEngineSize);
    // The memory of storage engine divided into Write and Compaction
    long writeSize =
        (long)
            (config.getAllocateMemoryForStorageEngine() * (1 - config.getCompactionProportion()));
    long compactionSize = storageEngineSize - writeSize;
    long bufferedArraySize =
        (long)
            (config.getAllocateMemoryForStorageEngine()
                * config.getBufferedArraysMemoryProportion());
    writeMemorySize =
        metricService.getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            STORAGE_ENGINE_WRITE,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[2]);
    writeMemorySize.set(writeSize);
    compactionMemorySize =
        metricService.getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            STORAGE_ENGINE_COMPACTION,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[2]);
    compactionMemorySize.set(compactionSize);
    // The write memory of storage engine divided into MemTable and TimePartitionInfo
    long memtableSize =
        (long)
            (config.getAllocateMemoryForStorageEngine() * config.getWriteProportionForMemtable());
    long timePartitionInfoSize = config.getAllocateMemoryForTimePartitionInfo();
    memtableMemorySize =
        metricService.getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            STORAGE_ENGINE_WRITE_MEMTABLE,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[3]);
    memtableMemorySize.set(memtableSize);
    // The memtable memory of storage engine contain DataNodeDevicePathCache (NOTICE: This part of
    // memory is not divided)
    long dataNodeDevicePathCacheSize =
        (long)
            (config.getAllocateMemoryForStorageEngine()
                * config.getWriteProportionForMemtable()
                * config.getDevicePathCacheProportion());
    dataNodeDevicePathCacheMemorySize =
        metricService.getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            STORAGE_ENGINE_WRITE_MEMTABLE_CACHE,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[4]);
    dataNodeDevicePathCacheMemorySize.set(dataNodeDevicePathCacheSize);
    bufferedArraysMemorySize =
        metricService.getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            STORAGE_ENGINE_WRITE_MEMTABLE_BUFFERED_ARRAYS,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[4]);
    bufferedArraysMemorySize.set(bufferedArraySize);
    timePartitionInfoMemorySize =
        metricService.getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            STORAGE_ENGINE_WRITE_TIME_PARTITION_INFO,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[3]);
    timePartitionInfoMemorySize.set(timePartitionInfoSize);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    totalMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;
    metricService.remove(
        MetricType.GAUGE,
        Metric.THRESHOLD_MEMORY_SIZE.toString(),
        Tag.NAME.toString(),
        TOTAL,
        Tag.TYPE.toString(),
        ON_HEAP,
        Tag.LEVEL.toString(),
        "0");
    storageEngineMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;
    queryEngineMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;
    schemaEngineMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;
    consensusMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;
    streamEngineMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;
    directBufferMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;
    Arrays.asList(STORAGE_ENGINE, QUERY_ENGINE, SCHEMA_ENGINE, CONSENSUS, STREAM_ENGINE)
        .forEach(
            name ->
                metricService.remove(
                    MetricType.GAUGE,
                    Metric.THRESHOLD_MEMORY_SIZE.toString(),
                    Tag.NAME.toString(),
                    name,
                    Tag.TYPE.toString(),
                    ON_HEAP,
                    Tag.LEVEL.toString(),
                    LEVELS[1]));
    Collections.singletonList(DIRECT_BUFFER)
        .forEach(
            name ->
                metricService.remove(
                    MetricType.GAUGE,
                    Metric.THRESHOLD_MEMORY_SIZE.toString(),
                    Tag.NAME.toString(),
                    name,
                    Tag.TYPE.toString(),
                    OFF_HEAP,
                    Tag.LEVEL.toString(),
                    LEVELS[1]));
    unbindStorageEngineRelatedMemoryMetrics(metricService);
  }

  private void unbindStorageEngineRelatedMemoryMetrics(AbstractMetricService metricService) {
    writeMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;
    compactionMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;
    Arrays.asList(STORAGE_ENGINE_WRITE, STORAGE_ENGINE_COMPACTION)
        .forEach(
            name ->
                metricService.remove(
                    MetricType.GAUGE,
                    Metric.THRESHOLD_MEMORY_SIZE.toString(),
                    Tag.NAME.toString(),
                    name,
                    Tag.TYPE.toString(),
                    ON_HEAP,
                    Tag.LEVEL.toString(),
                    LEVELS[2]));
    memtableMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;
    timePartitionInfoMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;
    bufferedArraysMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;
    Arrays.asList(STORAGE_ENGINE_WRITE_MEMTABLE, STORAGE_ENGINE_WRITE_TIME_PARTITION_INFO)
        .forEach(
            name ->
                metricService.remove(
                    MetricType.GAUGE,
                    Metric.THRESHOLD_MEMORY_SIZE.toString(),
                    Tag.NAME.toString(),
                    name,
                    Tag.TYPE.toString(),
                    ON_HEAP,
                    Tag.LEVEL.toString(),
                    LEVELS[3]));
    dataNodeDevicePathCacheMemorySize = DoNothingMetricManager.DO_NOTHING_GAUGE;
    Arrays.asList(
            STORAGE_ENGINE_WRITE_MEMTABLE_CACHE, STORAGE_ENGINE_WRITE_MEMTABLE_BUFFERED_ARRAYS)
        .forEach(
            name ->
                metricService.remove(
                    MetricType.GAUGE,
                    Metric.THRESHOLD_MEMORY_SIZE.toString(),
                    Tag.NAME.toString(),
                    name,
                    Tag.TYPE.toString(),
                    ON_HEAP,
                    Tag.LEVEL.toString(),
                    LEVELS[4]));
  }

  public static ThresholdMemoryMetrics getInstance() {
    return ThresholdMemoryMetrics.ThresholdMemoryMetricsHolder.INSTANCE;
  }

  private static class ThresholdMemoryMetricsHolder {

    private static final ThresholdMemoryMetrics INSTANCE = new ThresholdMemoryMetrics();

    private ThresholdMemoryMetricsHolder() {}
  }
}
