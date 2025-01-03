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
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Arrays;
import java.util.Collections;

public class ThresholdMemoryMetrics implements IMetricSet {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final SystemInfo systemInfo = SystemInfo.getInstance();

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
  private static final String QUERY_ENGINE_BLOOM_FILTER_CACHE = "QueryEngine-BloomFilterCache";
  private static final String QUERY_ENGINE_CHUNK_CACHE = "QueryEngine-ChunkCache";
  private static final String QUERY_ENGINE_TIME_SERIES_METADATA_CACHE =
      "QueryEngine-TimeSeriesMetadataCache";
  private static final String QUERY_ENGINE_OPERATORS = "QueryEngine-Operators";
  private static final String QUERY_ENGINE_DATA_EXCHANGE = "QueryEngine-DataExchange";
  private static final String QUERY_ENGINE_TIME_INDEX = "QueryEngine-TimeIndex";
  private static final String QUERY_ENGINE_COORDINATOR = "QueryEngine-Coordinator";
  private static final String SCHEMA_ENGINE = "SchemaEngine";
  private static final String SCHEMA_ENGINE_SCHEMA_REGION = "SchemaEngine-SchemaRegion";
  private static final String SCHEMA_ENGINE_SCHEMA_CACHE = "SchemaEngine-SchemaCache";
  private static final String SCHEMA_ENGINE_PARTITION_CACHE = "SchemaEngine-PartitionCache";
  private static final String CONSENSUS = "Consensus";
  private static final String STREAM_ENGINE = "StreamEngine";
  private static final String DIRECT_BUFFER = "DirectBuffer";
  private static final String[] LEVELS = {"0", "1", "2", "3", "4"};

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            TOTAL,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[0])
        .set(Runtime.getRuntime().maxMemory());
    bindStorageEngineRelatedMemoryMetrics(metricService);
    bindQueryEngineMemoryMetrics(metricService);
    bindSchemaEngineMemoryMetrics(metricService);
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            CONSENSUS,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[1])
        .set(config.getAllocateMemoryForConsensus());
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            STREAM_ENGINE,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[1])
        .set(config.getAllocateMemoryForPipe());
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            DIRECT_BUFFER,
            Tag.TYPE.toString(),
            OFF_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[1])
        .set(systemInfo.getTotalDirectBufferMemorySizeLimit());
  }

  /** Bind the memory threshold metrics of storage engine */
  private void bindStorageEngineRelatedMemoryMetrics(AbstractMetricService metricService) {
    long storageEngineSize = config.getAllocateMemoryForStorageEngine();
    // Total memory size of storage engine
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            STORAGE_ENGINE,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[1])
        .set(storageEngineSize);
    // The memory of storage engine divided into Write and Compaction
    long writeSize =
        (long)
            (config.getAllocateMemoryForStorageEngine() * (1 - config.getCompactionProportion()));
    long compactionSize = storageEngineSize - writeSize;
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            STORAGE_ENGINE_WRITE,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[2])
        .set(writeSize);
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            STORAGE_ENGINE_COMPACTION,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[2])
        .set(compactionSize);
    // The write memory of storage engine divided into MemTable and TimePartitionInfo
    long memtableSize =
        (long)
            (config.getAllocateMemoryForStorageEngine() * config.getWriteProportionForMemtable());
    long timePartitionInfoSize = config.getAllocateMemoryForTimePartitionInfo();
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            STORAGE_ENGINE_WRITE_MEMTABLE,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[3])
        .set(memtableSize);
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            STORAGE_ENGINE_WRITE_TIME_PARTITION_INFO,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[3])
        .set(timePartitionInfoSize);
    // The memtable memory of storage engine contain DataNodeDevicePathCache (NOTICE: This part of
    // memory is not divided)
    long dataNodeDevicePathCacheSize =
        (long)
            (config.getAllocateMemoryForStorageEngine()
                * config.getWriteProportionForMemtable()
                * config.getDevicePathCacheProportion());
    long bufferedArraySize =
        (long)
            (config.getAllocateMemoryForStorageEngine()
                * config.getBufferedArraysMemoryProportion());
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            STORAGE_ENGINE_WRITE_MEMTABLE_CACHE,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[4])
        .set(dataNodeDevicePathCacheSize);
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            STORAGE_ENGINE_WRITE_MEMTABLE_BUFFERED_ARRAYS,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[4])
        .set(bufferedArraySize);
  }

  /** Bind the memory threshold metrics of query engine */
  private void bindQueryEngineMemoryMetrics(AbstractMetricService metricService) {
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            QUERY_ENGINE,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[1])
        .set(config.getAllocateMemoryForRead());
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            QUERY_ENGINE_BLOOM_FILTER_CACHE,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[2])
        .set(config.getAllocateMemoryForBloomFilterCache());
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            QUERY_ENGINE_CHUNK_CACHE,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[2])
        .set(config.getAllocateMemoryForChunkCache());
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            QUERY_ENGINE_TIME_SERIES_METADATA_CACHE,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[2])
        .set(config.getAllocateMemoryForTimeSeriesMetaDataCache());
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            QUERY_ENGINE_OPERATORS,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[2])
        .set(config.getAllocateMemoryForOperators());
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            QUERY_ENGINE_DATA_EXCHANGE,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[2])
        .set(config.getAllocateMemoryForDataExchange());
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            QUERY_ENGINE_TIME_INDEX,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[2])
        .set(config.getAllocateMemoryForTimeIndex());
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            QUERY_ENGINE_COORDINATOR,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[2])
        .set(config.getAllocateMemoryForCoordinator());
  }

  /** Bind the memory threshold metrics of schema engine */
  private void bindSchemaEngineMemoryMetrics(AbstractMetricService metricService) {
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            SCHEMA_ENGINE,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[1])
        .set(config.getAllocateMemoryForSchema());
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            SCHEMA_ENGINE_SCHEMA_REGION,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[2])
        .set(config.getAllocateMemoryForSchemaRegion());
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            SCHEMA_ENGINE_SCHEMA_CACHE,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[2])
        .set(config.getAllocateMemoryForSchemaCache());
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            SCHEMA_ENGINE_PARTITION_CACHE,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[2])
        .set(config.getAllocateMemoryForPartitionCache());
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.GAUGE,
        Metric.THRESHOLD_MEMORY_SIZE.toString(),
        Tag.NAME.toString(),
        TOTAL,
        Tag.TYPE.toString(),
        ON_HEAP,
        Tag.LEVEL.toString(),
        "0");
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
    unbindStorageEngineRelatedMemoryMetrics(metricService);
    unbindQueryEngineMemoryMetrics(metricService);
    unbindSchemaEngineMemoryMetrics(metricService);
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
  }

  /** Unbind the memory threshold metrics of storage engine */
  private void unbindStorageEngineRelatedMemoryMetrics(AbstractMetricService metricService) {
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

  /** Unbind the memory threshold metrics of query engine */
  private void unbindQueryEngineMemoryMetrics(AbstractMetricService metricService) {
    Arrays.asList(
            QUERY_ENGINE_BLOOM_FILTER_CACHE,
            QUERY_ENGINE_CHUNK_CACHE,
            QUERY_ENGINE_TIME_SERIES_METADATA_CACHE,
            QUERY_ENGINE_OPERATORS,
            QUERY_ENGINE_DATA_EXCHANGE,
            QUERY_ENGINE_TIME_INDEX,
            QUERY_ENGINE_COORDINATOR)
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
  }

  private void unbindSchemaEngineMemoryMetrics(AbstractMetricService metricService) {
    Arrays.asList(
            SCHEMA_ENGINE_SCHEMA_REGION, SCHEMA_ENGINE_SCHEMA_CACHE, SCHEMA_ENGINE_PARTITION_CACHE)
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
  }

  public static ThresholdMemoryMetrics getInstance() {
    return ThresholdMemoryMetrics.ThresholdMemoryMetricsHolder.INSTANCE;
  }

  private static class ThresholdMemoryMetricsHolder {

    private static final ThresholdMemoryMetrics INSTANCE = new ThresholdMemoryMetrics();

    private ThresholdMemoryMetricsHolder() {}
  }
}
