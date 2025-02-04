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

import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Arrays;

public class StorageEngineMemoryMetrics implements IMetricSet {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String STORAGE_ENGINE = "StorageEngine";
  private static final String STORAGE_ENGINE_WRITE = "StorageEngine-Write";
  private static final String STORAGE_ENGINE_WRITE_MEMTABLE = "StorageEngine-Write-Memtable";
  private static final String STORAGE_ENGINE_WRITE_MEMTABLE_DEVICE_PATH_CACHE =
      "StorageEngine-Write-Memtable-DevicePathCache";
  private static final String STORAGE_ENGINE_WRITE_MEMTABLE_BUFFERED_ARRAYS =
      "StorageEngine-Write-Memtable-BufferedArrays";
  private static final String STORAGE_ENGINE_WRITE_MEMTABLE_WAL_BUFFER_QUEUE =
      "StorageEngine-Write-Memtable-WalBufferQueue";
  private static final String STORAGE_ENGINE_WRITE_TIME_PARTITION_INFO =
      "StorageEngine-Write-TimePartitionInfo";
  private static final String STORAGE_ENGINE_COMPACTION = "StorageEngine-Compaction";

  @Override
  public void bindTo(AbstractMetricService metricService) {
    long storageEngineSize = config.getStorageEngineMemoryManager().getTotalMemorySizeInBytes();
    // Total memory size of storage engine
    metricService
        .getOrCreateGauge(
            Metric.MEMORY_THRESHOLD_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            STORAGE_ENGINE,
            Tag.TYPE.toString(),
            GlobalMemoryMetrics.ON_HEAP,
            Tag.LEVEL.toString(),
            GlobalMemoryMetrics.LEVELS[1])
        .set(config.getStorageEngineMemoryManager().getTotalMemorySizeInBytes());
    // The memory of storage engine divided into Write and Compaction
    metricService
        .getOrCreateGauge(
            Metric.MEMORY_THRESHOLD_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            STORAGE_ENGINE_WRITE,
            Tag.TYPE.toString(),
            GlobalMemoryMetrics.ON_HEAP,
            Tag.LEVEL.toString(),
            GlobalMemoryMetrics.LEVELS[2])
        .set(config.getWriteMemoryManager().getTotalMemorySizeInBytes());
    metricService
        .getOrCreateGauge(
            Metric.MEMORY_THRESHOLD_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            STORAGE_ENGINE_COMPACTION,
            Tag.TYPE.toString(),
            GlobalMemoryMetrics.ON_HEAP,
            Tag.LEVEL.toString(),
            GlobalMemoryMetrics.LEVELS[2])
        .set(config.getCompactionMemoryManager().getTotalMemorySizeInBytes());
    metricService.createAutoGauge(
        Metric.MEMORY_ACTUAL_SIZE.toString(),
        MetricLevel.NORMAL,
        config.getCompactionMemoryManager(),
        MemoryManager::getUsedMemorySizeInBytes,
        Tag.NAME.toString(),
        STORAGE_ENGINE_COMPACTION,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[1]);
    // The write memory of storage engine divided into MemTable and TimePartitionInfo
    metricService
        .getOrCreateGauge(
            Metric.MEMORY_THRESHOLD_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            STORAGE_ENGINE_WRITE_MEMTABLE,
            Tag.TYPE.toString(),
            GlobalMemoryMetrics.ON_HEAP,
            Tag.LEVEL.toString(),
            GlobalMemoryMetrics.LEVELS[3])
        .set(config.getMemtableMemoryManager().getTotalMemorySizeInBytes());
    metricService
        .getOrCreateGauge(
            Metric.MEMORY_THRESHOLD_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            STORAGE_ENGINE_WRITE_TIME_PARTITION_INFO,
            Tag.TYPE.toString(),
            GlobalMemoryMetrics.ON_HEAP,
            Tag.LEVEL.toString(),
            GlobalMemoryMetrics.LEVELS[3])
        .set(config.getTimePartitionInfoMemoryManager().getTotalMemorySizeInBytes());
    // The memtable memory of storage engine contain DataNodeDevicePathCache (NOTICE: This part of
    // memory is not divided)
    metricService
        .getOrCreateGauge(
            Metric.MEMORY_THRESHOLD_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            STORAGE_ENGINE_WRITE_MEMTABLE_DEVICE_PATH_CACHE,
            Tag.TYPE.toString(),
            GlobalMemoryMetrics.ON_HEAP,
            Tag.LEVEL.toString(),
            GlobalMemoryMetrics.LEVELS[4])
        .set(config.getDevicePathCacheMemoryManager().getTotalMemorySizeInBytes());
    metricService
        .getOrCreateGauge(
            Metric.MEMORY_THRESHOLD_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            STORAGE_ENGINE_WRITE_MEMTABLE_BUFFERED_ARRAYS,
            Tag.TYPE.toString(),
            GlobalMemoryMetrics.ON_HEAP,
            Tag.LEVEL.toString(),
            GlobalMemoryMetrics.LEVELS[4])
        .set(config.getBufferedArraysMemoryManager().getTotalMemorySizeInBytes());
    metricService
        .getOrCreateGauge(
            Metric.MEMORY_THRESHOLD_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            STORAGE_ENGINE_WRITE_MEMTABLE_WAL_BUFFER_QUEUE,
            Tag.TYPE.toString(),
            GlobalMemoryMetrics.ON_HEAP,
            Tag.LEVEL.toString(),
            GlobalMemoryMetrics.LEVELS[4])
        .set(config.getWalBufferQueueManager().getTotalMemorySizeInBytes());
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.GAUGE,
        Metric.MEMORY_THRESHOLD_SIZE.toString(),
        Tag.NAME.toString(),
        STORAGE_ENGINE,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[1]);
    Arrays.asList(STORAGE_ENGINE_WRITE, STORAGE_ENGINE_COMPACTION)
        .forEach(
            name -> {
              metricService.remove(
                  MetricType.GAUGE,
                  Metric.MEMORY_THRESHOLD_SIZE.toString(),
                  Tag.NAME.toString(),
                  name,
                  Tag.TYPE.toString(),
                  GlobalMemoryMetrics.ON_HEAP,
                  Tag.LEVEL.toString(),
                  GlobalMemoryMetrics.LEVELS[2]);
              metricService.remove(
                  MetricType.AUTO_GAUGE,
                  Metric.MEMORY_ACTUAL_SIZE.toString(),
                  Tag.NAME.toString(),
                  name,
                  Tag.TYPE.toString(),
                  GlobalMemoryMetrics.ON_HEAP,
                  Tag.LEVEL.toString(),
                  GlobalMemoryMetrics.LEVELS[2]);
            });
    Arrays.asList(STORAGE_ENGINE_WRITE_MEMTABLE, STORAGE_ENGINE_WRITE_TIME_PARTITION_INFO)
        .forEach(
            name ->
                metricService.remove(
                    MetricType.GAUGE,
                    Metric.MEMORY_THRESHOLD_SIZE.toString(),
                    Tag.NAME.toString(),
                    name,
                    Tag.TYPE.toString(),
                    GlobalMemoryMetrics.ON_HEAP,
                    Tag.LEVEL.toString(),
                    GlobalMemoryMetrics.LEVELS[3]));
    Arrays.asList(
            STORAGE_ENGINE_WRITE_MEMTABLE_DEVICE_PATH_CACHE,
            STORAGE_ENGINE_WRITE_MEMTABLE_BUFFERED_ARRAYS,
            STORAGE_ENGINE_WRITE_MEMTABLE_WAL_BUFFER_QUEUE)
        .forEach(
            name ->
                metricService.remove(
                    MetricType.GAUGE,
                    Metric.MEMORY_THRESHOLD_SIZE.toString(),
                    Tag.NAME.toString(),
                    name,
                    Tag.TYPE.toString(),
                    GlobalMemoryMetrics.ON_HEAP,
                    Tag.LEVEL.toString(),
                    GlobalMemoryMetrics.LEVELS[4]));
  }

  public static StorageEngineMemoryMetrics getInstance() {
    return StorageEngineMemoryMetricsHolder.INSTANCE;
  }

  private static class StorageEngineMemoryMetricsHolder {

    private static final StorageEngineMemoryMetrics INSTANCE = new StorageEngineMemoryMetrics();

    private StorageEngineMemoryMetricsHolder() {}
  }
}
