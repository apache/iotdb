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
import org.apache.iotdb.db.conf.DataNodeMemoryConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Arrays;

public class StorageEngineMemoryMetrics implements IMetricSet {
  private static final DataNodeMemoryConfig memoryConfig =
      IoTDBDescriptor.getInstance().getMemoryConfig();
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
  private static final String STORAGE_ENGINE_PAM_ALLOCATION = "StorageEngine-PamAllocation";
  private static final String STORAGE_ENGINE_PAM_RELEASE = "StorageEngine-PamRelease";
  private static final String STORAGE_ENGINE_PAM_ALLOCATION_FAILURE =
      "StorageEngine-PamAllocationFailure";
  private static final String STORAGE_ENGINE_PAM_RELEASE_FAILURE =
      "StorageEngine-PamReleaseFailure";

  private Counter pamAllocationCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter pamReleaseCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter pamAllocationFailureCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter pamReleaseFailureCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;

  @Override
  public void bindTo(AbstractMetricService metricService) {
    // total memory of storage engine
    metricService.createAutoGauge(
        Metric.MEMORY_THRESHOLD_SIZE.toString(),
        MetricLevel.IMPORTANT,
        memoryConfig.getStorageEngineMemoryManager(),
        MemoryManager::getTotalMemorySizeInBytes,
        Tag.NAME.toString(),
        STORAGE_ENGINE,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[1]);
    bindStorageEngineDividedMetrics(metricService);
    bindWriteDividedMetrics(metricService);
    bindMemtableDividedMetrics(metricService);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.MEMORY_THRESHOLD_SIZE.toString(),
        Tag.NAME.toString(),
        STORAGE_ENGINE,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[1]);
    unbindStorageEngineDividedMetrics(metricService);
    unbindWriteDividedMetric(metricService);
    unbindMemtableDividedMetrics(metricService);
  }

  // region Storage Engine Divided Memory Metrics
  private void bindStorageEngineDividedMetrics(AbstractMetricService metricService) {
    // The memory of storage engine divided into write and compaction
    metricService.createAutoGauge(
        Metric.MEMORY_THRESHOLD_SIZE.toString(),
        MetricLevel.IMPORTANT,
        memoryConfig.getWriteMemoryManager(),
        MemoryManager::getTotalMemorySizeInBytes,
        Tag.NAME.toString(),
        STORAGE_ENGINE_WRITE,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[2]);
    metricService.createAutoGauge(
        Metric.MEMORY_THRESHOLD_SIZE.toString(),
        MetricLevel.IMPORTANT,
        memoryConfig.getCompactionMemoryManager(),
        MemoryManager::getTotalMemorySizeInBytes,
        Tag.NAME.toString(),
        STORAGE_ENGINE_COMPACTION,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[2]);
    metricService.createAutoGauge(
        Metric.MEMORY_ACTUAL_SIZE.toString(),
        MetricLevel.IMPORTANT,
        memoryConfig.getCompactionMemoryManager(),
        MemoryManager::getUsedMemorySizeInBytes,
        Tag.NAME.toString(),
        STORAGE_ENGINE_COMPACTION,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[2]);

    pamAllocationCounter =
        metricService.getOrCreateCounter(
            Metric.PAM_ALLOCATED_COUNT.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            STORAGE_ENGINE_PAM_ALLOCATION,
            Tag.TYPE.toString(),
            GlobalMemoryMetrics.ON_HEAP,
            Tag.LEVEL.toString(),
            GlobalMemoryMetrics.LEVELS[2]);
    pamReleaseCounter =
        metricService.getOrCreateCounter(
            Metric.PAM_RELEASED_COUNT.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            STORAGE_ENGINE_PAM_RELEASE,
            Tag.TYPE.toString(),
            GlobalMemoryMetrics.ON_HEAP,
            Tag.LEVEL.toString(),
            GlobalMemoryMetrics.LEVELS[2]);
    pamAllocationFailureCounter =
        metricService.getOrCreateCounter(
            Metric.PAM_ALLOCATED_FAILURE_COUNT.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            STORAGE_ENGINE_PAM_ALLOCATION,
            Tag.TYPE.toString(),
            GlobalMemoryMetrics.ON_HEAP,
            Tag.LEVEL.toString(),
            GlobalMemoryMetrics.LEVELS[2]);
    pamReleaseFailureCounter =
        metricService.getOrCreateCounter(
            Metric.PAM_RELEASED_FAILURE_COUNT.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            STORAGE_ENGINE_PAM_RELEASE,
            Tag.TYPE.toString(),
            GlobalMemoryMetrics.ON_HEAP,
            Tag.LEVEL.toString(),
            GlobalMemoryMetrics.LEVELS[2]);
  }

  private void unbindStorageEngineDividedMetrics(AbstractMetricService metricService) {
    Arrays.asList(STORAGE_ENGINE_WRITE, STORAGE_ENGINE_COMPACTION)
        .forEach(
            name -> {
              metricService.remove(
                  MetricType.AUTO_GAUGE,
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
    metricService.remove(
        MetricType.COUNTER,
        Metric.PAM_ALLOCATED_COUNT.toString(),
        Tag.NAME.toString(),
        STORAGE_ENGINE_PAM_ALLOCATION,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[2]);
    metricService.remove(
        MetricType.COUNTER,
        Metric.PAM_RELEASED_COUNT.toString(),
        Tag.NAME.toString(),
        STORAGE_ENGINE_PAM_RELEASE,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[2]);
    metricService.remove(
        MetricType.COUNTER,
        Metric.PAM_ALLOCATED_FAILURE_COUNT.toString(),
        Tag.NAME.toString(),
        STORAGE_ENGINE_PAM_ALLOCATION_FAILURE,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[2]);
    metricService.remove(
        MetricType.COUNTER,
        Metric.PAM_RELEASED_FAILURE_COUNT.toString(),
        Tag.NAME.toString(),
        STORAGE_ENGINE_PAM_RELEASE_FAILURE,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[2]);
    pamReleaseCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    pamAllocationCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    pamReleaseFailureCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    pamAllocationFailureCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  }

  // endregion

  // region Write Divided Memory Metrics
  private void bindWriteDividedMetrics(AbstractMetricService metricService) {
    // The memory of write divided into memtable and compaction
    metricService.createAutoGauge(
        Metric.MEMORY_THRESHOLD_SIZE.toString(),
        MetricLevel.IMPORTANT,
        memoryConfig.getMemtableMemoryManager(),
        MemoryManager::getTotalMemorySizeInBytes,
        Tag.NAME.toString(),
        STORAGE_ENGINE_WRITE_MEMTABLE,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[3]);
    metricService.createAutoGauge(
        Metric.MEMORY_THRESHOLD_SIZE.toString(),
        MetricLevel.IMPORTANT,
        memoryConfig.getTimePartitionInfoMemoryManager(),
        MemoryManager::getTotalMemorySizeInBytes,
        Tag.NAME.toString(),
        STORAGE_ENGINE_WRITE_TIME_PARTITION_INFO,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[3]);
    metricService.createAutoGauge(
        Metric.MEMORY_ACTUAL_SIZE.toString(),
        MetricLevel.IMPORTANT,
        memoryConfig.getTimePartitionInfoMemoryManager(),
        MemoryManager::getUsedMemorySizeInBytes,
        Tag.NAME.toString(),
        STORAGE_ENGINE_WRITE_TIME_PARTITION_INFO,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[3]);
  }

  private void unbindWriteDividedMetric(AbstractMetricService metricService) {
    Arrays.asList(STORAGE_ENGINE_WRITE_MEMTABLE, STORAGE_ENGINE_WRITE_TIME_PARTITION_INFO)
        .forEach(
            name ->
                metricService.remove(
                    MetricType.AUTO_GAUGE,
                    Metric.MEMORY_THRESHOLD_SIZE.toString(),
                    Tag.NAME.toString(),
                    name,
                    Tag.TYPE.toString(),
                    GlobalMemoryMetrics.ON_HEAP,
                    Tag.LEVEL.toString(),
                    GlobalMemoryMetrics.LEVELS[3]));
  }

  // endregion

  // region Memtable Divided Memory Metrics

  private void bindMemtableDividedMetrics(AbstractMetricService metricService) {
    // DevicePathCache related metrics
    metricService.createAutoGauge(
        Metric.MEMORY_THRESHOLD_SIZE.toString(),
        MetricLevel.IMPORTANT,
        memoryConfig.getDevicePathCacheMemoryManager(),
        MemoryManager::getTotalMemorySizeInBytes,
        Tag.NAME.toString(),
        STORAGE_ENGINE_WRITE_MEMTABLE_DEVICE_PATH_CACHE,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[4]);
    metricService.createAutoGauge(
        Metric.MEMORY_ACTUAL_SIZE.toString(),
        MetricLevel.IMPORTANT,
        memoryConfig.getDevicePathCacheMemoryManager(),
        MemoryManager::getUsedMemorySizeInBytes,
        Tag.NAME.toString(),
        STORAGE_ENGINE_WRITE_MEMTABLE_DEVICE_PATH_CACHE,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[4]);
    // BufferedArrays related metrics
    metricService.createAutoGauge(
        Metric.MEMORY_THRESHOLD_SIZE.toString(),
        MetricLevel.IMPORTANT,
        memoryConfig.getBufferedArraysMemoryManager(),
        MemoryManager::getTotalMemorySizeInBytes,
        Tag.NAME.toString(),
        STORAGE_ENGINE_WRITE_MEMTABLE_BUFFERED_ARRAYS,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[4]);
    metricService.createAutoGauge(
        Metric.MEMORY_ACTUAL_SIZE.toString(),
        MetricLevel.IMPORTANT,
        memoryConfig.getBufferedArraysMemoryManager(),
        MemoryManager::getUsedMemorySizeInBytes,
        Tag.NAME.toString(),
        STORAGE_ENGINE_WRITE_MEMTABLE_BUFFERED_ARRAYS,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[4]);
    // WalBufferQueue related metrics
    metricService.createAutoGauge(
        Metric.MEMORY_THRESHOLD_SIZE.toString(),
        MetricLevel.IMPORTANT,
        memoryConfig.getWalBufferQueueMemoryManager(),
        MemoryManager::getTotalMemorySizeInBytes,
        Tag.NAME.toString(),
        STORAGE_ENGINE_WRITE_MEMTABLE_WAL_BUFFER_QUEUE,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[4]);
    metricService.createAutoGauge(
        Metric.MEMORY_ACTUAL_SIZE.toString(),
        MetricLevel.IMPORTANT,
        memoryConfig.getWalBufferQueueMemoryManager(),
        MemoryManager::getUsedMemorySizeInBytes,
        Tag.NAME.toString(),
        STORAGE_ENGINE_WRITE_MEMTABLE_WAL_BUFFER_QUEUE,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[4]);
  }

  private void unbindMemtableDividedMetrics(AbstractMetricService metricService) {
    Arrays.asList(
            STORAGE_ENGINE_WRITE_MEMTABLE_DEVICE_PATH_CACHE,
            STORAGE_ENGINE_WRITE_MEMTABLE_BUFFERED_ARRAYS,
            STORAGE_ENGINE_WRITE_MEMTABLE_WAL_BUFFER_QUEUE)
        .forEach(
            name -> {
              metricService.remove(
                  MetricType.AUTO_GAUGE,
                  Metric.MEMORY_THRESHOLD_SIZE.toString(),
                  Tag.NAME.toString(),
                  name,
                  Tag.TYPE.toString(),
                  GlobalMemoryMetrics.ON_HEAP,
                  Tag.LEVEL.toString(),
                  GlobalMemoryMetrics.LEVELS[4]);
              metricService.remove(
                  MetricType.AUTO_GAUGE,
                  Metric.MEMORY_ACTUAL_SIZE.toString(),
                  Tag.NAME.toString(),
                  name,
                  Tag.TYPE.toString(),
                  GlobalMemoryMetrics.ON_HEAP,
                  Tag.LEVEL.toString(),
                  GlobalMemoryMetrics.LEVELS[4]);
            });
  }

  public void incPamAllocation() {
    pamAllocationCounter.inc();
  }

  public void incPamRelease() {
    pamReleaseCounter.inc();
  }

  public void incPamAllocationFailure() {
    pamAllocationFailureCounter.inc();
  }

  public void incPamReleaseFailure() {
    pamReleaseFailureCounter.inc();
  }

  public long getPamAllocation() {
    return pamAllocationCounter.getCount();
  }

  public long getPamRelease() {
    return pamReleaseCounter.getCount();
  }

  public long getPamAllocationFailure() {
    return pamAllocationFailureCounter.getCount();
  }

  public long getPamReleaseFailure() {
    return pamReleaseFailureCounter.getCount();
  }

  // endregion

  public static StorageEngineMemoryMetrics getInstance() {
    return StorageEngineMemoryMetricsHolder.INSTANCE;
  }

  private static class StorageEngineMemoryMetricsHolder {

    private static final StorageEngineMemoryMetrics INSTANCE = new StorageEngineMemoryMetrics();

    private StorageEngineMemoryMetricsHolder() {}
  }
}
