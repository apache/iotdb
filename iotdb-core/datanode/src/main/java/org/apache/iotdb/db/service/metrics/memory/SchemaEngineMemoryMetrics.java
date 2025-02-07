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

public class SchemaEngineMemoryMetrics implements IMetricSet {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String SCHEMA_ENGINE = "SchemaEngine";
  private static final String SCHEMA_ENGINE_SCHEMA_REGION = "SchemaEngine-SchemaRegion";
  private static final String SCHEMA_ENGINE_SCHEMA_CACHE = "SchemaEngine-SchemaCache";
  private static final String SCHEMA_ENGINE_PARTITION_CACHE = "SchemaEngine-PartitionCache";

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.MEMORY_THRESHOLD_SIZE.toString(),
        MetricLevel.NORMAL,
        config.getSchemaEngineMemoryManager(),
        MemoryManager::getTotalMemorySizeInBytes,
        Tag.NAME.toString(),
        SCHEMA_ENGINE,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[1]);
    metricService.createAutoGauge(
        Metric.MEMORY_THRESHOLD_SIZE.toString(),
        MetricLevel.NORMAL,
        config.getSchemaRegionMemoryManager(),
        MemoryManager::getTotalMemorySizeInBytes,
        Tag.NAME.toString(),
        SCHEMA_ENGINE_SCHEMA_REGION,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[2]);
    metricService.createAutoGauge(
        Metric.MEMORY_ACTUAL_SIZE.toString(),
        MetricLevel.NORMAL,
        config.getSchemaRegionMemoryManager(),
        MemoryManager::getUsedMemorySizeInBytes,
        Tag.NAME.toString(),
        SCHEMA_ENGINE_SCHEMA_REGION,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[2]);
    metricService.createAutoGauge(
        Metric.MEMORY_THRESHOLD_SIZE.toString(),
        MetricLevel.NORMAL,
        config.getSchemaCacheMemoryManager(),
        MemoryManager::getTotalMemorySizeInBytes,
        Tag.NAME.toString(),
        SCHEMA_ENGINE_SCHEMA_CACHE,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[2]);
    metricService.createAutoGauge(
        Metric.MEMORY_ACTUAL_SIZE.toString(),
        MetricLevel.NORMAL,
        config.getSchemaCacheMemoryManager(),
        MemoryManager::getUsedMemorySizeInBytes,
        Tag.NAME.toString(),
        SCHEMA_ENGINE_SCHEMA_CACHE,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[2]);
    metricService.createAutoGauge(
        Metric.MEMORY_THRESHOLD_SIZE.toString(),
        MetricLevel.NORMAL,
        config.getPartitionCacheMemoryManager(),
        MemoryManager::getTotalMemorySizeInBytes,
        Tag.NAME.toString(),
        SCHEMA_ENGINE_PARTITION_CACHE,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[2]);
    metricService.createAutoGauge(
        Metric.MEMORY_ACTUAL_SIZE.toString(),
        MetricLevel.NORMAL,
        config.getPartitionCacheMemoryManager(),
        MemoryManager::getUsedMemorySizeInBytes,
        Tag.NAME.toString(),
        SCHEMA_ENGINE_PARTITION_CACHE,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[2]);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.MEMORY_THRESHOLD_SIZE.toString(),
        Tag.NAME.toString(),
        SCHEMA_ENGINE,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[1]);
    Arrays.asList(
            SCHEMA_ENGINE_SCHEMA_REGION, SCHEMA_ENGINE_SCHEMA_CACHE, SCHEMA_ENGINE_PARTITION_CACHE)
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
  }

  public static SchemaEngineMemoryMetrics getInstance() {
    return SchemaEngineMemoryMetricsHolder.INSTANCE;
  }

  private static class SchemaEngineMemoryMetricsHolder {

    private static final SchemaEngineMemoryMetrics INSTANCE = new SchemaEngineMemoryMetrics();

    private SchemaEngineMemoryMetricsHolder() {}
  }
}
