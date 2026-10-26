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

package org.apache.iotdb.commons.memory;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.rpc.AutoResizingBufferMemoryManager;

public class AutoResizingBufferMemoryMetrics implements IMetricSet {

  private static final String ALLOCATION_COUNT = "auto_resizing_buffer_allocation_count";
  private static final String ALLOCATION_FAILURE_COUNT =
      "auto_resizing_buffer_allocation_failure_count";
  private static final String TOTAL_MEMORY = "auto_resizing_buffer_total_memory";
  private static final String USED_MEMORY = "auto_resizing_buffer_used_memory";
  private static final String AVAILABLE_MEMORY = "auto_resizing_buffer_available_memory";

  private final MemoryConfig memoryConfig;

  public AutoResizingBufferMemoryMetrics(MemoryConfig memoryConfig) {
    this.memoryConfig = memoryConfig;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.THRIFT_RPC_MEMORY_USAGE.toString(),
        MetricLevel.IMPORTANT,
        AutoResizingBufferMemoryManager.class,
        ignored -> AutoResizingBufferMemoryManager.getMemoryAllocationCount(),
        Tag.NAME.toString(),
        ALLOCATION_COUNT);
    metricService.createAutoGauge(
        Metric.THRIFT_RPC_MEMORY_USAGE.toString(),
        MetricLevel.IMPORTANT,
        AutoResizingBufferMemoryManager.class,
        ignored -> AutoResizingBufferMemoryManager.getMemoryAllocationFailureCount(),
        Tag.NAME.toString(),
        ALLOCATION_FAILURE_COUNT);
    metricService.createAutoGauge(
        Metric.THRIFT_RPC_MEMORY_USAGE.toString(),
        MetricLevel.IMPORTANT,
        memoryConfig,
        MemoryConfig::getAutoResizingBufferMemoryTotalSizeInBytes,
        Tag.NAME.toString(),
        TOTAL_MEMORY);
    metricService.createAutoGauge(
        Metric.THRIFT_RPC_MEMORY_USAGE.toString(),
        MetricLevel.IMPORTANT,
        memoryConfig,
        MemoryConfig::getAutoResizingBufferMemoryUsedSizeInBytes,
        Tag.NAME.toString(),
        USED_MEMORY);
    metricService.createAutoGauge(
        Metric.THRIFT_RPC_MEMORY_USAGE.toString(),
        MetricLevel.IMPORTANT,
        memoryConfig,
        MemoryConfig::getAutoResizingBufferMemoryAvailableSizeInBytes,
        Tag.NAME.toString(),
        AVAILABLE_MEMORY);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.THRIFT_RPC_MEMORY_USAGE.toString(),
        Tag.NAME.toString(),
        ALLOCATION_COUNT);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.THRIFT_RPC_MEMORY_USAGE.toString(),
        Tag.NAME.toString(),
        ALLOCATION_FAILURE_COUNT);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.THRIFT_RPC_MEMORY_USAGE.toString(),
        Tag.NAME.toString(),
        TOTAL_MEMORY);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.THRIFT_RPC_MEMORY_USAGE.toString(),
        Tag.NAME.toString(),
        USED_MEMORY);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.THRIFT_RPC_MEMORY_USAGE.toString(),
        Tag.NAME.toString(),
        AVAILABLE_MEMORY);
  }
}
