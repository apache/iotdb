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

package org.apache.iotdb.metrics.metricsets.jvm;

import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;

/** This file is modified from io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics */
public class JvmMemoryMetrics implements IMetricSet {
  @Override
  public void bindTo(AbstractMetricService metricService) {
    for (BufferPoolMXBean bufferPoolBean :
        ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
      metricService.createAutoGauge(
          "jvm_buffer_count_buffers",
          MetricLevel.CORE,
          bufferPoolBean,
          BufferPoolMXBean::getCount,
          "id",
          bufferPoolBean.getName());

      metricService.createAutoGauge(
          "jvm_buffer_memory_used_bytes",
          MetricLevel.CORE,
          bufferPoolBean,
          BufferPoolMXBean::getMemoryUsed,
          "id",
          bufferPoolBean.getName());

      metricService.createAutoGauge(
          "jvm_buffer_total_capacity_bytes",
          MetricLevel.CORE,
          bufferPoolBean,
          BufferPoolMXBean::getTotalCapacity,
          "id",
          bufferPoolBean.getName());
    }

    for (MemoryPoolMXBean memoryPoolBean :
        ManagementFactory.getPlatformMXBeans(MemoryPoolMXBean.class)) {
      String area = MemoryType.HEAP.equals(memoryPoolBean.getType()) ? "heap" : "nonheap";

      metricService.createAutoGauge(
          "jvm_memory_used_bytes",
          MetricLevel.CORE,
          memoryPoolBean,
          (mem) -> (long) JvmUtils.getUsageValue(mem, MemoryUsage::getUsed),
          "id",
          memoryPoolBean.getName(),
          "area",
          area);

      metricService.createAutoGauge(
          "jvm_memory_committed_bytes",
          MetricLevel.CORE,
          memoryPoolBean,
          (mem) -> (long) JvmUtils.getUsageValue(mem, MemoryUsage::getCommitted),
          "id",
          memoryPoolBean.getName(),
          "area",
          area);

      metricService.createAutoGauge(
          "jvm_memory_max_bytes",
          MetricLevel.CORE,
          memoryPoolBean,
          (mem) -> (long) JvmUtils.getUsageValue(mem, MemoryUsage::getMax),
          "id",
          memoryPoolBean.getName(),
          "area",
          area);
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    for (BufferPoolMXBean bufferPoolBean :
        ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
      metricService.remove(
          MetricType.AUTO_GAUGE, "jvm_buffer_count_buffers", "id", bufferPoolBean.getName());

      metricService.remove(
          MetricType.AUTO_GAUGE, "jvm_buffer_memory_used_bytes", "id", bufferPoolBean.getName());

      metricService.remove(
          MetricType.AUTO_GAUGE, "jvm_buffer_total_capacity_bytes", "id", bufferPoolBean.getName());
    }

    for (MemoryPoolMXBean memoryPoolBean :
        ManagementFactory.getPlatformMXBeans(MemoryPoolMXBean.class)) {
      String area = MemoryType.HEAP.equals(memoryPoolBean.getType()) ? "heap" : "nonheap";

      metricService.remove(
          MetricType.AUTO_GAUGE,
          "jvm_memory_used_bytes",
          "id",
          memoryPoolBean.getName(),
          "area",
          area);

      metricService.remove(
          MetricType.AUTO_GAUGE,
          "jvm_memory_committed_bytes",
          "id",
          memoryPoolBean.getName(),
          "area",
          area);

      metricService.remove(
          MetricType.AUTO_GAUGE,
          "jvm_memory_max_bytes",
          "id",
          memoryPoolBean.getName(),
          "area",
          area);
    }
  }
}
