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
import org.apache.iotdb.metrics.utils.SystemMetric;
import org.apache.iotdb.metrics.utils.SystemTag;

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
          SystemMetric.JVM_BUFFER_COUNT_BUFFERS.toString(),
          MetricLevel.CORE,
          bufferPoolBean,
          BufferPoolMXBean::getCount,
          SystemTag.ID.toString(),
          bufferPoolBean.getName());

      metricService.createAutoGauge(
          SystemMetric.JVM_BUFFER_MEMORY_USED_BYTES.toString(),
          MetricLevel.CORE,
          bufferPoolBean,
          BufferPoolMXBean::getMemoryUsed,
          SystemTag.ID.toString(),
          bufferPoolBean.getName());

      metricService.createAutoGauge(
          SystemMetric.JVM_BUFFER_TOTAL_CAPACITY_BYTES.toString(),
          MetricLevel.CORE,
          bufferPoolBean,
          BufferPoolMXBean::getTotalCapacity,
          SystemTag.ID.toString(),
          bufferPoolBean.getName());
    }

    for (MemoryPoolMXBean memoryPoolBean :
        ManagementFactory.getPlatformMXBeans(MemoryPoolMXBean.class)) {
      String area = MemoryType.HEAP.equals(memoryPoolBean.getType()) ? "heap" : "nonheap";

      metricService.createAutoGauge(
          SystemMetric.JVM_MEMORY_USED_BYTES.toString(),
          MetricLevel.CORE,
          memoryPoolBean,
          mem -> JvmUtils.getUsageValue(mem, MemoryUsage::getUsed),
          SystemTag.ID.toString(),
          memoryPoolBean.getName(),
          SystemTag.AREA.toString(),
          area);

      metricService.createAutoGauge(
          SystemMetric.JVM_MEMORY_COMMITTED_BYTES.toString(),
          MetricLevel.CORE,
          memoryPoolBean,
          mem -> JvmUtils.getUsageValue(mem, MemoryUsage::getCommitted),
          SystemTag.ID.toString(),
          memoryPoolBean.getName(),
          SystemTag.AREA.toString(),
          area);

      metricService.createAutoGauge(
          SystemMetric.JVM_MEMORY_MAX_BYTES.toString(),
          MetricLevel.CORE,
          memoryPoolBean,
          mem -> JvmUtils.getUsageValue(mem, MemoryUsage::getMax),
          SystemTag.ID.toString(),
          memoryPoolBean.getName(),
          SystemTag.AREA.toString(),
          area);
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    for (BufferPoolMXBean bufferPoolBean :
        ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
      metricService.remove(
          MetricType.AUTO_GAUGE,
          SystemMetric.JVM_BUFFER_COUNT_BUFFERS.toString(),
          SystemTag.ID.toString(),
          bufferPoolBean.getName());

      metricService.remove(
          MetricType.AUTO_GAUGE,
          SystemMetric.JVM_BUFFER_MEMORY_USED_BYTES.toString(),
          SystemTag.ID.toString(),
          bufferPoolBean.getName());

      metricService.remove(
          MetricType.AUTO_GAUGE,
          SystemMetric.JVM_BUFFER_TOTAL_CAPACITY_BYTES.toString(),
          SystemTag.ID.toString(),
          bufferPoolBean.getName());
    }

    for (MemoryPoolMXBean memoryPoolBean :
        ManagementFactory.getPlatformMXBeans(MemoryPoolMXBean.class)) {
      String area = MemoryType.HEAP.equals(memoryPoolBean.getType()) ? "heap" : "nonheap";

      metricService.remove(
          MetricType.AUTO_GAUGE,
          SystemMetric.JVM_MEMORY_USED_BYTES.toString(),
          SystemTag.ID.toString(),
          memoryPoolBean.getName(),
          SystemTag.AREA.toString(),
          area);

      metricService.remove(
          MetricType.AUTO_GAUGE,
          SystemMetric.JVM_MEMORY_COMMITTED_BYTES.toString(),
          SystemTag.ID.toString(),
          memoryPoolBean.getName(),
          SystemTag.AREA.toString(),
          area);

      metricService.remove(
          MetricType.AUTO_GAUGE,
          SystemMetric.JVM_MEMORY_MAX_BYTES.toString(),
          SystemTag.ID.toString(),
          memoryPoolBean.getName(),
          SystemTag.AREA.toString(),
          area);
    }
  }
}
