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

package org.apache.iotdb.metrics.metricsets.predefined.jvm;

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
      metricService.getOrCreateAutoGauge(
          "jvm.buffer.count.buffers",
          MetricLevel.IMPORTANT,
          bufferPoolBean,
          BufferPoolMXBean::getCount,
          "id",
          bufferPoolBean.getName());

      metricService.getOrCreateAutoGauge(
          "jvm.buffer.memory.used.bytes",
          MetricLevel.IMPORTANT,
          bufferPoolBean,
          BufferPoolMXBean::getMemoryUsed,
          "id",
          bufferPoolBean.getName());

      metricService.getOrCreateAutoGauge(
          "jvm.buffer.total.capacity.bytes",
          MetricLevel.IMPORTANT,
          bufferPoolBean,
          BufferPoolMXBean::getTotalCapacity,
          "id",
          bufferPoolBean.getName());
    }

    for (MemoryPoolMXBean memoryPoolBean :
        ManagementFactory.getPlatformMXBeans(MemoryPoolMXBean.class)) {
      String area = MemoryType.HEAP.equals(memoryPoolBean.getType()) ? "heap" : "nonheap";

      metricService.getOrCreateAutoGauge(
          "jvm.memory.used.bytes",
          MetricLevel.IMPORTANT,
          memoryPoolBean,
          (mem) -> (long) JvmUtils.getUsageValue(mem, MemoryUsage::getUsed),
          "id",
          memoryPoolBean.getName(),
          "area",
          area);

      metricService.getOrCreateAutoGauge(
          "jvm.memory.committed.bytes",
          MetricLevel.IMPORTANT,
          memoryPoolBean,
          (mem) -> (long) JvmUtils.getUsageValue(mem, MemoryUsage::getCommitted),
          "id",
          memoryPoolBean.getName(),
          "area",
          area);

      metricService.getOrCreateAutoGauge(
          "jvm.memory.max.bytes",
          MetricLevel.IMPORTANT,
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
          MetricType.GAUGE, "jvm.buffer.count.buffers", "id", bufferPoolBean.getName());

      metricService.remove(
          MetricType.GAUGE, "jvm.buffer.memory.used.bytes", "id", bufferPoolBean.getName());

      metricService.remove(
          MetricType.GAUGE, "jvm.buffer.total.capacity.bytes", "id", bufferPoolBean.getName());
    }

    for (MemoryPoolMXBean memoryPoolBean :
        ManagementFactory.getPlatformMXBeans(MemoryPoolMXBean.class)) {
      String area = MemoryType.HEAP.equals(memoryPoolBean.getType()) ? "heap" : "nonheap";

      metricService.remove(
          MetricType.GAUGE, "jvm.memory.used.bytes", "id", memoryPoolBean.getName(), "area", area);

      metricService.remove(
          MetricType.GAUGE,
          "jvm.memory.committed.bytes",
          "id",
          memoryPoolBean.getName(),
          "area",
          area);

      metricService.remove(
          MetricType.GAUGE, "jvm.memory.max.bytes", "id", memoryPoolBean.getName(), "area", area);
    }
  }
}
