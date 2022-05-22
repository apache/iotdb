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

package org.apache.iotdb.metrics.predefined.jvm;

import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.predefined.IMetricSet;
import org.apache.iotdb.metrics.utils.JvmUtils;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.PredefinedMetric;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;

/** This file is modified from io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics */
public class JvmMemoryMetrics implements IMetricSet {
  @Override
  public void bindTo(MetricManager metricManager) {
    for (BufferPoolMXBean bufferPoolBean :
        ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
      metricManager.getOrCreateAutoGauge(
          "jvm.buffer.count.buffers",
          MetricLevel.IMPORTANT,
          bufferPoolBean,
          BufferPoolMXBean::getCount,
          "id",
          bufferPoolBean.getName());

      metricManager.getOrCreateAutoGauge(
          "jvm.buffer.memory.used.bytes",
          MetricLevel.IMPORTANT,
          bufferPoolBean,
          BufferPoolMXBean::getMemoryUsed,
          "id",
          bufferPoolBean.getName());

      metricManager.getOrCreateAutoGauge(
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

      metricManager.getOrCreateAutoGauge(
          "jvm.memory.used.bytes",
          MetricLevel.IMPORTANT,
          memoryPoolBean,
          (mem) -> (long) JvmUtils.getUsageValue(mem, MemoryUsage::getUsed),
          "id",
          memoryPoolBean.getName(),
          "area",
          area);

      metricManager.getOrCreateAutoGauge(
          "jvm.memory.committed.bytes",
          MetricLevel.IMPORTANT,
          memoryPoolBean,
          (mem) -> (long) JvmUtils.getUsageValue(mem, MemoryUsage::getCommitted),
          "id",
          memoryPoolBean.getName(),
          "area",
          area);

      metricManager.getOrCreateAutoGauge(
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
  public PredefinedMetric getType() {
    return PredefinedMetric.JVM;
  }
}
