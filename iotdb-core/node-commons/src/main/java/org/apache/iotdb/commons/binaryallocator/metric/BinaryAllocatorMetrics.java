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

package org.apache.iotdb.commons.binaryallocator.metric;

import org.apache.iotdb.commons.binaryallocator.BinaryAllocator;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class BinaryAllocatorMetrics implements IMetricSet {

  private static final String TOTAL_MEMORY = "total-memory";
  private static final String ALLOCATE_FROM_SLAB = "allocate-from-slab";
  private static final String ALLOCATE_FROM_JVM = "allocate-from-jvm";
  private static final String ACTIVE_MEMORY = "active-memory";

  private final BinaryAllocator binaryAllocator;
  private Counter allocateFromSlab;
  private Counter allocateFromJVM;

  public BinaryAllocatorMetrics(final BinaryAllocator binaryAllocator) {
    this.binaryAllocator = binaryAllocator;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.BINARY_ALLOCATOR.toString(),
        MetricLevel.IMPORTANT,
        binaryAllocator,
        BinaryAllocator::getTotalUsedMemory,
        Tag.NAME.toString(),
        TOTAL_MEMORY);
    metricService.createAutoGauge(
        Metric.BINARY_ALLOCATOR.toString(),
        MetricLevel.IMPORTANT,
        binaryAllocator,
        BinaryAllocator::getTotalActiveMemory,
        Tag.NAME.toString(),
        ACTIVE_MEMORY);
    allocateFromSlab =
        metricService.getOrCreateCounter(
            Metric.BINARY_ALLOCATOR.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            ALLOCATE_FROM_SLAB);
    allocateFromJVM =
        metricService.getOrCreateCounter(
            Metric.BINARY_ALLOCATOR.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            ALLOCATE_FROM_JVM);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.BINARY_ALLOCATOR.toString(),
        Tag.NAME.toString(),
        TOTAL_MEMORY);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.BINARY_ALLOCATOR.toString(),
        Tag.NAME.toString(),
        ACTIVE_MEMORY);
    metricService.remove(
        MetricType.COUNTER,
        Metric.BINARY_ALLOCATOR.toString(),
        Tag.NAME.toString(),
        ALLOCATE_FROM_SLAB);
    metricService.remove(
        MetricType.COUNTER,
        Metric.BINARY_ALLOCATOR.toString(),
        Tag.NAME.toString(),
        ALLOCATE_FROM_JVM);
  }

  public void updateCounter(int allocateFromSlabDelta, int allocateFromJVMDelta) {
    allocateFromSlab.inc(allocateFromSlabDelta);
    allocateFromJVM.inc(allocateFromJVMDelta);
  }
}
