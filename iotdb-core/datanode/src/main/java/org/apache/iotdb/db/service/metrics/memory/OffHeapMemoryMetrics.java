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

import java.util.Collections;

public class OffHeapMemoryMetrics implements IMetricSet {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final String DIRECT_BUFFER = "DirectBuffer";

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.MEMORY_THRESHOLD_SIZE.toString(),
        MetricLevel.NORMAL,
        config.getDirectBufferMemoryManager(),
        MemoryManager::getTotalMemorySizeInBytes,
        Tag.NAME.toString(),
        DIRECT_BUFFER,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.OFF_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[1]);
    metricService.createAutoGauge(
        Metric.MEMORY_ACTUAL_SIZE.toString(),
        MetricLevel.NORMAL,
        config.getDirectBufferMemoryManager(),
        MemoryManager::getUsedMemorySizeInBytes,
        Tag.NAME.toString(),
        DIRECT_BUFFER,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.OFF_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[1]);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    Collections.singletonList(DIRECT_BUFFER)
        .forEach(
            name -> {
              metricService.remove(
                  MetricType.AUTO_GAUGE,
                  Metric.MEMORY_THRESHOLD_SIZE.toString(),
                  Tag.NAME.toString(),
                  name,
                  Tag.TYPE.toString(),
                  GlobalMemoryMetrics.OFF_HEAP,
                  Tag.LEVEL.toString(),
                  GlobalMemoryMetrics.LEVELS[1]);
              metricService.remove(
                  MetricType.AUTO_GAUGE,
                  Metric.MEMORY_ACTUAL_SIZE.toString(),
                  Tag.NAME.toString(),
                  name,
                  Tag.TYPE.toString(),
                  GlobalMemoryMetrics.OFF_HEAP,
                  Tag.LEVEL.toString(),
                  GlobalMemoryMetrics.LEVELS[1]);
            });
  }

  public static OffHeapMemoryMetrics getInstance() {
    return OffHeapMemoryMetrics.OffHeapMemoryMetricsHolder.INSTANCE;
  }

  private static class OffHeapMemoryMetricsHolder {

    private static final OffHeapMemoryMetrics INSTANCE = new OffHeapMemoryMetrics();

    private OffHeapMemoryMetricsHolder() {}
  }
}
