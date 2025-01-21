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

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Arrays;
import java.util.Collections;

public class GlobalMemoryMetrics implements IMetricSet {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final SystemInfo systemInfo = SystemInfo.getInstance();

  private static final String TOTAL = "Total";
  public static final String ON_HEAP = "OnHeap";
  public static final String OFF_HEAP = "OffHeap";
  public static final String[] LEVELS = {"0", "1", "2", "3", "4"};

  private static final String DIRECT_BUFFER = "DirectBuffer";

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService
        .getOrCreateGauge(
            Metric.MEMORY_THRESHOLD_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            TOTAL,
            Tag.TYPE.toString(),
            ON_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[0])
        .set(Runtime.getRuntime().maxMemory());
    metricService
        .getOrCreateGauge(
            Metric.MEMORY_THRESHOLD_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            TOTAL,
            Tag.TYPE.toString(),
            OFF_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[0])
        .set(config.getMaxOffHeapMemoryBytes());
    StorageEngineMemoryMetrics.getInstance().bindTo(metricService);
    QueryEngineMemoryMetrics.getInstance().bindTo(metricService);
    SchemaEngineMemoryMetrics.getInstance().bindTo(metricService);
    ConsensusMemoryMetrics.getInstance().bindTo(metricService);
    StreamEngineMemoryMetrics.getInstance().bindTo(metricService);
    metricService
        .getOrCreateGauge(
            Metric.MEMORY_THRESHOLD_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            DIRECT_BUFFER,
            Tag.TYPE.toString(),
            OFF_HEAP,
            Tag.LEVEL.toString(),
            LEVELS[1])
        .set(systemInfo.getTotalDirectBufferMemorySizeLimit());
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    Arrays.asList(ON_HEAP, OFF_HEAP)
        .forEach(
            type -> {
              metricService.remove(
                  MetricType.GAUGE,
                  Metric.MEMORY_THRESHOLD_SIZE.toString(),
                  Tag.NAME.toString(),
                  TOTAL,
                  Tag.TYPE.toString(),
                  type,
                  Tag.LEVEL.toString(),
                  LEVELS[0]);
            });
    StorageEngineMemoryMetrics.getInstance().unbindFrom(metricService);
    QueryEngineMemoryMetrics.getInstance().unbindFrom(metricService);
    SchemaEngineMemoryMetrics.getInstance().unbindFrom(metricService);
    ConsensusMemoryMetrics.getInstance().unbindFrom(metricService);
    StreamEngineMemoryMetrics.getInstance().unbindFrom(metricService);
    Collections.singletonList(DIRECT_BUFFER)
        .forEach(
            name ->
                metricService.remove(
                    MetricType.GAUGE,
                    Metric.MEMORY_THRESHOLD_SIZE.toString(),
                    Tag.NAME.toString(),
                    name,
                    Tag.TYPE.toString(),
                    OFF_HEAP,
                    Tag.LEVEL.toString(),
                    LEVELS[1]));
  }

  public static GlobalMemoryMetrics getInstance() {
    return GlobalMemoryMetricsHolder.INSTANCE;
  }

  private static class GlobalMemoryMetricsHolder {

    private static final GlobalMemoryMetrics INSTANCE = new GlobalMemoryMetrics();

    private GlobalMemoryMetricsHolder() {}
  }
}
