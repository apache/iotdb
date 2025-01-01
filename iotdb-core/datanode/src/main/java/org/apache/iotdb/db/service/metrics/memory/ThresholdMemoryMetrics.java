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

public class ThresholdMemoryMetrics implements IMetricSet {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final SystemInfo systemInfo = SystemInfo.getInstance();

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            "Total",
            Tag.TYPE.toString(),
            "OnHeap",
            Tag.LEVEL.toString(),
            "0")
        .set(Runtime.getRuntime().maxMemory());
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            "StorageEngine",
            Tag.TYPE.toString(),
            "OnHeap",
            Tag.LEVEL.toString(),
            "1")
        .set(config.getAllocateMemoryForStorageEngine());
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            "QueryEngine",
            Tag.TYPE.toString(),
            "OnHeap",
            Tag.LEVEL.toString(),
            "1")
        .set(config.getAllocateMemoryForRead());
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            "SchemaEngine",
            Tag.TYPE.toString(),
            "OnHeap",
            Tag.LEVEL.toString(),
            "1")
        .set(config.getAllocateMemoryForSchema());
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            "Consensus",
            Tag.TYPE.toString(),
            "OnHeap",
            Tag.LEVEL.toString(),
            "1")
        .set(config.getAllocateMemoryForConsensus());
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            "StreamEngine",
            Tag.TYPE.toString(),
            "OnHeap",
            Tag.LEVEL.toString(),
            "1")
        .set(config.getAllocateMemoryForPipe());
    metricService
        .getOrCreateGauge(
            Metric.THRESHOLD_MEMORY_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            "DirectBuffer",
            Tag.TYPE.toString(),
            "OffHeap",
            Tag.LEVEL.toString(),
            "1")
        .set(systemInfo.getTotalDirectBufferMemorySizeLimit());
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.GAUGE,
        Metric.THRESHOLD_MEMORY_SIZE.toString(),
        Tag.NAME.toString(),
        "Total",
        Tag.TYPE.toString(),
        "OnHeap",
        Tag.LEVEL.toString(),
        "0");
    metricService.remove(
        MetricType.GAUGE,
        Metric.THRESHOLD_MEMORY_SIZE.toString(),
        Tag.NAME.toString(),
        "StorageEngine",
        Tag.TYPE.toString(),
        "OnHeap",
        Tag.LEVEL.toString(),
        "1");
    metricService.remove(
        MetricType.GAUGE,
        Metric.THRESHOLD_MEMORY_SIZE.toString(),
        Tag.NAME.toString(),
        "QueryEngine",
        Tag.TYPE.toString(),
        "OnHeap",
        Tag.LEVEL.toString(),
        "1");
    metricService.remove(
        MetricType.GAUGE,
        Metric.THRESHOLD_MEMORY_SIZE.toString(),
        Tag.NAME.toString(),
        "SchemaEngine",
        Tag.TYPE.toString(),
        "OnHeap",
        Tag.LEVEL.toString(),
        "1");
    metricService.remove(
        MetricType.GAUGE,
        Metric.THRESHOLD_MEMORY_SIZE.toString(),
        Tag.NAME.toString(),
        "Consensus",
        Tag.TYPE.toString(),
        "OnHeap",
        Tag.LEVEL.toString(),
        "1");
    metricService.remove(
        MetricType.GAUGE,
        Metric.THRESHOLD_MEMORY_SIZE.toString(),
        Tag.NAME.toString(),
        "StreamEngine",
        Tag.TYPE.toString(),
        "OnHeap",
        Tag.LEVEL.toString(),
        "1");
    metricService.remove(
        MetricType.GAUGE,
        Metric.THRESHOLD_MEMORY_SIZE.toString(),
        Tag.NAME.toString(),
        "DirectBuffer",
        Tag.TYPE.toString(),
        "OffHeap",
        Tag.LEVEL.toString(),
        "1");
  }

  public static ThresholdMemoryMetrics getInstance() {
    return ThresholdMemoryMetrics.ThresholdMemoryMetricsHolder.INSTANCE;
  }

  private static class ThresholdMemoryMetricsHolder {

    private static final ThresholdMemoryMetrics INSTANCE = new ThresholdMemoryMetrics();

    private ThresholdMemoryMetricsHolder() {}
  }
}
