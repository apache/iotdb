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
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Arrays;

public class QueryEngineMemoryMetrics implements IMetricSet {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String QUERY_ENGINE = "QueryEngine";
  private static final String QUERY_ENGINE_BLOOM_FILTER_CACHE = "QueryEngine-BloomFilterCache";
  private static final String QUERY_ENGINE_CHUNK_CACHE = "QueryEngine-ChunkCache";
  private static final String QUERY_ENGINE_TIME_SERIES_METADATA_CACHE =
      "QueryEngine-TimeSeriesMetadataCache";
  private static final String QUERY_ENGINE_OPERATORS = "QueryEngine-Operators";
  private static final String QUERY_ENGINE_DATA_EXCHANGE = "QueryEngine-DataExchange";
  private static final String QUERY_ENGINE_TIME_INDEX = "QueryEngine-TimeIndex";
  private static final String QUERY_ENGINE_COORDINATOR = "QueryEngine-Coordinator";

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService
        .getOrCreateGauge(
            Metric.MEMORY_THRESHOLD_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            QUERY_ENGINE,
            Tag.TYPE.toString(),
            GlobalMemoryMetrics.ON_HEAP,
            Tag.LEVEL.toString(),
            GlobalMemoryMetrics.LEVELS[1])
        .set(config.getAllocateMemoryForRead());
    metricService
        .getOrCreateGauge(
            Metric.MEMORY_THRESHOLD_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            QUERY_ENGINE_BLOOM_FILTER_CACHE,
            Tag.TYPE.toString(),
            GlobalMemoryMetrics.ON_HEAP,
            Tag.LEVEL.toString(),
            GlobalMemoryMetrics.LEVELS[2])
        .set(config.getAllocateMemoryForBloomFilterCache());
    metricService
        .getOrCreateGauge(
            Metric.MEMORY_THRESHOLD_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            QUERY_ENGINE_CHUNK_CACHE,
            Tag.TYPE.toString(),
            GlobalMemoryMetrics.ON_HEAP,
            Tag.LEVEL.toString(),
            GlobalMemoryMetrics.LEVELS[2])
        .set(config.getAllocateMemoryForChunkCache());
    metricService
        .getOrCreateGauge(
            Metric.MEMORY_THRESHOLD_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            QUERY_ENGINE_TIME_SERIES_METADATA_CACHE,
            Tag.TYPE.toString(),
            GlobalMemoryMetrics.ON_HEAP,
            Tag.LEVEL.toString(),
            GlobalMemoryMetrics.LEVELS[2])
        .set(config.getAllocateMemoryForTimeSeriesMetaDataCache());
    metricService
        .getOrCreateGauge(
            Metric.MEMORY_THRESHOLD_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            QUERY_ENGINE_OPERATORS,
            Tag.TYPE.toString(),
            GlobalMemoryMetrics.ON_HEAP,
            Tag.LEVEL.toString(),
            GlobalMemoryMetrics.LEVELS[2])
        .set(config.getAllocateMemoryForOperators());
    metricService
        .getOrCreateGauge(
            Metric.MEMORY_THRESHOLD_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            QUERY_ENGINE_DATA_EXCHANGE,
            Tag.TYPE.toString(),
            GlobalMemoryMetrics.ON_HEAP,
            Tag.LEVEL.toString(),
            GlobalMemoryMetrics.LEVELS[2])
        .set(config.getAllocateMemoryForDataExchange());
    metricService
        .getOrCreateGauge(
            Metric.MEMORY_THRESHOLD_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            QUERY_ENGINE_TIME_INDEX,
            Tag.TYPE.toString(),
            GlobalMemoryMetrics.ON_HEAP,
            Tag.LEVEL.toString(),
            GlobalMemoryMetrics.LEVELS[2])
        .set(config.getAllocateMemoryForTimeIndex());
    metricService
        .getOrCreateGauge(
            Metric.MEMORY_THRESHOLD_SIZE.toString(),
            MetricLevel.NORMAL,
            Tag.NAME.toString(),
            QUERY_ENGINE_COORDINATOR,
            Tag.TYPE.toString(),
            GlobalMemoryMetrics.ON_HEAP,
            Tag.LEVEL.toString(),
            GlobalMemoryMetrics.LEVELS[2])
        .set(config.getAllocateMemoryForCoordinator());
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.GAUGE,
        Metric.MEMORY_THRESHOLD_SIZE.toString(),
        Tag.NAME.toString(),
        QUERY_ENGINE,
        Tag.TYPE.toString(),
        GlobalMemoryMetrics.ON_HEAP,
        Tag.LEVEL.toString(),
        GlobalMemoryMetrics.LEVELS[1]);
    Arrays.asList(
            QUERY_ENGINE_BLOOM_FILTER_CACHE,
            QUERY_ENGINE_CHUNK_CACHE,
            QUERY_ENGINE_TIME_SERIES_METADATA_CACHE,
            QUERY_ENGINE_OPERATORS,
            QUERY_ENGINE_DATA_EXCHANGE,
            QUERY_ENGINE_TIME_INDEX,
            QUERY_ENGINE_COORDINATOR)
        .forEach(
            name ->
                metricService.remove(
                    MetricType.GAUGE,
                    Metric.MEMORY_THRESHOLD_SIZE.toString(),
                    Tag.NAME.toString(),
                    name,
                    Tag.TYPE.toString(),
                    GlobalMemoryMetrics.ON_HEAP,
                    Tag.LEVEL.toString(),
                    GlobalMemoryMetrics.LEVELS[2]));
  }

  public static QueryEngineMemoryMetrics getInstance() {
    return QueryEngineMemoryMetricsHolder.INSTANCE;
  }

  private static class QueryEngineMemoryMetricsHolder {

    private static final QueryEngineMemoryMetrics INSTANCE = new QueryEngineMemoryMetrics();

    private QueryEngineMemoryMetricsHolder() {}
  }
}
