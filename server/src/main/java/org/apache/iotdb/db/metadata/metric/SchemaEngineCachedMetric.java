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
package org.apache.iotdb.db.metadata.metric;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.CacheMemoryManager;
import org.apache.iotdb.db.metadata.mtree.store.disk.memcontrol.ReleaseFlushStrategySizeBasedImpl;
import org.apache.iotdb.db.metadata.rescon.CachedSchemaEngineStatistics;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.concurrent.TimeUnit;

public class SchemaEngineCachedMetric implements ISchemaEngineMetric {

  private static final String RELEASE_THRESHOLD = "schema_file_release_threshold";
  private static final String FLUSH_THRESHOLD = "schema_file_flush_threshold";
  private static final String PINNED_NODE_NUM = "schema_file_pinned_num";
  private static final String UNPINNED_NODE_NUM = "schema_file_unpinned_num";
  private static final String PINNED_MEM_SIZE = "schema_file_pinned_mem";
  private static final String UNPINNED_MEM_SIZE = "schema_file_unpinned_mem";
  private static final String RELEASE_TIMER = "schema_file_release";
  private static final String FLUSH_TIMER = "schema_file_flush";
  private static final String RELEASE_THREAD_NUM = "schema_file_release_thread_num";
  private static final String FLUSH_THREAD_NUM = "schema_file_flush_thread_num";

  private final CachedSchemaEngineStatistics engineStatistics;

  private final SchemaEngineMemMetric schemaEngineMemMetric;

  public SchemaEngineCachedMetric(CachedSchemaEngineStatistics engineStatistics) {
    this.engineStatistics = engineStatistics;
    this.schemaEngineMemMetric = new SchemaEngineMemMetric(engineStatistics);
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    schemaEngineMemMetric.bindTo(metricService);
    metricService.gauge(
        (long)
            (IoTDBDescriptor.getInstance().getConfig().getAllocateMemoryForSchemaRegion()
                * ReleaseFlushStrategySizeBasedImpl.RELEASE_THRESHOLD_RATIO),
        Metric.SCHEMA_ENGINE.toString(),
        MetricLevel.IMPORTANT,
        Tag.NAME.toString(),
        RELEASE_THRESHOLD);
    metricService.gauge(
        (long)
            (IoTDBDescriptor.getInstance().getConfig().getAllocateMemoryForSchemaRegion()
                * ReleaseFlushStrategySizeBasedImpl.FLUSH_THRESHOLD_RATION),
        Metric.SCHEMA_ENGINE.toString(),
        MetricLevel.IMPORTANT,
        Tag.NAME.toString(),
        FLUSH_THRESHOLD);
    metricService.createAutoGauge(
        Metric.SCHEMA_ENGINE.toString(),
        MetricLevel.IMPORTANT,
        engineStatistics,
        CachedSchemaEngineStatistics::getPinnedMNodeNum,
        Tag.NAME.toString(),
        PINNED_NODE_NUM);
    metricService.createAutoGauge(
        Metric.SCHEMA_ENGINE.toString(),
        MetricLevel.IMPORTANT,
        engineStatistics,
        CachedSchemaEngineStatistics::getUnpinnedMNodeNum,
        Tag.NAME.toString(),
        UNPINNED_NODE_NUM);
    metricService.createAutoGauge(
        Metric.SCHEMA_ENGINE.toString(),
        MetricLevel.IMPORTANT,
        engineStatistics,
        CachedSchemaEngineStatistics::getPinnedMemorySize,
        Tag.NAME.toString(),
        PINNED_MEM_SIZE);
    metricService.createAutoGauge(
        Metric.SCHEMA_ENGINE.toString(),
        MetricLevel.IMPORTANT,
        engineStatistics,
        CachedSchemaEngineStatistics::getUnpinnedMemorySize,
        Tag.NAME.toString(),
        UNPINNED_MEM_SIZE);
    metricService.getOrCreateTimer(
        Metric.SCHEMA_ENGINE.toString(), MetricLevel.IMPORTANT, Tag.NAME.toString(), RELEASE_TIMER);
    metricService.getOrCreateTimer(
        Metric.SCHEMA_ENGINE.toString(), MetricLevel.IMPORTANT, Tag.NAME.toString(), FLUSH_TIMER);
    metricService.createAutoGauge(
        Metric.SCHEMA_ENGINE.toString(),
        MetricLevel.IMPORTANT,
        CacheMemoryManager.getInstance(),
        CacheMemoryManager::getReleaseThreadNum,
        Tag.NAME.toString(),
        RELEASE_THREAD_NUM);
    metricService.createAutoGauge(
        Metric.SCHEMA_ENGINE.toString(),
        MetricLevel.IMPORTANT,
        CacheMemoryManager.getInstance(),
        CacheMemoryManager::getFlushThreadNum,
        Tag.NAME.toString(),
        FLUSH_THREAD_NUM);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    schemaEngineMemMetric.unbindFrom(metricService);
    metricService.remove(
        MetricType.GAUGE, Metric.SCHEMA_ENGINE.toString(), Tag.NAME.toString(), RELEASE_THRESHOLD);
    metricService.remove(
        MetricType.GAUGE, Metric.SCHEMA_ENGINE.toString(), Tag.NAME.toString(), FLUSH_THRESHOLD);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SCHEMA_ENGINE.toString(),
        Tag.NAME.toString(),
        PINNED_NODE_NUM);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SCHEMA_ENGINE.toString(),
        Tag.NAME.toString(),
        UNPINNED_NODE_NUM);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SCHEMA_ENGINE.toString(),
        Tag.NAME.toString(),
        PINNED_MEM_SIZE);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SCHEMA_ENGINE.toString(),
        Tag.NAME.toString(),
        UNPINNED_MEM_SIZE);
    metricService.remove(
        MetricType.TIMER, Metric.SCHEMA_ENGINE.toString(), Tag.NAME.toString(), RELEASE_TIMER);
    metricService.remove(
        MetricType.TIMER, Metric.SCHEMA_ENGINE.toString(), Tag.NAME.toString(), FLUSH_TIMER);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SCHEMA_ENGINE.toString(),
        Tag.NAME.toString(),
        RELEASE_THREAD_NUM);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SCHEMA_ENGINE.toString(),
        Tag.NAME.toString(),
        FLUSH_THREAD_NUM);
  }

  public void recordFlush(long milliseconds) {
    MetricService.getInstance()
        .timer(
            milliseconds,
            TimeUnit.MILLISECONDS,
            Metric.SCHEMA_ENGINE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            FLUSH_TIMER);
  }

  public void recordRelease(long milliseconds) {
    MetricService.getInstance()
        .timer(
            milliseconds,
            TimeUnit.MILLISECONDS,
            Metric.SCHEMA_ENGINE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RELEASE_TIMER);
  }
}
