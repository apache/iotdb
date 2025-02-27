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

package org.apache.iotdb.db.schemaengine.metric;

import org.apache.iotdb.commons.memory.MemoryConfig;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.schemaengine.rescon.CachedSchemaEngineStatistics;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memcontrol.ReleaseFlushStrategySizeBasedImpl;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.ReleaseFlushMonitor;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class SchemaEngineCachedMetric implements ISchemaEngineMetric {

  private static final String RELEASE_THRESHOLD = "pbtree_release_threshold";
  private static final String PINNED_NODE_NUM = "pbtree_pinned_num";
  private static final String UNPINNED_NODE_NUM = "pbtree_unpinned_num";
  private static final String PINNED_MEM_SIZE = "pbtree_pinned_mem";
  private static final String UNPINNED_MEM_SIZE = "pbtree_unpinned_mem";
  private static final String RELEASE_FLUSH_THREAD_NUM = "pbtree_release_flush_thread_num";

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
            (MemoryConfig.getInstance().getSchemaRegionMemoryManager().getTotalMemorySizeInBytes()
                * ReleaseFlushStrategySizeBasedImpl.RELEASE_THRESHOLD_RATIO),
        Metric.SCHEMA_ENGINE.toString(),
        MetricLevel.IMPORTANT,
        Tag.NAME.toString(),
        RELEASE_THRESHOLD);
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
    metricService.createAutoGauge(
        Metric.SCHEMA_ENGINE.toString(),
        MetricLevel.IMPORTANT,
        ReleaseFlushMonitor.getInstance(),
        ReleaseFlushMonitor::getActiveWorkerNum,
        Tag.NAME.toString(),
        RELEASE_FLUSH_THREAD_NUM);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    schemaEngineMemMetric.unbindFrom(metricService);
    metricService.remove(
        MetricType.GAUGE, Metric.SCHEMA_ENGINE.toString(), Tag.NAME.toString(), RELEASE_THRESHOLD);
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
        MetricType.AUTO_GAUGE,
        Metric.SCHEMA_ENGINE.toString(),
        Tag.NAME.toString(),
        RELEASE_FLUSH_THREAD_NUM);
  }
}
