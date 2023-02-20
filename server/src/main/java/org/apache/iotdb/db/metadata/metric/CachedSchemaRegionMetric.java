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

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.metadata.rescon.CachedSchemaRegionStatistics;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class CachedSchemaRegionMetric implements ISchemaRegionMetric {

  private final CachedSchemaRegionStatistics regionStatistics;
  private final String regionTagValue;

  private static final String PINNED_NUM = "schema_file_pinned_num";
  private static final String UNPINNED_NUM = "schema_file_unpinned_num";
  private static final String PINNED_MEM = "schema_file_pinned_mem";
  private static final String UNPINNED_MEM = "schema_file_unpinned_mem";
  private static final String BUFFER_NUM = "schema_file_buffer_node_num";
  private static final String CACHE_NUM = "schema_file_cache_node_num";
  private static final String MLOG_LENGTH = "schema_file_mlog_length";
  private static final String MLOG_CHECKPOINT = "schema_file_mlog_checkpoint";

  public CachedSchemaRegionMetric(CachedSchemaRegionStatistics regionStatistics) {
    this.regionStatistics = regionStatistics;
    this.regionTagValue = String.format("SchemaRegion[%d]", regionStatistics.getSchemaRegionId());
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.SCHEMA_REGION.toString(),
        MetricLevel.IMPORTANT,
        regionStatistics,
        CachedSchemaRegionStatistics::getPinnedMNodeNum,
        Tag.NAME.toString(),
        PINNED_NUM,
        Tag.REGION.toString(),
        regionTagValue);
    metricService.createAutoGauge(
        Metric.SCHEMA_REGION.toString(),
        MetricLevel.IMPORTANT,
        regionStatistics,
        CachedSchemaRegionStatistics::getUnpinnedMNodeNum,
        Tag.NAME.toString(),
        UNPINNED_NUM,
        Tag.REGION.toString(),
        regionTagValue);
    metricService.createAutoGauge(
        Metric.SCHEMA_REGION.toString(),
        MetricLevel.IMPORTANT,
        regionStatistics,
        CachedSchemaRegionStatistics::getPinnedMemorySize,
        Tag.NAME.toString(),
        PINNED_MEM,
        Tag.REGION.toString(),
        regionTagValue);
    metricService.createAutoGauge(
        Metric.SCHEMA_REGION.toString(),
        MetricLevel.IMPORTANT,
        regionStatistics,
        CachedSchemaRegionStatistics::getUnpinnedMemorySize,
        Tag.NAME.toString(),
        UNPINNED_MEM,
        Tag.REGION.toString(),
        regionTagValue);
    metricService.createAutoGauge(
        Metric.SCHEMA_REGION.toString(),
        MetricLevel.IMPORTANT,
        regionStatistics,
        CachedSchemaRegionStatistics::getBufferNodeNum,
        Tag.NAME.toString(),
        BUFFER_NUM,
        Tag.REGION.toString(),
        regionTagValue);
    metricService.createAutoGauge(
        Metric.SCHEMA_REGION.toString(),
        MetricLevel.IMPORTANT,
        regionStatistics,
        CachedSchemaRegionStatistics::getCacheNodeNum,
        Tag.NAME.toString(),
        CACHE_NUM,
        Tag.REGION.toString(),
        regionTagValue);
    metricService.createAutoGauge(
        Metric.SCHEMA_REGION.toString(),
        MetricLevel.IMPORTANT,
        regionStatistics,
        CachedSchemaRegionStatistics::getMLogLength,
        Tag.NAME.toString(),
        MLOG_LENGTH,
        Tag.REGION.toString(),
        regionTagValue);
    metricService.createAutoGauge(
        Metric.SCHEMA_REGION.toString(),
        MetricLevel.IMPORTANT,
        regionStatistics,
        CachedSchemaRegionStatistics::getMLogCheckPoint,
        Tag.NAME.toString(),
        MLOG_CHECKPOINT,
        Tag.REGION.toString(),
        regionTagValue);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SCHEMA_REGION.toString(),
        Tag.NAME.toString(),
        PINNED_NUM,
        Tag.REGION.toString(),
        regionTagValue);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SCHEMA_REGION.toString(),
        Tag.NAME.toString(),
        UNPINNED_NUM,
        Tag.REGION.toString(),
        regionTagValue);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SCHEMA_REGION.toString(),
        Tag.NAME.toString(),
        PINNED_MEM,
        Tag.REGION.toString(),
        regionTagValue);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SCHEMA_REGION.toString(),
        Tag.NAME.toString(),
        UNPINNED_MEM,
        Tag.REGION.toString(),
        regionTagValue);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SCHEMA_REGION.toString(),
        Tag.NAME.toString(),
        BUFFER_NUM,
        Tag.REGION.toString(),
        regionTagValue);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SCHEMA_REGION.toString(),
        Tag.NAME.toString(),
        CACHE_NUM,
        Tag.REGION.toString(),
        regionTagValue);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SCHEMA_REGION.toString(),
        Tag.NAME.toString(),
        MLOG_LENGTH,
        Tag.REGION.toString(),
        regionTagValue);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SCHEMA_REGION.toString(),
        Tag.NAME.toString(),
        MLOG_CHECKPOINT,
        Tag.REGION.toString(),
        regionTagValue);
  }
}
