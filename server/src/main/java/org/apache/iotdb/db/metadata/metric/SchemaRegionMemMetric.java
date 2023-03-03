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
import org.apache.iotdb.db.metadata.rescon.MemSchemaRegionStatistics;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class SchemaRegionMemMetric implements ISchemaRegionMetric {

  private static final String MEM_USAGE = "schema_region_mem_usage";
  private static final String SERIES_CNT = "schema_region_series_cnt";

  private final MemSchemaRegionStatistics regionStatistics;
  private final String regionTagValue;

  public SchemaRegionMemMetric(MemSchemaRegionStatistics regionStatistics) {
    this.regionStatistics = regionStatistics;
    this.regionTagValue = String.format("SchemaRegion[%d]", regionStatistics.getSchemaRegionId());
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.SCHEMA_REGION.toString(),
        MetricLevel.IMPORTANT,
        regionStatistics,
        MemSchemaRegionStatistics::getRegionMemoryUsage,
        Tag.NAME.toString(),
        MEM_USAGE,
        Tag.REGION.toString(),
        regionTagValue);
    metricService.createAutoGauge(
        Metric.SCHEMA_REGION.toString(),
        MetricLevel.IMPORTANT,
        regionStatistics,
        MemSchemaRegionStatistics::getSeriesNumber,
        Tag.NAME.toString(),
        SERIES_CNT,
        Tag.REGION.toString(),
        regionTagValue);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SCHEMA_REGION.toString(),
        Tag.NAME.toString(),
        MEM_USAGE,
        Tag.REGION.toString(),
        regionTagValue);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SCHEMA_REGION.toString(),
        Tag.NAME.toString(),
        SERIES_CNT,
        Tag.REGION.toString(),
        regionTagValue);
  }
}
