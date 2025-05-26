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

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.schemaengine.rescon.MemSchemaRegionStatistics;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.concurrent.TimeUnit;

public class SchemaRegionMemMetric implements ISchemaRegionMetric {

  private static final String MEM_USAGE = "schema_region_mem_usage";
  private static final String SERIES_CNT = "schema_region_series_cnt";
  private static final String NON_VIEW_SERIES_CNT = "schema_region_non_view_series_cnt";
  private static final String DEVICE_NUMBER = "schema_region_device_cnt";
  private static final String TEMPLATE_CNT = "activated_template_cnt";
  private static final String TEMPLATE_SERIES_CNT = "template_series_cnt";
  private static final String TRAVERSER_TIMER = "schema_region_traverser_timer";

  private Timer traverserTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private final MemSchemaRegionStatistics regionStatistics;
  private final String regionTagValue;
  private final String database;

  public SchemaRegionMemMetric(MemSchemaRegionStatistics regionStatistics, String database) {
    this.regionStatistics = regionStatistics;
    this.regionTagValue = String.format("SchemaRegion[%d]", regionStatistics.getSchemaRegionId());
    this.database = database;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.SCHEMA_REGION.toString(),
        MetricLevel.IMPORTANT,
        regionStatistics,
        MemSchemaRegionStatistics::getDevicesNumber,
        Tag.NAME.toString(),
        DEVICE_NUMBER,
        Tag.REGION.toString(),
        regionTagValue,
        Tag.DATABASE.toString(),
        database);
    metricService.createAutoGauge(
        Metric.SCHEMA_REGION.toString(),
        MetricLevel.IMPORTANT,
        regionStatistics,
        MemSchemaRegionStatistics::getRegionMemoryUsage,
        Tag.NAME.toString(),
        MEM_USAGE,
        Tag.REGION.toString(),
        regionTagValue,
        Tag.DATABASE.toString(),
        database);
    metricService.createAutoGauge(
        Metric.SCHEMA_REGION.toString(),
        MetricLevel.IMPORTANT,
        regionStatistics,
        i -> i.getSeriesNumber(true),
        Tag.NAME.toString(),
        SERIES_CNT,
        Tag.REGION.toString(),
        regionTagValue,
        Tag.DATABASE.toString(),
        database);
    metricService.createAutoGauge(
        Metric.SCHEMA_REGION.toString(),
        MetricLevel.IMPORTANT,
        regionStatistics,
        i -> i.getSeriesNumber(false),
        Tag.NAME.toString(),
        NON_VIEW_SERIES_CNT,
        Tag.REGION.toString(),
        regionTagValue,
        Tag.DATABASE.toString(),
        database);
    metricService.createAutoGauge(
        Metric.SCHEMA_REGION.toString(),
        MetricLevel.IMPORTANT,
        regionStatistics,
        MemSchemaRegionStatistics::getTemplateActivatedNumber,
        Tag.NAME.toString(),
        TEMPLATE_CNT,
        Tag.REGION.toString(),
        regionTagValue,
        Tag.DATABASE.toString(),
        database);
    metricService.createAutoGauge(
        Metric.SCHEMA_REGION.toString(),
        MetricLevel.IMPORTANT,
        regionStatistics,
        MemSchemaRegionStatistics::getTemplateSeriesNumber,
        Tag.NAME.toString(),
        TEMPLATE_SERIES_CNT,
        Tag.REGION.toString(),
        regionTagValue,
        Tag.DATABASE.toString(),
        database);
    traverserTimer =
        metricService.getOrCreateTimer(
            Metric.SCHEMA_REGION.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            TRAVERSER_TIMER,
            Tag.REGION.toString(),
            regionTagValue,
            Tag.DATABASE.toString(),
            database);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    traverserTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SCHEMA_REGION.toString(),
        Tag.NAME.toString(),
        DEVICE_NUMBER,
        Tag.REGION.toString(),
        regionTagValue,
        Tag.DATABASE.toString(),
        database);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SCHEMA_REGION.toString(),
        Tag.NAME.toString(),
        MEM_USAGE,
        Tag.REGION.toString(),
        regionTagValue,
        Tag.DATABASE.toString(),
        database);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SCHEMA_REGION.toString(),
        Tag.NAME.toString(),
        SERIES_CNT,
        Tag.REGION.toString(),
        regionTagValue,
        Tag.DATABASE.toString(),
        database);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SCHEMA_REGION.toString(),
        Tag.NAME.toString(),
        TEMPLATE_CNT,
        Tag.REGION.toString(),
        regionTagValue,
        Tag.DATABASE.toString(),
        database);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SCHEMA_REGION.toString(),
        Tag.NAME.toString(),
        TEMPLATE_SERIES_CNT,
        Tag.REGION.toString(),
        regionTagValue,
        Tag.DATABASE.toString(),
        database);
    metricService.remove(
        MetricType.TIMER,
        Metric.SCHEMA_REGION.toString(),
        Tag.NAME.toString(),
        TRAVERSER_TIMER,
        Tag.REGION.toString(),
        regionTagValue,
        Tag.DATABASE.toString(),
        database);
  }

  public void recordTraverser(long time) {
    traverserTimer.update(time, TimeUnit.MILLISECONDS);
  }
}
