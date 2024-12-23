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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Objects;

public class TableDeviceSchemaCacheMetrics implements IMetricSet {
  private final TableDeviceSchemaCache tableDeviceSchemaCache;

  public TableDeviceSchemaCacheMetrics(final TableDeviceSchemaCache dataNodeSchemaCache) {
    this.tableDeviceSchemaCache = dataNodeSchemaCache;
  }

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.CACHE.toString(),
        MetricLevel.IMPORTANT,
        tableDeviceSchemaCache,
        TableDeviceSchemaCache::getHitCount,
        Tag.NAME.toString(),
        "SchemaCache",
        Tag.TYPE.toString(),
        "hit");
    metricService.createAutoGauge(
        Metric.CACHE.toString(),
        MetricLevel.IMPORTANT,
        tableDeviceSchemaCache,
        TableDeviceSchemaCache::getRequestCount,
        Tag.NAME.toString(),
        "SchemaCache",
        Tag.TYPE.toString(),
        "all");
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.CACHE.toString(),
        Tag.NAME.toString(),
        "SchemaCache",
        Tag.TYPE.toString(),
        "hit");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.CACHE.toString(),
        Tag.NAME.toString(),
        "SchemaCache",
        Tag.TYPE.toString(),
        "all");
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TableDeviceSchemaCacheMetrics that = (TableDeviceSchemaCacheMetrics) o;
    return Objects.equals(tableDeviceSchemaCache, that.tableDeviceSchemaCache);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableDeviceSchemaCache);
  }
}
