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

package org.apache.iotdb.db.query.pool;

import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.db.service.metrics.enums.Metric;
import org.apache.iotdb.db.service.metrics.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Objects;

public class RawQueryReadTaskPoolManagerMetrics implements IMetricSet {
  private RawQueryReadTaskPoolManager rawQueryReadTaskPoolManager;

  public RawQueryReadTaskPoolManagerMetrics(
      RawQueryReadTaskPoolManager rawQueryReadTaskPoolManager) {
    this.rawQueryReadTaskPoolManager = rawQueryReadTaskPoolManager;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService.getOrCreateAutoGauge(
        Metric.QUEUE.toString(),
        MetricLevel.IMPORTANT,
        rawQueryReadTaskPoolManager,
        RawQueryReadTaskPoolManager::getActiveCount,
        Tag.NAME.toString(),
        ThreadName.SUB_RAW_QUERY_SERVICE.getName(),
        Tag.STATUS.toString(),
        "running");
    metricService.getOrCreateAutoGauge(
        Metric.QUEUE.toString(),
        MetricLevel.IMPORTANT,
        rawQueryReadTaskPoolManager,
        RawQueryReadTaskPoolManager::getWaitingCount,
        Tag.NAME.toString(),
        ThreadName.SUB_RAW_QUERY_SERVICE.getName(),
        Tag.STATUS.toString(),
        "waiting");
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.GAUGE,
        Metric.QUEUE.toString(),
        Tag.NAME.toString(),
        ThreadName.SUB_RAW_QUERY_SERVICE.getName(),
        Tag.STATUS.toString(),
        "running");
    metricService.remove(
        MetricType.GAUGE,
        Metric.QUEUE.toString(),
        Tag.NAME.toString(),
        ThreadName.SUB_RAW_QUERY_SERVICE.getName(),
        Tag.STATUS.toString(),
        "waiting");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RawQueryReadTaskPoolManagerMetrics that = (RawQueryReadTaskPoolManagerMetrics) o;
    return Objects.equals(rawQueryReadTaskPoolManager, that.rawQueryReadTaskPoolManager);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rawQueryReadTaskPoolManager);
  }
}
