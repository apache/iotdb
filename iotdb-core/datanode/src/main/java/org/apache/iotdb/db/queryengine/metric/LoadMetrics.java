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

package org.apache.iotdb.db.queryengine.metric;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadMetrics implements IMetricSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadMetrics.class);

  private Counter loadWriteCounter;

  //////////////////////////// bindTo & unbindFrom ////////////////////////////

  @Override
  public void bindTo(AbstractMetricService metricService) {
    loadWriteCounter =
        metricService.getOrCreateCounter(
            Metric.LOAD_WRITE_POINT_COUNTER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            Metric.LOAD_WRITE_POINT_COUNTER.toString());
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    // do nothing
  }

  public Counter getLoadWriteCounter() {
    return loadWriteCounter;
  }

  //////////////////////////// singleton ////////////////////////////

  private static class LoadMetricsHolder {

    private static final LoadMetrics INSTANCE = new LoadMetrics();

    private LoadMetricsHolder() {
      // empty constructor
    }
  }

  public static LoadMetrics getInstance() {
    return LoadMetrics.LoadMetricsHolder.INSTANCE;
  }
}
