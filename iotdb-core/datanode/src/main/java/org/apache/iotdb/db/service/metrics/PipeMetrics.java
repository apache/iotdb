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

package org.apache.iotdb.db.service.metrics;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.concurrent.TimeUnit;

public class PipeMetrics implements IMetricSet {

  private static final String EVENT = "event";
  private Timer eventTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private PipeMetrics() {}

  public static PipeMetrics getInstance() {
    return PipeMetricsInstanceHolder.INSTANCE;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    eventTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE.toString(), MetricLevel.IMPORTANT, Tag.TYPE.toString(), EVENT);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(MetricType.TIMER, Metric.PIPE.toString(), Tag.TYPE.toString(), EVENT);
  }

  public void recordEventCost(long costTimeInNanos) {
    eventTimer.update(costTimeInNanos, TimeUnit.NANOSECONDS);
  }

  private static class PipeMetricsInstanceHolder {
    private static final PipeMetrics INSTANCE = new PipeMetrics();

    private PipeMetricsInstanceHolder() {}
  }
}
