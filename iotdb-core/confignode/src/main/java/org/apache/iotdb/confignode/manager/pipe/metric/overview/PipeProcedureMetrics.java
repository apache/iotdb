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

package org.apache.iotdb.confignode.manager.pipe.metric.overview;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PipeProcedureMetrics implements IMetricSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeProcedureMetrics.class);

  private final Map<String, Timer> timerMap = new HashMap<>();

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(AbstractMetricService metricService) {
    Arrays.stream(PipeTaskOperation.values())
        .forEach(
            op ->
                timerMap.put(
                    op.getName(),
                    metricService.getOrCreateTimer(
                        Metric.PIPE_PROCEDURE.toString(),
                        MetricLevel.IMPORTANT,
                        Tag.NAME.toString(),
                        op.getName())));
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    timerMap.forEach(
        (name, timer) ->
            metricService.remove(
                MetricType.TIMER, Metric.PIPE_PROCEDURE.toString(), Tag.NAME.toString(), name));
  }

  //////////////////////////// pipe integration ////////////////////////////

  public void updateTimer(String name, long durationMillis) {
    Timer timer = timerMap.get(name);
    if (timer == null) {
      LOGGER.warn("Failed to update pipe procedure timer, PipeProcedure({}) does not exist", name);
      return;
    }
    timer.updateMillis(durationMillis);
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeProcedureMetricsHolder {

    private static final PipeProcedureMetrics INSTANCE = new PipeProcedureMetrics();

    private PipeProcedureMetricsHolder() {
      // empty constructor
    }
  }

  public static PipeProcedureMetrics getInstance() {
    return PipeProcedureMetrics.PipeProcedureMetricsHolder.INSTANCE;
  }

  private PipeProcedureMetrics() {
    // empty constructor
  }
}
