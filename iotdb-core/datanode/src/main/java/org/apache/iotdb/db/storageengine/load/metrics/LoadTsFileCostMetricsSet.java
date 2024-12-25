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

package org.apache.iotdb.db.storageengine.load.metrics;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Arrays;

public class LoadTsFileCostMetricsSet implements IMetricSet {

  private static final LoadTsFileCostMetricsSet INSTANCE = new LoadTsFileCostMetricsSet();

  public static final String ANALYSIS = "analysis";
  public static final String FIRST_PHASE = "first_phase";
  public static final String SECOND_PHASE = "second_phase";
  public static final String LOAD_LOCALLY = "load_locally";

  private LoadTsFileCostMetricsSet() {
    // empty constructor
  }

  private Timer analyzerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer firstPhaseTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer secondPhaseTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer loadLocallyTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private Counter diskIOCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;

  public void recordPhaseTimeCost(String stage, long costTimeInNanos) {
    switch (stage) {
      case ANALYSIS:
        analyzerTimer.updateNanos(costTimeInNanos);
        break;
      case FIRST_PHASE:
        firstPhaseTimer.updateNanos(costTimeInNanos);
        break;
      case SECOND_PHASE:
        secondPhaseTimer.updateNanos(costTimeInNanos);
        break;
      case LOAD_LOCALLY:
        loadLocallyTimer.updateNanos(costTimeInNanos);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported stage: " + stage);
    }
  }

  public void recordDiskIO(long bytes) {
    diskIOCounter.inc(bytes);
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    analyzerTimer =
        metricService.getOrCreateTimer(
            Metric.LOAD_TIME_COST.toString(), MetricLevel.IMPORTANT, Tag.NAME.toString(), ANALYSIS);
    firstPhaseTimer =
        metricService.getOrCreateTimer(
            Metric.LOAD_TIME_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            FIRST_PHASE);
    secondPhaseTimer =
        metricService.getOrCreateTimer(
            Metric.LOAD_TIME_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            SECOND_PHASE);
    loadLocallyTimer =
        metricService.getOrCreateTimer(
            Metric.LOAD_TIME_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            LOAD_LOCALLY);

    diskIOCounter =
        metricService.getOrCreateCounter(
            Metric.LOAD_DISK_IO.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "DataNode " + IoTDBDescriptor.getInstance().getConfig().getDataNodeId());
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    Arrays.asList(ANALYSIS, FIRST_PHASE, SECOND_PHASE, LOAD_LOCALLY)
        .forEach(
            stage ->
                metricService.remove(
                    MetricType.TIMER,
                    Metric.LOAD_TIME_COST.toString(),
                    Tag.NAME.toString(),
                    stage));

    metricService.remove(
        MetricType.RATE,
        Metric.LOAD_DISK_IO.toString(),
        Tag.NAME.toString(),
        String.valueOf(IoTDBDescriptor.getInstance().getConfig().getDataNodeId()));
  }

  public static LoadTsFileCostMetricsSet getInstance() {
    return LoadTsFileCostMetricsSet.INSTANCE;
  }
}
