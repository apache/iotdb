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
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Arrays;

public class LoadTsFileMetricSet implements IMetricSet {

  public static final String LOAD_TSFILE_ANALYSIS_PHASE = "LoadTsFileAnalysisPhase";
  public static final String LOAD_TSFILE_SPLIT_PHASE = "LoadTsFileSplitPhase";
  public static final String LOAD_TSFILE_DISPATCH_FIRST_PHASE = "LoadTsFileDispatchFirstPhase";
  public static final String LOAD_TSFILE_DISPATCH_SECOND_PHASE = "LoadTsFileDispatchSecondPhase";
  public static final String LOAD_TSFILE_WRITE_PHASE = "LoadTsFileWritePhase";

  private Timer analysisPhaseTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer splitPhaseTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer dispatchFirstPhaseTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer dispatchSecondPhaseTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer writePhaseTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  @Override
  public void bindTo(AbstractMetricService metricService) {
    analysisPhaseTimer =
        metricService.getOrCreateTimer(
            Metric.LOAD_TIME_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_TSFILE_ANALYSIS_PHASE);
    splitPhaseTimer =
        metricService.getOrCreateTimer(
            Metric.LOAD_TIME_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_TSFILE_SPLIT_PHASE);
    dispatchFirstPhaseTimer =
        metricService.getOrCreateTimer(
            Metric.LOAD_TIME_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_TSFILE_DISPATCH_FIRST_PHASE);
    dispatchSecondPhaseTimer =
        metricService.getOrCreateTimer(
            Metric.LOAD_TIME_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_TSFILE_DISPATCH_SECOND_PHASE);
    writePhaseTimer =
        metricService.getOrCreateTimer(
            Metric.LOAD_TIME_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_TSFILE_WRITE_PHASE);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    analysisPhaseTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    splitPhaseTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    dispatchFirstPhaseTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    dispatchSecondPhaseTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    writePhaseTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    Arrays.asList(
            LOAD_TSFILE_ANALYSIS_PHASE,
            LOAD_TSFILE_SPLIT_PHASE,
            LOAD_TSFILE_DISPATCH_FIRST_PHASE,
            LOAD_TSFILE_DISPATCH_SECOND_PHASE,
            LOAD_TSFILE_WRITE_PHASE)
        .forEach(
            stage ->
                metricService.remove(
                    MetricType.TIMER,
                    Metric.LOAD_TIME_COST.toString(),
                    Tag.STAGE.toString(),
                    stage));
  }

  public void recordLoadTsFileTimeCost(String stage, long costTimeInNanos) {
    switch (stage) {
      case LOAD_TSFILE_ANALYSIS_PHASE:
        analysisPhaseTimer.updateNanos(costTimeInNanos);
        break;
      case LOAD_TSFILE_SPLIT_PHASE:
        splitPhaseTimer.updateNanos(costTimeInNanos);
        break;
      case LOAD_TSFILE_DISPATCH_FIRST_PHASE:
        dispatchFirstPhaseTimer.updateNanos(costTimeInNanos);
        break;
      case LOAD_TSFILE_DISPATCH_SECOND_PHASE:
        dispatchSecondPhaseTimer.updateNanos(costTimeInNanos);
        break;
      case LOAD_TSFILE_WRITE_PHASE:
        writePhaseTimer.updateNanos(costTimeInNanos);
        break;
      default:
        break;
    }
  }

  //////////////////////////// singleton ////////////////////////////

  private static class LoadTsFileMetricSetHolder {

    private static final LoadTsFileMetricSet INSTANCE = new LoadTsFileMetricSet();

    private LoadTsFileMetricSetHolder() {
      // empty constructor
    }
  }

  public static LoadTsFileMetricSet getInstance() {
    return LoadTsFileMetricSet.LoadTsFileMetricSetHolder.INSTANCE;
  }

  private LoadTsFileMetricSet() {
    // empty constructor
  }
}
