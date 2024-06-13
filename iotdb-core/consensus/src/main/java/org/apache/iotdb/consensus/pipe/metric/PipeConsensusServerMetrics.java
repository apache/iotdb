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

package org.apache.iotdb.consensus.pipe.metric;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.consensus.pipe.PipeConsensusServerImpl;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class PipeConsensusServerMetrics implements IMetricSet {
  private final PipeConsensusServerImpl impl;
  private final PipeConsensusSyncLagManager syncLagManager;

  private Timer getStateMachineLockTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer userWriteStateMachineTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer replicaWriteStateMachineTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  public PipeConsensusServerMetrics(PipeConsensusServerImpl impl) {
    this.impl = impl;
    this.syncLagManager = PipeConsensusSyncLagManager.getInstance(impl.getConsensusGroupId());
  }

  private static final String IMPL = "PipeConsensusServerImpl";

  public void recordGetStateMachineLockTime(long costTimeInNanos) {
    getStateMachineLockTimer.updateNanos(costTimeInNanos);
  }

  public void recordUserWriteStateMachineTime(long costTimeInNanos) {
    userWriteStateMachineTimer.updateNanos(costTimeInNanos);
  }

  public void recordReplicaWriteStateMachineTime(long costTimeInNanos) {
    replicaWriteStateMachineTimer.updateNanos(costTimeInNanos);
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    bindAutoGauge(metricService);
    bindGauge(metricService);
    bindStageTimer(metricService);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    unbindAutoGauge(metricService);
    unbindGauge(metricService);
    unbindStageTimer(metricService);

    // release corresponding resource
    PipeConsensusSyncLagManager.release(impl.getConsensusGroupId());
  }

  public void bindGauge(AbstractMetricService metricService) {
    metricService
        .getOrCreateGauge(
            Metric.PIPE_CONSENSUS_MODE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            IMPL,
            Tag.TYPE.toString(),
            "replicateMode")
        .set(impl.getReplicateMode());
  }

  public void unbindGauge(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.GAUGE,
        Metric.PIPE_CONSENSUS_MODE.toString(),
        Tag.NAME.toString(),
        IMPL,
        Tag.TYPE.toString(),
        "replicateMode");
  }

  public void bindAutoGauge(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.PIPE_CONSENSUS.toString(),
        MetricLevel.IMPORTANT,
        syncLagManager,
        PipeConsensusSyncLagManager::calculateSyncLag,
        Tag.NAME.toString(),
        IMPL,
        Tag.REGION.toString(),
        impl.getConsensusGroupId(),
        Tag.TYPE.toString(),
        "syncLag");
  }

  public void unbindAutoGauge(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_CONSENSUS.toString(),
        Tag.NAME.toString(),
        IMPL,
        Tag.REGION.toString(),
        impl.getConsensusGroupId(),
        Tag.TYPE.toString(),
        "syncLag");
  }

  public void bindStageTimer(AbstractMetricService metricService) {
    getStateMachineLockTimer =
        metricService.getOrCreateTimer(
            Metric.STAGE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            Metric.PIPE_CONSENSUS.toString(),
            Tag.TYPE.toString(),
            "getStateMachineLock",
            Tag.REGION.toString(),
            impl.getConsensusGroupId());
    userWriteStateMachineTimer =
        metricService.getOrCreateTimer(
            Metric.STAGE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            Metric.PIPE_CONSENSUS.toString(),
            Tag.TYPE.toString(),
            "userWriteStateMachine",
            Tag.REGION.toString(),
            impl.getConsensusGroupId());
    replicaWriteStateMachineTimer =
        metricService.getOrCreateTimer(
            Metric.PIPE_RECEIVE_EVENT.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            Metric.PIPE_CONSENSUS.toString(),
            Tag.TYPE.toString(),
            "replicaWriteStateMachine",
            Tag.REGION.toString(),
            impl.getConsensusGroupId());
  }

  public void unbindStageTimer(AbstractMetricService metricService) {
    getStateMachineLockTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    userWriteStateMachineTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    replicaWriteStateMachineTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

    metricService.remove(
        MetricType.TIMER,
        Metric.STAGE.toString(),
        Tag.NAME.toString(),
        Metric.PIPE_CONSENSUS.toString(),
        Tag.TYPE.toString(),
        "getStateMachineLock",
        Tag.REGION.toString(),
        impl.getConsensusGroupId());
    metricService.remove(
        MetricType.TIMER,
        Metric.STAGE.toString(),
        Tag.NAME.toString(),
        Metric.PIPE_CONSENSUS.toString(),
        Tag.TYPE.toString(),
        "writeStateMachine",
        Tag.REGION.toString(),
        impl.getConsensusGroupId());
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_RECEIVE_EVENT.toString(),
        Tag.NAME.toString(),
        Metric.PIPE_CONSENSUS.toString(),
        Tag.TYPE.toString(),
        "replicaWriteStateMachine",
        Tag.REGION.toString(),
        impl.getConsensusGroupId());
  }
}
