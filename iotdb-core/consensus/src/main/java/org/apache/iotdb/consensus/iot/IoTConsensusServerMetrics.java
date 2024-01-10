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

package org.apache.iotdb.consensus.iot;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class IoTConsensusServerMetrics implements IMetricSet {
  private final IoTConsensusServerImpl impl;

  private Timer getStateMachineLockTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer checkingBeforeWriteTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer writeStateMachineTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer offerRequestToQueueTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer consensusWriteTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private static final String IOT_RECEIVE_LOG = Metric.IOT_RECEIVE_LOG.toString();
  private Timer deserializeTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer sortTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer applyTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private static final String DESERIALIZE = "deserialize";
  private static final String SORT = "sort";
  private static final String APPLY = "apply";

  public IoTConsensusServerMetrics(IoTConsensusServerImpl impl) {
    this.impl = impl;
  }

  private static final String IMPL = "ioTConsensusServerImpl";

  public void recordDeserializeCost(long costTimeInNanos) {
    deserializeTimer.updateNanos(costTimeInNanos);
  }

  public void recordSortCost(long costTimeInNanos) {
    sortTimer.updateNanos(costTimeInNanos);
  }

  public void recordApplyCost(long costTimeInNanos) {
    applyTimer.updateNanos(costTimeInNanos);
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    bindAutoGauge(metricService);
    bindStageTimer(metricService);
    bindSyncLogTimer(metricService);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    unbindAutoGauge(metricService);
    unbindStageTimer(metricService);
    unbindSyncLogTimer(metricService);
  }

  private void bindAutoGauge(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.IOT_CONSENSUS.toString(),
        MetricLevel.IMPORTANT,
        impl,
        IoTConsensusServerImpl::getSearchIndex,
        Tag.NAME.toString(),
        IMPL,
        Tag.REGION.toString(),
        impl.getThisNode().getGroupId().toString(),
        Tag.TYPE.toString(),
        "searchIndex");
    metricService.createAutoGauge(
        Metric.IOT_CONSENSUS.toString(),
        MetricLevel.IMPORTANT,
        impl,
        IoTConsensusServerImpl::getMinSyncIndex,
        Tag.NAME.toString(),
        IMPL,
        Tag.REGION.toString(),
        impl.getThisNode().getGroupId().toString(),
        Tag.TYPE.toString(),
        "safeIndex");
    // TODO: Consider adding topological order to the traversal of metricEntry.
    metricService.createAutoGauge(
        Metric.IOT_CONSENSUS.toString(),
        MetricLevel.IMPORTANT,
        impl,
        IoTConsensusServerImpl::getSyncLag,
        Tag.NAME.toString(),
        IMPL,
        Tag.REGION.toString(),
        impl.getThisNode().getGroupId().toString(),
        Tag.TYPE.toString(),
        "syncLag");
    metricService.createAutoGauge(
        Metric.IOT_CONSENSUS.toString(),
        MetricLevel.IMPORTANT,
        impl,
        IoTConsensusServerImpl::getLogEntriesFromWAL,
        Tag.NAME.toString(),
        IMPL,
        Tag.REGION.toString(),
        impl.getThisNode().getGroupId().toString(),
        Tag.TYPE.toString(),
        "LogEntriesFromWAL");
    metricService.createAutoGauge(
        Metric.IOT_CONSENSUS.toString(),
        MetricLevel.IMPORTANT,
        impl,
        IoTConsensusServerImpl::getLogEntriesFromQueue,
        Tag.NAME.toString(),
        IMPL,
        Tag.REGION.toString(),
        impl.getThisNode().getGroupId().toString(),
        Tag.TYPE.toString(),
        "LogEntriesFromQueue");
  }

  private void bindStageTimer(AbstractMetricService metricService) {
    getStateMachineLockTimer =
        metricService.getOrCreateTimer(
            Metric.STAGE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            Metric.IOT_CONSENSUS.toString(),
            Tag.TYPE.toString(),
            "getStateMachineLock",
            Tag.REGION.toString(),
            impl.getConsensusGroupId());
    checkingBeforeWriteTimer =
        metricService.getOrCreateTimer(
            Metric.STAGE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            Metric.IOT_CONSENSUS.toString(),
            Tag.TYPE.toString(),
            "checkingBeforeWrite",
            Tag.REGION.toString(),
            impl.getConsensusGroupId());
    writeStateMachineTimer =
        metricService.getOrCreateTimer(
            Metric.STAGE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            Metric.IOT_CONSENSUS.toString(),
            Tag.TYPE.toString(),
            "writeStateMachine",
            Tag.REGION.toString(),
            impl.getConsensusGroupId());
    offerRequestToQueueTimer =
        metricService.getOrCreateTimer(
            Metric.STAGE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            Metric.IOT_CONSENSUS.toString(),
            Tag.TYPE.toString(),
            "offerRequestToQueue",
            Tag.REGION.toString(),
            impl.getConsensusGroupId());
    consensusWriteTimer =
        metricService.getOrCreateTimer(
            Metric.STAGE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            Metric.IOT_CONSENSUS.toString(),
            Tag.TYPE.toString(),
            "consensusWrite",
            Tag.REGION.toString(),
            impl.getConsensusGroupId());
  }

  private void bindSyncLogTimer(AbstractMetricService metricService) {
    // bind sync log timers
    deserializeTimer =
        metricService.getOrCreateTimer(
            IOT_RECEIVE_LOG,
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            DESERIALIZE,
            Tag.REGION.toString(),
            impl.getConsensusGroupId());
    sortTimer =
        metricService.getOrCreateTimer(
            IOT_RECEIVE_LOG,
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            SORT,
            Tag.REGION.toString(),
            impl.getConsensusGroupId());
    applyTimer =
        metricService.getOrCreateTimer(
            IOT_RECEIVE_LOG,
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            APPLY,
            Tag.REGION.toString(),
            impl.getConsensusGroupId());
  }

  private void unbindAutoGauge(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.IOT_CONSENSUS.toString(),
        Tag.NAME.toString(),
        IMPL,
        Tag.REGION.toString(),
        impl.getThisNode().getGroupId().toString(),
        Tag.TYPE.toString(),
        "searchIndex");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.IOT_CONSENSUS.toString(),
        Tag.NAME.toString(),
        IMPL,
        Tag.REGION.toString(),
        impl.getThisNode().getGroupId().toString(),
        Tag.TYPE.toString(),
        "safeIndex");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.IOT_CONSENSUS.toString(),
        Tag.NAME.toString(),
        IMPL,
        Tag.REGION.toString(),
        impl.getThisNode().getGroupId().toString(),
        Tag.TYPE.toString(),
        "syncLag");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.IOT_CONSENSUS.toString(),
        Tag.NAME.toString(),
        IMPL,
        Tag.REGION.toString(),
        impl.getThisNode().getGroupId().toString(),
        Tag.TYPE.toString(),
        "LogEntriesFromWAL");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.IOT_CONSENSUS.toString(),
        Tag.NAME.toString(),
        IMPL,
        Tag.REGION.toString(),
        impl.getThisNode().getGroupId().toString(),
        Tag.TYPE.toString(),
        "LogEntriesFromQueue");
  }

  private void unbindStageTimer(AbstractMetricService metricService) {
    getStateMachineLockTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    checkingBeforeWriteTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    writeStateMachineTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    offerRequestToQueueTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    consensusWriteTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    metricService.remove(
        MetricType.TIMER,
        Metric.STAGE.toString(),
        Tag.NAME.toString(),
        Metric.IOT_CONSENSUS.toString(),
        Tag.TYPE.toString(),
        "getStateMachineLock",
        Tag.REGION.toString(),
        impl.getConsensusGroupId());
    metricService.remove(
        MetricType.TIMER,
        Metric.STAGE.toString(),
        Tag.NAME.toString(),
        Metric.IOT_CONSENSUS.toString(),
        Tag.TYPE.toString(),
        "checkingBeforeWrite",
        Tag.REGION.toString(),
        impl.getConsensusGroupId());
    metricService.remove(
        MetricType.TIMER,
        Metric.STAGE.toString(),
        Tag.NAME.toString(),
        Metric.IOT_CONSENSUS.toString(),
        Tag.TYPE.toString(),
        "writeStateMachine",
        Tag.REGION.toString(),
        impl.getConsensusGroupId());
    metricService.remove(
        MetricType.TIMER,
        Metric.STAGE.toString(),
        Tag.NAME.toString(),
        Metric.IOT_CONSENSUS.toString(),
        Tag.TYPE.toString(),
        "offerRequestToQueue",
        Tag.REGION.toString(),
        impl.getConsensusGroupId());
    metricService.remove(
        MetricType.TIMER,
        Metric.STAGE.toString(),
        Tag.NAME.toString(),
        Metric.IOT_CONSENSUS.toString(),
        Tag.TYPE.toString(),
        "consensusWrite",
        Tag.REGION.toString(),
        impl.getConsensusGroupId());
  }

  private void unbindSyncLogTimer(AbstractMetricService metricService) {
    // unbind sync log timers
    metricService.remove(
        MetricType.TIMER,
        IOT_RECEIVE_LOG,
        Tag.STAGE.toString(),
        DESERIALIZE,
        Tag.REGION.toString(),
        impl.getConsensusGroupId());
    metricService.remove(
        MetricType.TIMER,
        IOT_RECEIVE_LOG,
        Tag.STAGE.toString(),
        SORT,
        Tag.REGION.toString(),
        impl.getConsensusGroupId());
    metricService.remove(
        MetricType.TIMER,
        IOT_RECEIVE_LOG,
        Tag.STAGE.toString(),
        APPLY,
        Tag.REGION.toString(),
        impl.getConsensusGroupId());
  }

  public void recordGetStateMachineLockTime(long costTimeInNanos) {
    getStateMachineLockTimer.updateNanos(costTimeInNanos);
  }

  public void recordCheckingBeforeWriteTime(long costTimeInNanos) {
    checkingBeforeWriteTimer.updateNanos(costTimeInNanos);
  }

  public void recordWriteStateMachineTime(long costTimeInNanos) {
    writeStateMachineTimer.updateNanos(costTimeInNanos);
  }

  public void recordOfferRequestToQueueTime(long costTimeInNanos) {
    offerRequestToQueueTimer.updateNanos(costTimeInNanos);
  }

  public void recordConsensusWriteTime(long costTimeInNanos) {
    consensusWriteTimer.updateNanos(costTimeInNanos);
  }
}
