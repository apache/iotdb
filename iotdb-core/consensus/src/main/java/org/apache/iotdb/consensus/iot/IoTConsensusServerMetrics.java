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
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class IoTConsensusServerMetrics implements IMetricSet {
  private final IoTConsensusServerImpl impl;

  private Histogram getStateMachineLockHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram checkingBeforeWriteHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram writeStateMachineHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram offerRequestToQueueHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram consensusWriteHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;

  public IoTConsensusServerMetrics(IoTConsensusServerImpl impl) {
    this.impl = impl;
  }

  private static final String IMPL = "ioTConsensusServerImpl";

  @Override
  public void bindTo(AbstractMetricService metricService) {
    bindAutoGauge(metricService);
    bindStageHistogram(metricService);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    unbindAutoGauge(metricService);
    unbindStageHistogram(metricService);
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
        IoTConsensusServerImpl::getCurrentSafelyDeletedSearchIndex,
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
        IoTConsensusServerImpl::getLogEntriesFromWal,
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

  private void bindStageHistogram(AbstractMetricService metricService) {
    getStateMachineLockHistogram =
        metricService.getOrCreateHistogram(
            Metric.STAGE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            Metric.IOT_CONSENSUS.toString(),
            Tag.TYPE.toString(),
            "getStateMachineLock",
            Tag.REGION.toString(),
            impl.getConsensusGroupId());
    checkingBeforeWriteHistogram =
        metricService.getOrCreateHistogram(
            Metric.STAGE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            Metric.IOT_CONSENSUS.toString(),
            Tag.TYPE.toString(),
            "checkingBeforeWrite",
            Tag.REGION.toString(),
            impl.getConsensusGroupId());
    writeStateMachineHistogram =
        metricService.getOrCreateHistogram(
            Metric.STAGE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            Metric.IOT_CONSENSUS.toString(),
            Tag.TYPE.toString(),
            "writeStateMachine",
            Tag.REGION.toString(),
            impl.getConsensusGroupId());
    offerRequestToQueueHistogram =
        metricService.getOrCreateHistogram(
            Metric.STAGE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            Metric.IOT_CONSENSUS.toString(),
            Tag.TYPE.toString(),
            "offerRequestToQueue",
            Tag.REGION.toString(),
            impl.getConsensusGroupId());
    consensusWriteHistogram =
        metricService.getOrCreateHistogram(
            Metric.STAGE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            Metric.IOT_CONSENSUS.toString(),
            Tag.TYPE.toString(),
            "consensusWrite",
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

  private void unbindStageHistogram(AbstractMetricService metricService) {
    getStateMachineLockHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    checkingBeforeWriteHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    writeStateMachineHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    offerRequestToQueueHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    consensusWriteHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.STAGE.toString(),
        Tag.NAME.toString(),
        Metric.IOT_CONSENSUS.toString(),
        Tag.TYPE.toString(),
        "getStateMachineLock",
        Tag.REGION.toString(),
        impl.getConsensusGroupId());
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.STAGE.toString(),
        Tag.NAME.toString(),
        Metric.IOT_CONSENSUS.toString(),
        Tag.TYPE.toString(),
        "checkingBeforeWrite",
        Tag.REGION.toString(),
        impl.getConsensusGroupId());
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.STAGE.toString(),
        Tag.NAME.toString(),
        Metric.IOT_CONSENSUS.toString(),
        Tag.TYPE.toString(),
        "writeStateMachine",
        Tag.REGION.toString(),
        impl.getConsensusGroupId());
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.STAGE.toString(),
        Tag.NAME.toString(),
        Metric.IOT_CONSENSUS.toString(),
        Tag.TYPE.toString(),
        "offerRequestToQueue",
        Tag.REGION.toString(),
        impl.getConsensusGroupId());
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.STAGE.toString(),
        Tag.NAME.toString(),
        Metric.IOT_CONSENSUS.toString(),
        Tag.TYPE.toString(),
        "consensusWrite",
        Tag.REGION.toString(),
        impl.getConsensusGroupId());
  }

  public void recordGetStateMachineLockTime(long time) {
    getStateMachineLockHistogram.update(time);
  }

  public void recordCheckingBeforeWriteTime(long time) {
    checkingBeforeWriteHistogram.update(time);
  }

  public void recordWriteStateMachineTime(long time) {
    writeStateMachineHistogram.update(time);
  }

  public void recordOfferRequestToQueueTime(long time) {
    offerRequestToQueueHistogram.update(time);
  }

  public void recordConsensusWriteTime(long time) {
    consensusWriteHistogram.update(time);
  }
}
