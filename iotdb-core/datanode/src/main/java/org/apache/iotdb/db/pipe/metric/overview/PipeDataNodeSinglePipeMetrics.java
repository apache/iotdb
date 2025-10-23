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

package org.apache.iotdb.db.pipe.metric.overview;

import org.apache.iotdb.commons.pipe.agent.task.progress.PipeEventCommitManager;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.source.dataregion.IoTDBDataRegionSource;
import org.apache.iotdb.db.pipe.source.schemaregion.IoTDBSchemaRegionSource;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class PipeDataNodeSinglePipeMetrics implements IMetricSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeDataNodeSinglePipeMetrics.class);

  @SuppressWarnings("java:S3077")
  private volatile AbstractMetricService metricService;

  public final Map<String, PipeDataNodeRemainingEventAndTimeOperator>
      remainingEventAndTimeOperatorMap = new ConcurrentHashMap<>();

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    this.metricService = metricService;
    ImmutableSet.copyOf(remainingEventAndTimeOperatorMap.keySet()).forEach(this::createMetrics);
  }

  private void createMetrics(final String pipeID) {
    createAutoGauge(pipeID);
  }

  private void createAutoGauge(final String pipeID) {
    final PipeDataNodeRemainingEventAndTimeOperator operator =
        remainingEventAndTimeOperatorMap.get(pipeID);
    metricService.createAutoGauge(
        Metric.PIPE_DATANODE_REMAINING_EVENT_COUNT.toString(),
        MetricLevel.IMPORTANT,
        operator,
        PipeDataNodeRemainingEventAndTimeOperator::getRemainingNonHeartbeatEvents,
        Tag.NAME.toString(),
        operator.getPipeName(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(operator.getCreationTime()));
    metricService.createAutoGauge(
        Metric.PIPE_DATANODE_REMAINING_TIME.toString(),
        MetricLevel.IMPORTANT,
        operator,
        PipeDataNodeRemainingEventAndTimeOperator::getRemainingTime,
        Tag.NAME.toString(),
        operator.getPipeName(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(operator.getCreationTime()));

    // Resources
    metricService.createAutoGauge(
        Metric.PIPE_FLOATING_MEMORY_USAGE.toString(),
        MetricLevel.IMPORTANT,
        PipeDataNodeAgent.task(),
        a -> a.getFloatingMemoryUsageInByte(operator.getPipeName()),
        Tag.NAME.toString(),
        operator.getPipeName(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(operator.getCreationTime()));
    metricService.createAutoGauge(
        Metric.PIPE_LINKED_TSFILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        PipeDataNodeResourceManager.tsfile(),
        a -> a.getLinkedTsFileCount(operator.getPipeName()),
        Tag.NAME.toString(),
        operator.getPipeName(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(operator.getCreationTime()));
    metricService.createAutoGauge(
        Metric.PIPE_LINKED_TSFILE_SIZE.toString(),
        MetricLevel.IMPORTANT,
        PipeDataNodeResourceManager.tsfile(),
        a -> a.getTotalLinkedTsFileSize(operator.getPipeName()),
        Tag.NAME.toString(),
        operator.getPipeName(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(operator.getCreationTime()));

    operator.setInsertNodeTransferTimer(
        metricService.getOrCreateTimer(
            Metric.PIPE_INSERT_NODE_EVENT_TRANSFER_TIME.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            operator.getPipeName()));

    operator.setTsFileTransferTimer(
        metricService.getOrCreateTimer(
            Metric.PIPE_TSFILE_EVENT_TRANSFER_TIME.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            operator.getPipeName()));
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    ImmutableSet.copyOf(remainingEventAndTimeOperatorMap.keySet()).forEach(this::deregister);
    if (!remainingEventAndTimeOperatorMap.isEmpty()) {
      LOGGER.warn(
          "Failed to unbind from pipe remaining event and time metrics, RemainingEventAndTimeOperator map not empty");
    }
  }

  private void removeMetrics(final String pipeID) {
    removeAutoGauge(pipeID);
  }

  private void removeAutoGauge(final String pipeID) {
    final PipeDataNodeRemainingEventAndTimeOperator operator =
        remainingEventAndTimeOperatorMap.get(pipeID);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_DATANODE_REMAINING_EVENT_COUNT.toString(),
        Tag.NAME.toString(),
        operator.getPipeName(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(operator.getCreationTime()));
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_DATANODE_REMAINING_TIME.toString(),
        Tag.NAME.toString(),
        operator.getPipeName(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(operator.getCreationTime()));
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_FLOATING_MEMORY_USAGE.toString(),
        Tag.NAME.toString(),
        operator.getPipeName(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(operator.getCreationTime()));
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_LINKED_TSFILE_COUNT.toString(),
        Tag.NAME.toString(),
        operator.getPipeName(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(operator.getCreationTime()));
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_LINKED_TSFILE_SIZE.toString(),
        Tag.NAME.toString(),
        operator.getPipeName(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(operator.getCreationTime()));
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_INSERT_NODE_EVENT_TRANSFER_TIME.toString(),
        Tag.NAME.toString(),
        operator.getPipeName());
    metricService.remove(
        MetricType.TIMER,
        Metric.PIPE_TSFILE_EVENT_TRANSFER_TIME.toString(),
        Tag.NAME.toString(),
        operator.getPipeName());
    remainingEventAndTimeOperatorMap.remove(pipeID);
  }

  //////////////////////////// register & deregister (pipe integration) ////////////////////////////

  public void register(final IoTDBDataRegionSource extractor) {
    // The metric is global thus the regionId is omitted
    final String pipeID = extractor.getPipeName() + "_" + extractor.getCreationTime();
    remainingEventAndTimeOperatorMap.computeIfAbsent(
        pipeID,
        k ->
            new PipeDataNodeRemainingEventAndTimeOperator(
                extractor.getPipeName(), extractor.getCreationTime()));
    if (Objects.nonNull(metricService)) {
      createMetrics(pipeID);
    }
  }

  public void register(final IoTDBSchemaRegionSource extractor) {
    // The metric is global thus the regionId is omitted
    final String pipeID = extractor.getPipeName() + "_" + extractor.getCreationTime();
    remainingEventAndTimeOperatorMap
        .computeIfAbsent(
            pipeID,
            k ->
                new PipeDataNodeRemainingEventAndTimeOperator(
                    extractor.getPipeName(), extractor.getCreationTime()))
        .register(extractor);
    if (Objects.nonNull(metricService)) {
      createMetrics(pipeID);
    }
  }

  public void increaseInsertNodeEventCount(final String pipeName, final long creationTime) {
    remainingEventAndTimeOperatorMap
        .computeIfAbsent(
            pipeName + "_" + creationTime,
            k -> new PipeDataNodeRemainingEventAndTimeOperator(pipeName, creationTime))
        .increaseInsertNodeEventCount();
  }

  public void decreaseInsertNodeEventCount(
      final String pipeName, final long creationTime, final long transferTime) {
    PipeDataNodeRemainingEventAndTimeOperator operator =
        remainingEventAndTimeOperatorMap.computeIfAbsent(
            pipeName + "_" + creationTime,
            k -> new PipeDataNodeRemainingEventAndTimeOperator(pipeName, creationTime));

    operator.decreaseInsertNodeEventCount();

    if (transferTime > 0) {
      operator.getInsertNodeTransferTimer().update(transferTime, TimeUnit.NANOSECONDS);
    }
  }

  public void updateInsertNodeTransferTimer(
      final String pipeName, final long creationTime, final long transferTime) {
    if (transferTime > 0) {
      remainingEventAndTimeOperatorMap
          .computeIfAbsent(
              pipeName + "_" + creationTime,
              k -> new PipeDataNodeRemainingEventAndTimeOperator(pipeName, creationTime))
          .getInsertNodeTransferTimer()
          .update(transferTime, TimeUnit.NANOSECONDS);
    }
  }

  public void increaseRawTabletEventCount(final String pipeName, final long creationTime) {
    remainingEventAndTimeOperatorMap
        .computeIfAbsent(
            pipeName + "_" + creationTime,
            k -> new PipeDataNodeRemainingEventAndTimeOperator(pipeName, creationTime))
        .increaseRawTabletEventCount();
  }

  public void decreaseRawTabletEventCount(final String pipeName, final long creationTime) {
    remainingEventAndTimeOperatorMap
        .computeIfAbsent(
            pipeName + "_" + creationTime,
            k -> new PipeDataNodeRemainingEventAndTimeOperator(pipeName, creationTime))
        .decreaseRawTabletEventCount();
  }

  public void increaseTsFileEventCount(final String pipeName, final long creationTime) {
    remainingEventAndTimeOperatorMap
        .computeIfAbsent(
            pipeName + "_" + creationTime,
            k -> new PipeDataNodeRemainingEventAndTimeOperator(pipeName, creationTime))
        .increaseTsFileEventCount();
  }

  public void decreaseTsFileEventCount(
      final String pipeName, final long creationTime, final long transferTime) {
    final PipeDataNodeRemainingEventAndTimeOperator operator =
        remainingEventAndTimeOperatorMap.computeIfAbsent(
            pipeName + "_" + creationTime,
            k -> new PipeDataNodeRemainingEventAndTimeOperator(pipeName, creationTime));

    operator.decreaseTsFileEventCount();

    if (transferTime > 0) {
      operator.getTsFileTransferTimer().update(transferTime, TimeUnit.NANOSECONDS);
    }
  }

  public void updateTsFileTransferTimer(
      final String pipeName, final long creationTime, final long transferTime) {
    if (transferTime > 0) {
      remainingEventAndTimeOperatorMap
          .computeIfAbsent(
              pipeName + "_" + creationTime,
              k -> new PipeDataNodeRemainingEventAndTimeOperator(pipeName, creationTime))
          .getTsFileTransferTimer()
          .update(transferTime, TimeUnit.NANOSECONDS);
    }
  }

  public void increaseHeartbeatEventCount(final String pipeName, final long creationTime) {
    remainingEventAndTimeOperatorMap
        .computeIfAbsent(
            pipeName + "_" + creationTime,
            k -> new PipeDataNodeRemainingEventAndTimeOperator(pipeName, creationTime))
        .increaseHeartbeatEventCount();
  }

  public void decreaseHeartbeatEventCount(final String pipeName, final long creationTime) {
    remainingEventAndTimeOperatorMap
        .computeIfAbsent(
            pipeName + "_" + creationTime,
            k -> new PipeDataNodeRemainingEventAndTimeOperator(pipeName, creationTime))
        .decreaseHeartbeatEventCount();
  }

  public void thawRate(final String pipeID) {
    if (!remainingEventAndTimeOperatorMap.containsKey(pipeID)) {
      // In dataNode, the "thawRate" may be called when there are no subtasks, and we call
      // "startPipe".
      // We thaw it later in "startPipeTask".
      return;
    }
    remainingEventAndTimeOperatorMap.get(pipeID).thawRate(true);
  }

  public void freezeRate(final String pipeID) {
    if (!remainingEventAndTimeOperatorMap.containsKey(pipeID)) {
      // In dataNode, the "freezeRate" may be called when there are no subtasks, and we call
      // "stopPipe" after calling "startPipe".
      // We do nothing because in that case the rate is not thawed initially
      return;
    }
    remainingEventAndTimeOperatorMap.get(pipeID).freezeRate(true);
  }

  public void deregister(final String pipeID) {
    if (!remainingEventAndTimeOperatorMap.containsKey(pipeID)) {
      LOGGER.warn(
          "Failed to deregister pipe remaining event and time metrics, RemainingEventAndTimeOperator({}) does not exist",
          pipeID);
      return;
    }
    if (Objects.nonNull(metricService)) {
      removeMetrics(pipeID);
    }
  }

  public void markRegionCommit(final String pipeID, final boolean isDataRegion) {
    if (Objects.isNull(metricService)) {
      return;
    }
    final PipeDataNodeRemainingEventAndTimeOperator operator =
        remainingEventAndTimeOperatorMap.get(pipeID);
    if (Objects.isNull(operator)) {
      LOGGER.warn(
          "Failed to mark pipe region commit, RemainingEventAndTimeOperator({}) does not exist",
          pipeID);
      return;
    }

    if (isDataRegion) {
      operator.markDataRegionCommit();
    } else {
      operator.markSchemaRegionCommit();
    }
  }

  public void markTsFileCollectInvocationCount(
      final String pipeID, final long collectInvocationCount) {
    if (Objects.isNull(metricService)) {
      return;
    }
    final PipeDataNodeRemainingEventAndTimeOperator operator =
        remainingEventAndTimeOperatorMap.get(pipeID);
    if (Objects.isNull(operator)) {
      return;
    }

    operator.markTsFileCollectInvocationCount(collectInvocationCount);
  }

  //////////////////////////// Show pipes ////////////////////////////

  public Pair<Long, Double> getRemainingEventAndTime(
      final String pipeName, final long creationTime) {
    final PipeDataNodeRemainingEventAndTimeOperator operator =
        remainingEventAndTimeOperatorMap.computeIfAbsent(
            pipeName + "_" + creationTime,
            k -> new PipeDataNodeRemainingEventAndTimeOperator(pipeName, creationTime));
    return new Pair<>(operator.getRemainingNonHeartbeatEvents(), operator.getRemainingTime());
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeDataNodeSinglePipeMetricsHolder {

    private static final PipeDataNodeSinglePipeMetrics INSTANCE =
        new PipeDataNodeSinglePipeMetrics();

    private PipeDataNodeSinglePipeMetricsHolder() {
      // Empty constructor
    }
  }

  public static PipeDataNodeSinglePipeMetrics getInstance() {
    return PipeDataNodeSinglePipeMetricsHolder.INSTANCE;
  }

  private PipeDataNodeSinglePipeMetrics() {
    PipeEventCommitManager.getInstance().setCommitRateMarker(this::markRegionCommit);
  }
}
