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

package org.apache.iotdb.db.pipe.metric;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.progress.PipeEventCommitManager;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.pipe.extractor.dataregion.IoTDBDataRegionExtractor;
import org.apache.iotdb.db.pipe.extractor.schemaregion.IoTDBSchemaRegionExtractor;
import org.apache.iotdb.db.pipe.task.subtask.connector.PipeConnectorSubtask;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.storageengine.StorageEngine;
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

public class PipeDataNodeRemainingEventAndTimeMetrics implements IMetricSet {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeDataNodeRemainingEventAndTimeMetrics.class);

  private volatile AbstractMetricService metricService;

  private final Map<String, PipeDataNodeRemainingEventAndTimeOperator>
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
        PipeDataNodeRemainingEventAndTimeOperator::getRemainingEvents,
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
        Metric.PIPE_DATANODE_REMAINING_TIME.toString(),
        Tag.NAME.toString(),
        operator.getPipeName(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(operator.getCreationTime()));
    remainingEventAndTimeOperatorMap.remove(pipeID);
  }

  //////////////////////////// register & deregister (pipe integration) ////////////////////////////

  public void register(final IoTDBDataRegionExtractor extractor) {
    // The metric is global thus the regionId is omitted
    final String pipeID = extractor.getPipeName() + "_" + extractor.getCreationTime();
    remainingEventAndTimeOperatorMap
        .computeIfAbsent(pipeID, k -> new PipeDataNodeRemainingEventAndTimeOperator())
        .register(extractor);
    if (Objects.nonNull(metricService)) {
      createMetrics(pipeID);
    }
  }

  public void register(
      final PipeConnectorSubtask connectorSubtask, final String pipeName, final long creationTime) {
    // The metric is global thus the regionId is omitted
    final String pipeID = pipeName + "_" + creationTime;
    remainingEventAndTimeOperatorMap
        .computeIfAbsent(pipeID, k -> new PipeDataNodeRemainingEventAndTimeOperator())
        .register(connectorSubtask, pipeName, creationTime);
    if (Objects.nonNull(metricService)) {
      createMetrics(pipeID);
    }
  }

  public void register(final IoTDBSchemaRegionExtractor extractor) {
    // The metric is global thus the regionId is omitted
    final String pipeID = extractor.getPipeName() + "_" + extractor.getCreationTime();
    remainingEventAndTimeOperatorMap
        .computeIfAbsent(pipeID, k -> new PipeDataNodeRemainingEventAndTimeOperator())
        .register(extractor);
    if (Objects.nonNull(metricService)) {
      createMetrics(pipeID);
    }
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

  public void markRegionCommit(final PipeTaskRuntimeEnvironment pipeTaskRuntimeEnvironment) {
    // Filter commit attempt from assigner
    final String pipeName = pipeTaskRuntimeEnvironment.getPipeName();
    final int regionId = pipeTaskRuntimeEnvironment.getRegionId();
    final long creationTime = pipeTaskRuntimeEnvironment.getCreationTime();
    final String pipeID = pipeName + "_" + creationTime;

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
    // Prevent not set pipeName / creation times & potential differences between pipeNames and
    // creation times
    if (!Objects.equals(pipeName, operator.getPipeName())
        || !Objects.equals(creationTime, operator.getCreationTime())) {
      return;
    }

    // Prevent empty region-ids
    if (StorageEngine.getInstance().getAllDataRegionIds().contains(new DataRegionId(regionId))) {
      operator.markDataRegionCommit();
    }

    if (SchemaEngine.getInstance().getAllSchemaRegionIds().contains(new SchemaRegionId(regionId))) {
      operator.markSchemaRegionCommit();
    }
  }

  //////////////////////////// Show pipes ////////////////////////////

  public Pair<Long, Double> getRemainingEventAndTime(
      final String pipeName, final long creationTime) {
    final PipeDataNodeRemainingEventAndTimeOperator operator =
        remainingEventAndTimeOperatorMap.computeIfAbsent(
            pipeName + "_" + creationTime, k -> new PipeDataNodeRemainingEventAndTimeOperator());
    return new Pair<>(operator.getRemainingEvents(), operator.getRemainingTime());
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeDataNodeRemainingEventAndTimeMetricsHolder {

    private static final PipeDataNodeRemainingEventAndTimeMetrics INSTANCE =
        new PipeDataNodeRemainingEventAndTimeMetrics();

    private PipeDataNodeRemainingEventAndTimeMetricsHolder() {
      // Empty constructor
    }
  }

  public static PipeDataNodeRemainingEventAndTimeMetrics getInstance() {
    return PipeDataNodeRemainingEventAndTimeMetricsHolder.INSTANCE;
  }

  private PipeDataNodeRemainingEventAndTimeMetrics() {
    PipeEventCommitManager.getInstance().setCommitRateMarker(this::markRegionCommit);
  }
}
