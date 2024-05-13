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
import org.apache.iotdb.db.pipe.task.subtask.processor.PipeProcessorSubtask;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class PipeRemainingEventAndTimeMetrics implements IMetricSet {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeRemainingEventAndTimeMetrics.class);

  private volatile AbstractMetricService metricService;

  private final Map<String, PipeRemainingEventAndTimeOperator> remainingEventAndTimeOperatorMap =
      new ConcurrentHashMap<>();

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    this.metricService = metricService;
    ImmutableSet.copyOf(remainingEventAndTimeOperatorMap.keySet()).forEach(this::createMetrics);
  }

  private void createMetrics(final String taskID) {
    createAutoGauge(taskID);
  }

  private void createAutoGauge(final String taskID) {
    final PipeRemainingEventAndTimeOperator operator = remainingEventAndTimeOperatorMap.get(taskID);
    metricService.createAutoGauge(
        Metric.PIPE_DATANODE_REMAINING_EVENT_COUNT.toString(),
        MetricLevel.IMPORTANT,
        operator,
        PipeRemainingEventAndTimeOperator::getRemainingEvents,
        Tag.NAME.toString(),
        operator.getPipeName(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(operator.getCreationTime()));
    metricService.createAutoGauge(
        Metric.PIPE_DATANODE_REMAINING_TIME.toString(),
        MetricLevel.IMPORTANT,
        operator,
        PipeRemainingEventAndTimeOperator::getRemainingTime,
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
          "Failed to unbind from pipe remaining time metrics, remainingTimeOperator map not empty");
    }
  }

  private void removeMetrics(final String taskID) {
    removeAutoGauge(taskID);
  }

  private void removeAutoGauge(final String taskID) {
    final PipeRemainingEventAndTimeOperator operator = remainingEventAndTimeOperatorMap.get(taskID);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_DATANODE_REMAINING_TIME.toString(),
        Tag.NAME.toString(),
        operator.getPipeName(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(operator.getCreationTime()));
    remainingEventAndTimeOperatorMap.remove(taskID);
  }

  //////////////////////////// register & deregister (pipe integration) ////////////////////////////

  public void register(final IoTDBDataRegionExtractor extractor) {
    // The metric is global thus the regionId is omitted
    final String taskID = extractor.getPipeName() + "_" + extractor.getCreationTime();
    remainingEventAndTimeOperatorMap
        .computeIfAbsent(taskID, k -> new PipeRemainingEventAndTimeOperator())
        .register(extractor);
    if (Objects.nonNull(metricService)) {
      createMetrics(taskID);
    }
  }

  public void register(final PipeProcessorSubtask processorSubtask) {
    // The metric is global thus the regionId is omitted
    final String taskID = processorSubtask.getPipeName() + "_" + processorSubtask.getCreationTime();
    remainingEventAndTimeOperatorMap
        .computeIfAbsent(taskID, k -> new PipeRemainingEventAndTimeOperator())
        .register(processorSubtask);
    if (Objects.nonNull(metricService)) {
      createMetrics(taskID);
    }
  }

  public void register(
      final PipeConnectorSubtask connectorSubtask, final String pipeName, final long creationTime) {
    // The metric is global thus the regionId is omitted
    final String taskID = pipeName + "_" + creationTime;
    remainingEventAndTimeOperatorMap
        .computeIfAbsent(taskID, k -> new PipeRemainingEventAndTimeOperator())
        .register(connectorSubtask, pipeName, creationTime);
    if (Objects.nonNull(metricService)) {
      createMetrics(taskID);
    }
  }

  public void register(final IoTDBSchemaRegionExtractor extractor) {
    // The metric is global thus the regionId is omitted
    final String taskID = extractor.getPipeName() + "_" + extractor.getCreationTime();
    remainingEventAndTimeOperatorMap
        .computeIfAbsent(taskID, k -> new PipeRemainingEventAndTimeOperator())
        .register(extractor);
    if (Objects.nonNull(metricService)) {
      createMetrics(taskID);
    }
  }

  public void deregister(final String taskID) {
    if (!remainingEventAndTimeOperatorMap.containsKey(taskID)) {
      LOGGER.warn(
          "Failed to deregister pipe remaining time metrics, RemainingTimeOperator({}) does not exist",
          taskID);
      return;
    }
    if (Objects.nonNull(metricService)) {
      removeMetrics(taskID);
    }
    remainingEventAndTimeOperatorMap.remove(taskID);
  }

  public void markRegionCommit(final PipeTaskRuntimeEnvironment pipeTaskRuntimeEnvironment) {
    // Filter commit attempt from assigner
    final String pipeName = pipeTaskRuntimeEnvironment.getPipeName();
    final int regionId = pipeTaskRuntimeEnvironment.getRegionId();
    final long creationTime = pipeTaskRuntimeEnvironment.getCreationTime();
    final String taskID = pipeName + "_" + creationTime;

    if (Objects.isNull(metricService)) {
      return;
    }
    final PipeRemainingEventAndTimeOperator operator = remainingEventAndTimeOperatorMap.get(taskID);
    if (Objects.isNull(operator)) {
      LOGGER.warn(
          "Failed to mark pipe region commit, RemainingTimeOperator({}) does not exist", taskID);
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

  //////////////////////////// singleton ////////////////////////////

  private static class PipeRemainTimeMetricsHolder {

    private static final PipeRemainingEventAndTimeMetrics INSTANCE =
        new PipeRemainingEventAndTimeMetrics();

    private PipeRemainTimeMetricsHolder() {
      // Empty constructor
    }
  }

  public static PipeRemainingEventAndTimeMetrics getInstance() {
    return PipeRemainTimeMetricsHolder.INSTANCE;
  }

  private PipeRemainingEventAndTimeMetrics() {
    PipeEventCommitManager.getInstance().setCommitRateMarker(this::markRegionCommit);
  }
}
