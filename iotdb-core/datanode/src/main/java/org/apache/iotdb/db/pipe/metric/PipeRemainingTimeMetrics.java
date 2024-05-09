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

public class PipeRemainingTimeMetrics implements IMetricSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeRemainingTimeMetrics.class);

  private volatile AbstractMetricService metricService;

  private final Map<String, PipeRemainingTimeOperator> remainingTimeOperatorMap =
      new ConcurrentHashMap<>();

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    this.metricService = metricService;
    ImmutableSet.copyOf(remainingTimeOperatorMap.keySet()).forEach(this::createMetrics);
  }

  private void createMetrics(final String taskID) {
    createAutoGauge(taskID);
  }

  private void createAutoGauge(final String taskID) {
    final PipeRemainingTimeOperator operator = remainingTimeOperatorMap.get(taskID);
    metricService.createAutoGauge(
        Metric.PIPE_DATANODE_REMAINING_TIME.toString(),
        MetricLevel.IMPORTANT,
        operator,
        PipeRemainingTimeOperator::getRemainingTime,
        Tag.NAME.toString(),
        operator.getPipeName(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(operator.getCreationTime()));
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    ImmutableSet.copyOf(remainingTimeOperatorMap.keySet()).forEach(this::deregister);
    if (!remainingTimeOperatorMap.isEmpty()) {
      LOGGER.warn(
          "Failed to unbind from pipe schema region extractor metrics, extractor map not empty");
    }
  }

  private void removeMetrics(final String taskID) {
    removeAutoGauge(taskID);
  }

  private void removeAutoGauge(final String taskID) {
    final PipeRemainingTimeOperator operator = remainingTimeOperatorMap.get(taskID);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_DATANODE_REMAINING_TIME.toString(),
        Tag.NAME.toString(),
        operator.getPipeName(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(operator.getCreationTime()));
    remainingTimeOperatorMap.remove(taskID);
  }

  //////////////////////////// register & deregister (pipe integration) ////////////////////////////

  public void register(final IoTDBDataRegionExtractor extractor) {
    // The metric is global thus the regionId is omitted
    final String taskID = extractor.getPipeName() + "_" + extractor.getCreationTime();
    remainingTimeOperatorMap
        .computeIfAbsent(taskID, k -> new PipeRemainingTimeOperator())
        .register(extractor);
    if (Objects.nonNull(metricService)) {
      createMetrics(taskID);
    }
  }

  public void register(final PipeProcessorSubtask processorSubtask) {
    // The metric is global thus the regionId is omitted
    final String taskID = processorSubtask.getPipeName() + "_" + processorSubtask.getCreationTime();
    remainingTimeOperatorMap
        .computeIfAbsent(taskID, k -> new PipeRemainingTimeOperator())
        .register(processorSubtask);
    if (Objects.nonNull(metricService)) {
      createMetrics(taskID);
    }
  }

  public void register(
      final PipeConnectorSubtask connectorSubtask, final String pipeName, final long creationTime) {
    // The metric is global thus the regionId is omitted
    final String taskID = pipeName + "_" + creationTime;
    remainingTimeOperatorMap
        .computeIfAbsent(taskID, k -> new PipeRemainingTimeOperator())
        .register(connectorSubtask, pipeName, creationTime);
    if (Objects.nonNull(metricService)) {
      createMetrics(taskID);
    }
  }

  public void register(final IoTDBSchemaRegionExtractor extractor) {
    // The metric is global thus the regionId is omitted
    final String taskID = extractor.getPipeName() + "_" + extractor.getCreationTime();
    remainingTimeOperatorMap
        .computeIfAbsent(taskID, k -> new PipeRemainingTimeOperator())
        .register(extractor);
    if (Objects.nonNull(metricService)) {
      createMetrics(taskID);
    }
  }

  public void deregister(final String taskID) {
    if (!remainingTimeOperatorMap.containsKey(taskID)) {
      LOGGER.warn(
          "Failed to deregister pipe remain time metrics, RemainTimeOperator({}) does not exist",
          taskID);
      return;
    }
    if (Objects.nonNull(metricService)) {
      removeMetrics(taskID);
    }
    remainingTimeOperatorMap.remove(taskID);
  }

  public void markRegionCommit(final String committerKey) {
    final String[] params = committerKey.split("_");
    if (params.length != 3) {
      return;
    }
    final String pipeName = params[0];
    final int creationTime = Integer.parseInt(params[1]);
    final int regionId = Integer.parseInt(params[2]);
    final String taskID = params[0] + "_" + params[1];

    if (Objects.isNull(metricService)) {
      return;
    }
    final PipeRemainingTimeOperator operator = remainingTimeOperatorMap.get(taskID);
    if (Objects.isNull(operator)) {
      LOGGER.warn(
          "Failed to mark pipe processor tablet event, PipeProcessorSubtask({}) does not exist",
          taskID);
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

    private static final PipeRemainingTimeMetrics INSTANCE = new PipeRemainingTimeMetrics();

    private PipeRemainTimeMetricsHolder() {
      // Empty constructor
    }
  }

  public static PipeRemainingTimeMetrics getInstance() {
    return PipeRemainTimeMetricsHolder.INSTANCE;
  }

  private PipeRemainingTimeMetrics() {
    PipeEventCommitManager.getInstance().setCommitRateMarker(this::markRegionCommit);
  }
}
