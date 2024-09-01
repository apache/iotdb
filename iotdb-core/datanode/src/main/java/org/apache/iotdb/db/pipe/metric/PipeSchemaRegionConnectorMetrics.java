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

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.pipe.task.subtask.connector.PipeConnectorSubtask;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PipeSchemaRegionConnectorMetrics implements IMetricSet {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeSchemaRegionConnectorMetrics.class);

  @SuppressWarnings("java:S3077")
  private volatile AbstractMetricService metricService;

  private final ConcurrentMap<String, PipeConnectorSubtask> connectorMap =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Rate> schemaRateMap = new ConcurrentHashMap<>();

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    this.metricService = metricService;
    ImmutableSet.copyOf(connectorMap.keySet()).forEach(this::createMetrics);
  }

  private void createMetrics(final String taskID) {
    createRate(taskID);
  }

  private void createRate(final String taskID) {
    final PipeConnectorSubtask connector = connectorMap.get(taskID);
    // Transfer event rate
    schemaRateMap.put(
        taskID,
        metricService.getOrCreateRate(
            Metric.PIPE_CONNECTOR_SCHEMA_TRANSFER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            connector.getAttributeSortedString(),
            Tag.CREATION_TIME.toString(),
            String.valueOf(connector.getCreationTime())));
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    ImmutableSet.copyOf(connectorMap.keySet()).forEach(this::deregister);
    if (!connectorMap.isEmpty()) {
      LOGGER.warn(
          "Failed to unbind from pipe schema region connector metrics, connector map not empty");
    }
  }

  private void removeMetrics(final String taskID) {
    removeRate(taskID);
  }

  private void removeRate(final String taskID) {
    final PipeConnectorSubtask connector = connectorMap.get(taskID);
    // Transfer event rate
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_CONNECTOR_SCHEMA_TRANSFER.toString(),
        Tag.NAME.toString(),
        connector.getAttributeSortedString(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(connector.getCreationTime()));
    schemaRateMap.remove(taskID);
  }

  //////////////////////////// Register & deregister (pipe integration) ////////////////////////////

  public void register(@NonNull final PipeConnectorSubtask pipeConnectorSubtask) {
    final String taskID = pipeConnectorSubtask.getTaskID();
    connectorMap.putIfAbsent(taskID, pipeConnectorSubtask);
    if (Objects.nonNull(metricService)) {
      createMetrics(taskID);
    }
  }

  public void deregister(final String taskID) {
    if (!connectorMap.containsKey(taskID)) {
      LOGGER.warn(
          "Failed to deregister pipe schema region connector metrics, PipeConnectorSubtask({}) does not exist",
          taskID);
      return;
    }
    if (Objects.nonNull(metricService)) {
      removeMetrics(taskID);
    }
    connectorMap.remove(taskID);
  }

  public void markSchemaEvent(final String taskID) {
    if (Objects.isNull(metricService)) {
      return;
    }
    final Rate rate = schemaRateMap.get(taskID);
    if (rate == null) {
      LOGGER.info(
          "Failed to mark pipe schema region write plan event, PipeConnectorSubtask({}) does not exist",
          taskID);
      return;
    }
    rate.mark();
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeSchemaRegionConnectorMetricsHolder {

    private static final PipeSchemaRegionConnectorMetrics INSTANCE =
        new PipeSchemaRegionConnectorMetrics();

    private PipeSchemaRegionConnectorMetricsHolder() {
      // Empty constructor
    }
  }

  public static PipeSchemaRegionConnectorMetrics getInstance() {
    return PipeSchemaRegionConnectorMetricsHolder.INSTANCE;
  }

  private PipeSchemaRegionConnectorMetrics() {
    // Empty constructor
  }
}
