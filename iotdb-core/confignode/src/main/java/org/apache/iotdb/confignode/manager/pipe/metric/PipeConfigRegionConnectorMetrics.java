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

package org.apache.iotdb.confignode.manager.pipe.metric;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.confignode.manager.pipe.execution.PipeConfigNodeSubtask;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PipeConfigRegionConnectorMetrics implements IMetricSet {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeConfigRegionConnectorMetrics.class);

  @SuppressWarnings("java:S3077")
  private volatile AbstractMetricService metricService;

  private final ConcurrentMap<String, PipeConfigNodeSubtask> subtaskMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Rate> configRateMap = new ConcurrentHashMap<>();

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    this.metricService = metricService;
    ImmutableSet.copyOf(subtaskMap.keySet()).forEach(this::createMetrics);
  }

  private void createMetrics(final String taskID) {
    createRate(taskID);
  }

  private void createRate(final String taskID) {
    final PipeConfigNodeSubtask subtask = subtaskMap.get(taskID);
    // Transfer event rate
    configRateMap.put(
        taskID,
        metricService.getOrCreateRate(
            Metric.PIPE_CONNECTOR_CONFIG_TRANSFER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            subtask.getPipeName(),
            Tag.CREATION_TIME.toString(),
            String.valueOf(subtask.getCreationTime())));
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    ImmutableSet.copyOf(subtaskMap.keySet()).forEach(this::deregister);
    if (!subtaskMap.isEmpty()) {
      LOGGER.warn(
          "Failed to unbind from pipe config region connector metrics, connector map not empty");
    }
  }

  private void removeMetrics(final String taskID) {
    removeRate(taskID);
  }

  private void removeRate(final String taskID) {
    final PipeConfigNodeSubtask subtask = subtaskMap.get(taskID);
    // Transfer event rate
    metricService.remove(
        MetricType.RATE,
        Metric.PIPE_CONNECTOR_CONFIG_TRANSFER.toString(),
        Tag.NAME.toString(),
        subtask.getPipeName(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(subtask.getCreationTime()));
    configRateMap.remove(taskID);
  }

  //////////////////////////// register & deregister (pipe integration) ////////////////////////////

  public void register(final PipeConfigNodeSubtask pipeConfigNodeSubtask) {
    final String taskID = pipeConfigNodeSubtask.getTaskID();
    subtaskMap.putIfAbsent(taskID, pipeConfigNodeSubtask);
    if (Objects.nonNull(metricService)) {
      createMetrics(taskID);
    }
  }

  public void deregister(final String taskID) {
    if (!subtaskMap.containsKey(taskID)) {
      LOGGER.warn(
          "Failed to deregister pipe config region connector metrics, PipeConfigNodeSubtask({}) does not exist",
          taskID);
      return;
    }
    if (Objects.nonNull(metricService)) {
      removeMetrics(taskID);
    }
    subtaskMap.remove(taskID);
  }

  public void markConfigEvent(final String taskID) {
    if (Objects.isNull(metricService)) {
      return;
    }
    final Rate rate = configRateMap.get(taskID);
    if (rate == null) {
      LOGGER.info(
          "Failed to mark pipe config region write plan event, PipeConfigNodeSubtask({}) does not exist",
          taskID);
      return;
    }
    rate.mark();
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeConfigNodeSubtaskMetricsHolder {

    private static final PipeConfigRegionConnectorMetrics INSTANCE =
        new PipeConfigRegionConnectorMetrics();

    private PipeConfigNodeSubtaskMetricsHolder() {
      // Empty constructor
    }
  }

  public static PipeConfigRegionConnectorMetrics getInstance() {
    return PipeConfigNodeSubtaskMetricsHolder.INSTANCE;
  }

  private PipeConfigRegionConnectorMetrics() {
    // Empty constructor
  }
}
