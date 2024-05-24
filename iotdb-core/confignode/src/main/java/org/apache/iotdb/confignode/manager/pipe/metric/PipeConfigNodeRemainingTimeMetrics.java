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

import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.progress.PipeEventCommitManager;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.confignode.manager.pipe.extractor.IoTDBConfigRegionExtractor;
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

public class PipeConfigNodeRemainingTimeMetrics implements IMetricSet {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeConfigNodeRemainingTimeMetrics.class);

  private volatile AbstractMetricService metricService;

  private final Map<String, PipeConfigNodeRemainingTimeOperator> remainingTimeOperatorMap =
      new ConcurrentHashMap<>();

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    this.metricService = metricService;
    ImmutableSet.copyOf(remainingTimeOperatorMap.keySet()).forEach(this::createMetrics);
  }

  private void createMetrics(final String pipeID) {
    createAutoGauge(pipeID);
  }

  private void createAutoGauge(final String pipeID) {
    final PipeConfigNodeRemainingTimeOperator operator = remainingTimeOperatorMap.get(pipeID);
    metricService.createAutoGauge(
        Metric.PIPE_CONFIGNODE_REMAINING_TIME.toString(),
        MetricLevel.IMPORTANT,
        operator,
        PipeConfigNodeRemainingTimeOperator::getRemainingTime,
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
          "Failed to unbind from pipe remaining time metrics, RemainingTimeOperator map not empty");
    }
  }

  private void removeMetrics(final String pipeID) {
    removeAutoGauge(pipeID);
  }

  private void removeAutoGauge(final String pipeID) {
    final PipeConfigNodeRemainingTimeOperator operator = remainingTimeOperatorMap.get(pipeID);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_CONFIGNODE_REMAINING_TIME.toString(),
        Tag.NAME.toString(),
        operator.getPipeName(),
        Tag.CREATION_TIME.toString(),
        String.valueOf(operator.getCreationTime()));
    remainingTimeOperatorMap.remove(pipeID);
  }

  //////////////////////////// register & deregister (pipe integration) ////////////////////////////

  public void register(final IoTDBConfigRegionExtractor extractor) {
    // The metric is global thus the regionId is omitted
    final String pipeID = extractor.getPipeName() + "_" + extractor.getCreationTime();
    remainingTimeOperatorMap
        .computeIfAbsent(pipeID, k -> new PipeConfigNodeRemainingTimeOperator())
        .register(extractor);
    if (Objects.nonNull(metricService)) {
      createMetrics(pipeID);
    }
  }

  public void deregister(final String pipeID) {
    if (!remainingTimeOperatorMap.containsKey(pipeID)) {
      LOGGER.warn(
          "Failed to deregister pipe remaining time metrics, RemainingTimeOperator({}) does not exist",
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
    final long creationTime = pipeTaskRuntimeEnvironment.getCreationTime();
    final String pipeID = pipeName + "_" + creationTime;

    if (Objects.isNull(metricService)) {
      return;
    }
    final PipeConfigNodeRemainingTimeOperator operator = remainingTimeOperatorMap.get(pipeID);
    if (Objects.isNull(operator)) {
      LOGGER.warn(
          "Failed to mark pipe region commit, RemainingTimeOperator({}) does not exist", pipeID);
      return;
    }
    // Prevent not set pipeName / creation times & potential differences between pipeNames and
    // creation times
    if (!Objects.equals(pipeName, operator.getPipeName())
        || !Objects.equals(creationTime, operator.getCreationTime())) {
      return;
    }

    operator.markConfigRegionCommit();
  }

  //////////////////////////// Show pipes ////////////////////////////

  public double getRemainingTime(final String pipeName, final long creationTime) {
    return remainingTimeOperatorMap
        .computeIfAbsent(
            pipeName + "_" + creationTime, k -> new PipeConfigNodeRemainingTimeOperator())
        .getRemainingTime();
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeConfigNodeRemainingTimeMetricsHolder {

    private static final PipeConfigNodeRemainingTimeMetrics INSTANCE =
        new PipeConfigNodeRemainingTimeMetrics();

    private PipeConfigNodeRemainingTimeMetricsHolder() {
      // Empty constructor
    }
  }

  public static PipeConfigNodeRemainingTimeMetrics getInstance() {
    return PipeConfigNodeRemainingTimeMetricsHolder.INSTANCE;
  }

  private PipeConfigNodeRemainingTimeMetrics() {
    PipeEventCommitManager.getInstance().setCommitRateMarker(this::markRegionCommit);
  }
}
