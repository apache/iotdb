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

package org.apache.iotdb.commons.pipe.metric;

import org.apache.iotdb.commons.pipe.agent.task.progress.PipeEventCommitter;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
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

public class PipeEventCommitMetrics implements IMetricSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeEventCommitMetrics.class);

  private volatile AbstractMetricService metricService;

  private final Map<String, PipeEventCommitter> eventCommitterMap = new ConcurrentHashMap<>();

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    this.metricService = metricService;
    final ImmutableSet<String> committerKeys = ImmutableSet.copyOf(eventCommitterMap.keySet());
    for (String committerKey : committerKeys) {
      createMetrics(committerKey);
    }
  }

  private void createMetrics(final String committerKey) {
    createAutoGauge(committerKey);
  }

  private void createAutoGauge(final String committerKey) {
    PipeEventCommitter eventCommitter = eventCommitterMap.get(committerKey);
    metricService.createAutoGauge(
        Metric.PIPE_EVENT_COMMIT_QUEUE_SIZE.toString(),
        MetricLevel.IMPORTANT,
        eventCommitter,
        PipeEventCommitter::commitQueueSize,
        Tag.NAME.toString(),
        String.valueOf(eventCommitter.getPipeName()),
        Tag.REGION.toString(),
        String.valueOf(eventCommitter.getRegionId()));
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    final ImmutableSet<String> committerKeys = ImmutableSet.copyOf(eventCommitterMap.keySet());
    for (String committerKey : committerKeys) {
      deregister(committerKey);
    }
    if (!eventCommitterMap.isEmpty()) {
      LOGGER.warn("Failed to unbind from pipe event commit metrics, event committer map not empty");
    }
  }

  private void removeMetrics(final String committerKey) {
    removeAutoGauge(committerKey);
  }

  private void removeAutoGauge(final String committerKey) {
    final PipeEventCommitter eventCommitter = eventCommitterMap.get(committerKey);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_EVENT_COMMIT_QUEUE_SIZE.toString(),
        Tag.NAME.toString(),
        String.valueOf(eventCommitter.getPipeName()),
        Tag.REGION.toString(),
        String.valueOf(eventCommitter.getRegionId()));
  }

  //////////////////////////// register & deregister (pipe integration) ////////////////////////////

  public void register(final PipeEventCommitter eventCommitter, final String committerKey) {
    if (Objects.isNull(eventCommitter)) {
      return;
    }

    eventCommitterMap.putIfAbsent(committerKey, eventCommitter);
    if (Objects.nonNull(metricService)) {
      createMetrics(committerKey);
    }
  }

  public void deregister(final String committerKey) {
    if (!eventCommitterMap.containsKey(committerKey)) {
      LOGGER.warn(
          "Failed to deregister pipe event commit metrics, PipeEventCommitter({}) does not exist",
          committerKey);
      return;
    }
    if (Objects.nonNull(metricService) && Objects.nonNull(committerKey)) {
      removeMetrics(committerKey);
    }
    eventCommitterMap.remove(committerKey);
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeEventCommitMetricsHolder {

    private static final PipeEventCommitMetrics INSTANCE = new PipeEventCommitMetrics();

    private PipeEventCommitMetricsHolder() {
      // Empty constructor
    }
  }

  public static PipeEventCommitMetrics getInstance() {
    return PipeEventCommitMetrics.PipeEventCommitMetricsHolder.INSTANCE;
  }

  private PipeEventCommitMetrics() {
    // Empty constructor
  }
}
