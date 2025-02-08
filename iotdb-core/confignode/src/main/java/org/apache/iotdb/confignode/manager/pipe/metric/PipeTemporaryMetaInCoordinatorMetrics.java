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

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTemporaryMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTemporaryMetaInCoordinator;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The {@link PipeTemporaryMetaInCoordinatorMetrics} is to calculate the pipe-statistics from the
 * {@link PipeTemporaryMeta}. The class is lock-free and can only read from the thread-safe
 * variables from the {@link PipeTemporaryMeta}.
 */
public class PipeTemporaryMetaInCoordinatorMetrics implements IMetricSet {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeTemporaryMetaInCoordinatorMetrics.class);

  @SuppressWarnings("java:S3077")
  private volatile AbstractMetricService metricService;

  private final Map<String, PipeTemporaryMetaInCoordinator> pipeTemporaryMetaMap =
      new ConcurrentHashMap<>();

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    this.metricService = metricService;
    ImmutableSet.copyOf(pipeTemporaryMetaMap.keySet()).forEach(this::createMetrics);
  }

  private void createMetrics(final String pipeID) {
    createAutoGauge(pipeID);
  }

  private void createAutoGauge(final String pipeID) {
    final PipeTemporaryMetaInCoordinator pipeTemporaryMeta = pipeTemporaryMetaMap.get(pipeID);
    final String[] pipeNameAndCreationTime = pipeID.split("_");
    metricService.createAutoGauge(
        Metric.PIPE_GLOBAL_REMAINING_EVENT_COUNT.toString(),
        MetricLevel.IMPORTANT,
        pipeTemporaryMeta,
        PipeTemporaryMetaInCoordinator::getGlobalRemainingEvents,
        Tag.NAME.toString(),
        pipeNameAndCreationTime[0],
        Tag.CREATION_TIME.toString(),
        pipeNameAndCreationTime[1]);
    metricService.createAutoGauge(
        Metric.PIPE_GLOBAL_REMAINING_TIME.toString(),
        MetricLevel.IMPORTANT,
        pipeTemporaryMeta,
        PipeTemporaryMetaInCoordinator::getGlobalRemainingTime,
        Tag.NAME.toString(),
        pipeNameAndCreationTime[0],
        Tag.CREATION_TIME.toString(),
        pipeNameAndCreationTime[1]);
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    ImmutableSet.copyOf(pipeTemporaryMetaMap.keySet()).forEach(this::deregister);
    if (!pipeTemporaryMetaMap.isEmpty()) {
      LOGGER.warn(
          "Failed to unbind from pipe temporary meta metrics, PipeTemporaryMeta map not empty");
    }
  }

  private void removeMetrics(final String pipeID) {
    removeAutoGauge(pipeID);
  }

  private void removeAutoGauge(final String pipeID) {
    final String[] pipeNameAndCreationTime = pipeID.split("_");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_GLOBAL_REMAINING_EVENT_COUNT.toString(),
        Tag.NAME.toString(),
        pipeNameAndCreationTime[0],
        Tag.CREATION_TIME.toString(),
        pipeNameAndCreationTime[1]);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_GLOBAL_REMAINING_TIME.toString(),
        Tag.NAME.toString(),
        pipeNameAndCreationTime[0],
        Tag.CREATION_TIME.toString(),
        pipeNameAndCreationTime[1]);
    pipeTemporaryMetaMap.remove(pipeID);
  }

  //////////////////////////// register & deregister (pipe integration) ////////////////////////////

  public void register(final PipeMeta pipeMeta) {
    final String taskID =
        pipeMeta.getStaticMeta().getPipeName() + "_" + pipeMeta.getStaticMeta().getCreationTime();
    pipeTemporaryMetaMap.putIfAbsent(
        taskID, (PipeTemporaryMetaInCoordinator) pipeMeta.getTemporaryMeta());
    if (Objects.nonNull(metricService)) {
      createMetrics(taskID);
    }
  }

  public void deregister(final String pipeID) {
    if (!pipeTemporaryMetaMap.containsKey(pipeID)) {
      LOGGER.warn(
          "Failed to deregister pipe temporary meta metrics, PipeTemporaryMeta({}) does not exist",
          pipeID);
      return;
    }
    if (Objects.nonNull(metricService)) {
      removeMetrics(pipeID);
    }
  }

  public void handleTemporaryMetaChanges(final Iterable<PipeMeta> pipeMetaList) {
    final Set<String> pipeTaskIDs = new HashSet<>();
    pipeMetaList.forEach(
        pipeMeta -> {
          final String pipeTaskID =
              pipeMeta.getStaticMeta().getPipeName()
                  + "_"
                  + pipeMeta.getStaticMeta().getCreationTime();
          if (!pipeTemporaryMetaMap.containsKey(pipeTaskID)) {
            register(pipeMeta);
          }
          pipeTaskIDs.add(pipeTaskID);
        });
    ImmutableSet.copyOf(pipeTemporaryMetaMap.keySet()).stream()
        .filter(pipeTaskID -> !pipeTaskIDs.contains(pipeTaskID))
        .forEach(this::deregister);
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeTemporaryMetaMetricsHolder {

    private static final PipeTemporaryMetaInCoordinatorMetrics INSTANCE =
        new PipeTemporaryMetaInCoordinatorMetrics();

    private PipeTemporaryMetaMetricsHolder() {
      // Empty constructor
    }
  }

  public static PipeTemporaryMetaInCoordinatorMetrics getInstance() {
    return PipeTemporaryMetaMetricsHolder.INSTANCE;
  }
}
