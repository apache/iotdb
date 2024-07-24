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
import org.apache.iotdb.db.pipe.extractor.schemaregion.SchemaRegionListeningQueue;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PipeSchemaRegionListenerMetrics implements IMetricSet {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeSchemaRegionListenerMetrics.class);

  @SuppressWarnings("java:S3077")
  private volatile AbstractMetricService metricService;

  private final ConcurrentMap<Integer, SchemaRegionListeningQueue> listeningQueueMap =
      new ConcurrentHashMap<>();

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    this.metricService = metricService;
    ImmutableSet.copyOf(listeningQueueMap.keySet()).forEach(this::createMetrics);
  }

  private void createMetrics(final Integer schemaRegionId) {
    createAutoGauge(schemaRegionId);
  }

  private void createAutoGauge(final Integer schemaRegionId) {
    metricService.createAutoGauge(
        Metric.PIPE_SCHEMA_LINKED_QUEUE_SIZE.toString(),
        MetricLevel.IMPORTANT,
        listeningQueueMap.get(schemaRegionId),
        SchemaRegionListeningQueue::getSize,
        Tag.REGION.toString(),
        String.valueOf(schemaRegionId));
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    ImmutableSet.copyOf(listeningQueueMap.keySet()).forEach(this::deregister);
    if (!listeningQueueMap.isEmpty()) {
      LOGGER.warn(
          "Failed to unbind from pipe schema region listener metrics, listening queue map not empty");
    }
  }

  private void removeMetrics(final Integer schemaRegionId) {
    removeAutoGauge(schemaRegionId);
  }

  private void removeAutoGauge(final Integer schemaRegionId) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_SCHEMA_LINKED_QUEUE_SIZE.toString(),
        Tag.REGION.toString(),
        String.valueOf(schemaRegionId));
  }

  //////////////////////////// register & deregister (pipe integration) ////////////////////////////

  public void register(
      @NonNull final SchemaRegionListeningQueue schemaRegionListeningQueue,
      final Integer schemaRegionId) {
    listeningQueueMap.putIfAbsent(schemaRegionId, schemaRegionListeningQueue);
    if (Objects.nonNull(metricService)) {
      createMetrics(schemaRegionId);
    }
  }

  public void deregister(final Integer schemaRegionId) {
    if (!listeningQueueMap.containsKey(schemaRegionId)) {
      LOGGER.warn(
          "Failed to deregister schema region listener metrics, SchemaRegionListeningQueue({}) does not exist",
          schemaRegionId);
      return;
    }
    if (Objects.nonNull(metricService)) {
      removeMetrics(schemaRegionId);
    }
    listeningQueueMap.remove(schemaRegionId);
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeSchemaRegionListenerMetricsHolder {

    private static final PipeSchemaRegionListenerMetrics INSTANCE =
        new PipeSchemaRegionListenerMetrics();

    private PipeSchemaRegionListenerMetricsHolder() {
      // Empty constructor
    }
  }

  public static PipeSchemaRegionListenerMetrics getInstance() {
    return PipeSchemaRegionListenerMetricsHolder.INSTANCE;
  }

  private PipeSchemaRegionListenerMetrics() {
    // Empty constructor
  }
}
