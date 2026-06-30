/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.metric.schema;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.pipe.agent.task.subtask.sink.PipeSinkSubtask;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PipeSchemaRegionSinkMetricsTest {

  @Test
  public void testRegisterAndDeregisterCreateAndRemoveHistograms() throws Exception {
    final String taskId = "schema-task-" + System.nanoTime();
    boolean deregistered = false;
    final AbstractMetricService metricService = Mockito.mock(AbstractMetricService.class);
    final PipeSinkSubtask subtask = Mockito.mock(PipeSinkSubtask.class);
    final Rate rate = Mockito.mock(Rate.class);
    final Histogram batchSizeHistogram = Mockito.mock(Histogram.class);
    final Histogram batchTimeHistogram = Mockito.mock(Histogram.class);
    final Histogram eventSizeHistogram = Mockito.mock(Histogram.class);

    when(subtask.getTaskID()).thenReturn(taskId);
    when(subtask.getAttributeSortedString()).thenReturn("schema_test");
    when(subtask.getCreationTime()).thenReturn(1L);
    when(metricService.getOrCreateRate(
            eq(Metric.PIPE_CONNECTOR_SCHEMA_TRANSFER.toString()),
            eq(MetricLevel.IMPORTANT),
            eq(Tag.NAME.toString()),
            eq("schema_test"),
            eq(Tag.CREATION_TIME.toString()),
            eq("1")))
        .thenReturn(rate);
    when(metricService.getOrCreateHistogram(
            eq(Metric.PIPE_SCHEMA_BATCH_SIZE.toString()),
            eq(MetricLevel.IMPORTANT),
            eq(Tag.NAME.toString()),
            eq("schema_test"),
            eq(Tag.CREATION_TIME.toString()),
            eq("1")))
        .thenReturn(batchSizeHistogram);
    when(metricService.getOrCreateHistogram(
            eq(Metric.PIPE_SCHEMA_BATCH_TIME_COST.toString()),
            eq(MetricLevel.IMPORTANT),
            eq(Tag.NAME.toString()),
            eq("schema_test"),
            eq(Tag.CREATION_TIME.toString()),
            eq("1")))
        .thenReturn(batchTimeHistogram);
    when(metricService.getOrCreateHistogram(
            eq(Metric.PIPE_CONNECTOR_BATCH_SIZE.toString()),
            eq(MetricLevel.IMPORTANT),
            eq(Tag.NAME.toString()),
            eq("schema_test")))
        .thenReturn(eventSizeHistogram);

    final PipeSchemaRegionSinkMetrics metrics = PipeSchemaRegionSinkMetrics.getInstance();

    final Field metricServiceField =
        PipeSchemaRegionSinkMetrics.class.getDeclaredField("metricService");
    metricServiceField.setAccessible(true);
    final Field connectorMapField =
        PipeSchemaRegionSinkMetrics.class.getDeclaredField("connectorMap");
    connectorMapField.setAccessible(true);
    final Field schemaRateMapField =
        PipeSchemaRegionSinkMetrics.class.getDeclaredField("schemaRateMap");
    schemaRateMapField.setAccessible(true);

    ((Map<?, ?>) connectorMapField.get(metrics)).clear();
    ((Map<?, ?>) schemaRateMapField.get(metrics)).clear();
    metricServiceField.set(metrics, null);

    try {
      metrics.register(subtask);
      metrics.bindTo(metricService);

      verify(subtask).setSchemaBatchSizeHistogram(batchSizeHistogram);
      verify(subtask).setSchemaBatchTimeIntervalHistogram(batchTimeHistogram);
      verify(subtask).setEventSizeHistogram(eventSizeHistogram);

      metrics.deregister(taskId);

      verify(metricService)
          .remove(
              MetricType.HISTOGRAM,
              Metric.PIPE_SCHEMA_BATCH_SIZE.toString(),
              Tag.NAME.toString(),
              "schema_test",
              Tag.CREATION_TIME.toString(),
              "1");
      verify(metricService)
          .remove(
              MetricType.HISTOGRAM,
              Metric.PIPE_SCHEMA_BATCH_TIME_COST.toString(),
              Tag.NAME.toString(),
              "schema_test",
              Tag.CREATION_TIME.toString(),
              "1");
      verify(metricService)
          .remove(
              MetricType.HISTOGRAM,
              Metric.PIPE_CONNECTOR_BATCH_SIZE.toString(),
              Tag.NAME.toString(),
              "schema_test");
      verify(metricService)
          .remove(
              MetricType.RATE,
              Metric.PIPE_CONNECTOR_SCHEMA_TRANSFER.toString(),
              Tag.NAME.toString(),
              "schema_test",
              Tag.CREATION_TIME.toString(),
              "1");
      deregistered = true;
    } finally {
      if (!deregistered) {
        metrics.deregister(taskId);
      }
      ((Map<?, ?>) connectorMapField.get(metrics)).clear();
      ((Map<?, ?>) schemaRateMapField.get(metrics)).clear();
      metricServiceField.set(metrics, null);
    }
  }
}
