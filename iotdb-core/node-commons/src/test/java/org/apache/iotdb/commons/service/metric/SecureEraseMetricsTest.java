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

package org.apache.iotdb.commons.service.metric;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import org.junit.Test;

import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SecureEraseMetricsTest {

  @Test
  public void testRecordAndUnbind() {
    AbstractMetricService metricService = mock(AbstractMetricService.class);
    Counter deleteCounter = mock(Counter.class);
    Counter truncateCounter = mock(Counter.class);
    when(metricService.getOrCreateCounter(
            eq(Metric.SECURE_ERASE_BYTES.toString()),
            eq(MetricLevel.IMPORTANT),
            eq(Tag.OPERATION.toString()),
            eq(SecureEraseMetrics.DELETE)))
        .thenReturn(deleteCounter);
    when(metricService.getOrCreateCounter(
            eq(Metric.SECURE_ERASE_BYTES.toString()),
            eq(MetricLevel.IMPORTANT),
            eq(Tag.OPERATION.toString()),
            eq(SecureEraseMetrics.TRUNCATE)))
        .thenReturn(truncateCounter);

    SecureEraseMetrics metrics = SecureEraseMetrics.getInstance();
    metrics.bindTo(metricService);
    try {
      metrics.recordErasedBytes(SecureEraseMetrics.DELETE, 10);
      metrics.recordErasedBytes(SecureEraseMetrics.TRUNCATE, 20);
      metrics.recordErasedBytes(SecureEraseMetrics.DELETE, 0);
      metrics.recordErasedBytes("unknown", 30);

      verify(deleteCounter).inc(10);
      verify(truncateCounter).inc(20);
      verify(deleteCounter, never()).inc(0);
      verify(deleteCounter, never()).inc(30);
      verify(truncateCounter, never()).inc(30);
    } finally {
      metrics.unbindFrom(metricService);
    }

    verify(metricService)
        .remove(
            MetricType.COUNTER,
            Metric.SECURE_ERASE_BYTES.toString(),
            Tag.OPERATION.toString(),
            SecureEraseMetrics.DELETE);
    verify(metricService)
        .remove(
            MetricType.COUNTER,
            Metric.SECURE_ERASE_BYTES.toString(),
            Tag.OPERATION.toString(),
            SecureEraseMetrics.TRUNCATE);
  }
}
