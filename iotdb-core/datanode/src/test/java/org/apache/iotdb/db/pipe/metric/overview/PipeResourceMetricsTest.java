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

package org.apache.iotdb.db.pipe.metric.overview;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.pipe.metric.overview.PipeResourceMetrics.TsFileParserAdmissionRejectReason;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import org.junit.Test;

import java.lang.reflect.Constructor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PipeResourceMetricsTest {

  @Test
  public void testAdmissionRejectReasonsAreBoundAndRemoved() throws Exception {
    final AbstractMetricService service = mock(AbstractMetricService.class);
    final Constructor<PipeResourceMetrics> constructor =
        PipeResourceMetrics.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    final PipeResourceMetrics metrics = constructor.newInstance();
    final Counter cap = mock(Counter.class);
    final Counter memory = mock(Counter.class);
    final Counter fairness = mock(Counter.class);
    final String name = Metric.PIPE_TSFILE_PARSER_ADMISSION_REJECT.toString();
    final Counter aggregate = mock(Counter.class);
    when(service.getOrCreateCounter(name, MetricLevel.IMPORTANT)).thenReturn(aggregate);
    final String reasonMetricName = Metric.PIPE_TSFILE_PARSER_ADMISSION_REJECT_REASON.toString();
    when(service.getOrCreateCounter(
            reasonMetricName, MetricLevel.IMPORTANT, Tag.REASON.toString(), "cap"))
        .thenReturn(cap);
    when(service.getOrCreateCounter(
            reasonMetricName, MetricLevel.IMPORTANT, Tag.REASON.toString(), "memory"))
        .thenReturn(memory);
    when(service.getOrCreateCounter(
            reasonMetricName, MetricLevel.IMPORTANT, Tag.REASON.toString(), "fairness"))
        .thenReturn(fairness);

    metrics.bindTo(service);
    metrics.recordTsFileParserAdmissionReject(TsFileParserAdmissionRejectReason.CAP);
    metrics.recordTsFileParserAdmissionReject(TsFileParserAdmissionRejectReason.MEMORY);
    metrics.recordTsFileParserAdmissionReject(TsFileParserAdmissionRejectReason.FAIRNESS);
    metrics.unbindFrom(service);
    verify(service).remove(MetricType.COUNTER, name);
    for (final String reason : new String[] {"cap", "memory", "fairness"}) {
      verify(service).remove(MetricType.COUNTER, reasonMetricName, Tag.REASON.toString(), reason);
    }
    // After unbinding, admissions must no longer update detached metric instances.
    metrics.recordTsFileParserAdmissionReject(TsFileParserAdmissionRejectReason.CAP);
    verify(aggregate, times(3)).inc();
    verify(cap, times(1)).inc();
    verify(memory, times(1)).inc();
    verify(fairness, times(1)).inc();
  }
}
