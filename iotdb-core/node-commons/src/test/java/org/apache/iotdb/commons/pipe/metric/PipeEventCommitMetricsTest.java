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

package org.apache.iotdb.commons.pipe.metric;

import org.apache.iotdb.commons.pipe.agent.task.progress.PipeEventCommitter;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class PipeEventCommitMetricsTest {

  private static final String COMMITTER_KEY = "committer";

  private final PipeEventCommitMetrics metrics = PipeEventCommitMetrics.getInstance();
  private final MetricService service = MetricService.getInstance();
  private final MetricConfig config = MetricConfigDescriptor.getInstance().getMetricConfig();
  private MetricLevel originalLevel;
  private String originalReporters;

  @Before
  public void setUp() {
    originalLevel = config.getMetricLevel();
    originalReporters =
        config.getMetricReporterList().stream().map(Enum::name).collect(Collectors.joining(","));
    config.setMetricLevel(MetricLevel.IMPORTANT);
    config.setMetricReporterList("");
    service.startService();
    service.addMetricSet(metrics);
  }

  @After
  public void tearDown() {
    service.removeMetricSet(metrics);
    service.stopService();
    config.setMetricLevel(originalLevel);
    config.setMetricReporterList(originalReporters);
  }

  @Test
  public void testCommitQueueSizeSurvivesMetricServiceRestart() {
    final PipeEventCommitter committer = Mockito.mock(PipeEventCommitter.class);
    Mockito.when(committer.getPipeName()).thenReturn("pipe");
    Mockito.when(committer.getRegionId()).thenReturn(1);
    Mockito.when(committer.commitQueueSize()).thenReturn(3L);
    metrics.register(committer, COMMITTER_KEY);
    try {
      service.restartService();
      final AutoGauge gauge = getCommitQueueSizeGauge();
      assertNotNull(gauge);
      assertEquals(3, gauge.getValue(), 0);
    } finally {
      metrics.deregister(COMMITTER_KEY);
    }
    assertNull(getCommitQueueSizeGauge());
  }

  private AutoGauge getCommitQueueSizeGauge() {
    for (final Map.Entry<MetricInfo, IMetric> entry : service.getAllMetrics().entrySet()) {
      if (Metric.PIPE_EVENT_COMMIT_QUEUE_SIZE.toString().equals(entry.getKey().getName())) {
        return (AutoGauge) entry.getValue();
      }
    }
    return null;
  }
}
