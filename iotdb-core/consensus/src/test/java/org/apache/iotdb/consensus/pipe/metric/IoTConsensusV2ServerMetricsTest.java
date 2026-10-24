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

package org.apache.iotdb.consensus.pipe.metric;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.consensus.pipe.IoTConsensusV2ServerImpl;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeName;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeSink;
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

public class IoTConsensusV2ServerMetricsTest {

  private static final DataRegionId GROUP_ID = new DataRegionId(1);

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
    IoTConsensusV2SyncLagManager.build();
  }

  @After
  public void tearDown() {
    IoTConsensusV2SyncLagManager.release(GROUP_ID.toString());
    service.stopService();
    config.setMetricLevel(originalLevel);
    config.setMetricReporterList(originalReporters);
  }

  @Test
  public void testSyncLagSurvivesMetricServiceRestart() {
    final IoTConsensusV2ServerImpl impl = Mockito.mock(IoTConsensusV2ServerImpl.class);
    Mockito.when(impl.getConsensusGroupId()).thenReturn(GROUP_ID.toString());
    final ConsensusPipeSink sink = Mockito.mock(ConsensusPipeSink.class);
    Mockito.when(sink.getLeaderReplicateProgress()).thenReturn(10L);
    Mockito.when(sink.getFollowerApplyProgress()).thenReturn(3L);
    IoTConsensusV2SyncLagManager.getInstance(GROUP_ID.toString())
        .addConsensusPipeConnector(new ConsensusPipeName(GROUP_ID, 1, 2), sink);

    final IoTConsensusV2ServerMetrics metrics = new IoTConsensusV2ServerMetrics(impl);
    service.addMetricSet(metrics);
    try {
      assertEquals(7, getSyncLag(), 0);
      service.restartService();
      assertEquals(7, getSyncLag(), 0);
    } finally {
      service.removeMetricSet(metrics);
    }
  }

  private double getSyncLag() {
    AutoGauge gauge = null;
    for (final Map.Entry<MetricInfo, IMetric> entry : service.getAllMetrics().entrySet()) {
      if (Metric.IOT_CONSENSUS_V2.toString().equals(entry.getKey().getName())
          && "syncLag".equals(entry.getKey().getTags().get(Tag.TYPE.toString()))) {
        gauge = (AutoGauge) entry.getValue();
      }
    }
    assertNotNull(gauge);
    return gauge.getValue();
  }
}
