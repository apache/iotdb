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

package org.apache.iotdb.consensus.iot.logdispatcher;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class LogDispatcherThreadMetricsTest {

  private static final DataRegionId REGION = new DataRegionId(1);

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
  }

  @After
  public void tearDown() {
    service.stopService();
    config.setMetricLevel(originalLevel);
    config.setMetricReporterList(originalReporters);
  }

  /** The dispatcher threads of all peers of a region record into the same stage timers. */
  @Test
  public void testStageTimersAreSharedByThePeersOfARegion() {
    final LogDispatcherThreadMetrics first = new LogDispatcherThreadMetrics(mockThread(2));
    final LogDispatcherThreadMetrics second = new LogDispatcherThreadMetrics(mockThread(3));
    service.addMetricSet(first);
    service.addMetricSet(second);
    try {
      service.restartService();
      first.recordConstructBatchTime(1);
      second.recordConstructBatchTime(1);
      assertEquals(2, getConstructBatchTimer().getCount());

      // A peer leaving the region must not remove the timers still used by the other peers
      service.removeMetricSet(second);
      first.recordConstructBatchTime(1);
      assertEquals(3, getConstructBatchTimer().getCount());
    } finally {
      service.removeMetricSet(first);
      service.removeMetricSet(second);
    }
    assertNull(findConstructBatchTimer());
  }

  private static LogDispatcher.LogDispatcherThread mockThread(final int peerNodeId) {
    final LogDispatcher.LogDispatcherThread thread =
        Mockito.mock(LogDispatcher.LogDispatcherThread.class);
    Mockito.when(thread.getPeer())
        .thenReturn(new Peer(REGION, peerNodeId, new TEndPoint("127.0.0.1", 10000 + peerNodeId)));
    return thread;
  }

  private Timer getConstructBatchTimer() {
    final IMetric timer = findConstructBatchTimer();
    if (timer == null) {
      throw new AssertionError("The constructBatch timer of " + REGION + " is not registered");
    }
    return (Timer) timer;
  }

  private IMetric findConstructBatchTimer() {
    for (final Map.Entry<MetricInfo, IMetric> entry : service.getAllMetrics().entrySet()) {
      final Map<String, String> tags = entry.getKey().getTags();
      if (Metric.IOT_SEND_LOG.toString().equals(entry.getKey().getName())
          && "constructBatch".equals(tags.get(Tag.STAGE.toString()))
          && REGION.toString().equals(tags.get(Tag.REGION.toString()))) {
        return entry.getValue();
      }
    }
    return null;
  }
}
