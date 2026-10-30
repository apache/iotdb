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

package org.apache.iotdb.db.pipe.metric.receiver;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.junit.Test;

import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class PipeDataNodeReceiverMetricsTest {

  @Test
  public void testUnbindRemovesAllRequestSizeRates() {
    final MetricService service = MetricService.getInstance();
    final MetricConfig config = MetricConfigDescriptor.getInstance().getMetricConfig();
    final MetricLevel originalLevel = config.getMetricLevel();
    final String originalReporters =
        config.getMetricReporterList().stream().map(Enum::name).collect(Collectors.joining(","));
    final PipeDataNodeReceiverMetrics metrics = PipeDataNodeReceiverMetrics.getInstance();
    try {
      config.setMetricLevel(MetricLevel.IMPORTANT);
      config.setMetricReporterList("");
      service.startService();
      metrics.bindTo(service);
      metrics.markHandshakeDatanodeV1Size(11);
      metrics.markHandshakeDatanodeV2Size(13);
      assertEquals(20, countRequestSizeRates(service));

      metrics.unbindFrom(service);
      assertEquals(0, countRequestSizeRates(service));

      metrics.bindTo(service);
      for (final String type : new String[] {"handshakeDataNodeV1", "handshakeDataNodeV2"}) {
        assertEquals(
            0,
            service
                .getOrCreateRate(
                    Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
                    MetricLevel.IMPORTANT,
                    Tag.NAME.toString(),
                    "pipeDataNodeReceiver",
                    Tag.TYPE.toString(),
                    type)
                .getCount());
      }
    } finally {
      metrics.unbindFrom(service);
      service.stopService();
      config.setMetricLevel(originalLevel);
      config.setMetricReporterList(originalReporters);
    }
  }

  private static long countRequestSizeRates(final MetricService service) {
    return service.getAllMetrics().keySet().stream()
        .filter(info -> Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString().equals(info.getName()))
        .count();
  }
}
