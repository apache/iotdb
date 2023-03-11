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

package org.apache.iotdb.metrics.metricsets.net;

import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Set;

public class NetMetrics implements IMetricSet {
  private final INetMetricManager netMetricManager = INetMetricManager.getNetMetricManager();

  private final String processName;

  private static final String RECEIVED_BYTES = "received_bytes";
  private static final String RECEIVED_PACKETS = "received_packets";
  private static final String TRANSMITTED_BYTES = "transmitted_bytes";
  private static final String TRANSMITTED_PACKETS = "transmitted_packets";

  private static final String TYPE = "type";
  private static final String NAME = "name";
  private static final String RECEIVE = "receive";
  private static final String TRANSMIT = "transmit";

  public NetMetrics(String processName) {
    this.processName = processName;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    // metrics for net
    Set<String> iFaceSet = netMetricManager.getIfaceSet();
    for (String iFace : iFaceSet) {
      metricService.createAutoGauge(
          RECEIVED_BYTES,
          MetricLevel.IMPORTANT,
          netMetricManager,
          x -> x.getReceivedByte().get(iFace),
          TYPE,
          RECEIVE,
          NAME,
          iFace);
      metricService.createAutoGauge(
          TRANSMITTED_BYTES,
          MetricLevel.IMPORTANT,
          netMetricManager,
          x -> x.getTransmittedBytes().get(iFace),
          TYPE,
          TRANSMIT,
          NAME,
          iFace);
      metricService.createAutoGauge(
          RECEIVED_PACKETS,
          MetricLevel.IMPORTANT,
          netMetricManager,
          x -> x.getReceivedPackets().get(iFace),
          TYPE,
          RECEIVE,
          NAME,
          iFace);
      metricService.createAutoGauge(
          TRANSMITTED_PACKETS,
          MetricLevel.IMPORTANT,
          netMetricManager,
          x -> x.getTransmittedPackets().get(iFace),
          TYPE,
          TRANSMIT,
          NAME,
          iFace);
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    Set<String> iFaceSet = netMetricManager.getIfaceSet();
    for (String iFace : iFaceSet) {
      metricService.remove(MetricType.AUTO_GAUGE, RECEIVED_BYTES, TYPE, RECEIVE, NAME, iFace);
      metricService.remove(MetricType.AUTO_GAUGE, TRANSMIT, TYPE, TRANSMIT, NAME, iFace);
      metricService.remove(MetricType.AUTO_GAUGE, RECEIVED_PACKETS, TYPE, RECEIVE, NAME, iFace);
      metricService.remove(MetricType.AUTO_GAUGE, TRANSMITTED_PACKETS, TYPE, TRANSMIT, NAME, iFace);
    }
  }
}
