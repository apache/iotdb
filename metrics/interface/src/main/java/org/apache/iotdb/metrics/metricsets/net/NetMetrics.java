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

  private static final String RECEIVED_BYTES = "received_bytes";
  private static final String RECEIVED_PACKETS = "received_packets";
  private static final String TRANSMITTED_BYTES = "transmitted_bytes";
  private static final String TRANSMITTED_PACKETS = "transmitted_packets";

  private static final String TYPE = "type";
  private static final String IFACE_NAME = "iface_name";
  private static final String RECEIVE = "receive";
  private static final String TRANSMIT = "transmit";

  @Override
  public void bindTo(AbstractMetricService metricService) {
    // metrics for net
    Set<String> ifaceSet = netMetricManager.getIfaceSet();
    for (String iface : ifaceSet) {
      metricService.createAutoGauge(
          RECEIVED_BYTES,
          MetricLevel.IMPORTANT,
          netMetricManager,
          x -> x.getReceivedByte().get(iface),
          TYPE,
          RECEIVE,
          IFACE_NAME,
          iface);
      metricService.createAutoGauge(
          TRANSMITTED_BYTES,
          MetricLevel.IMPORTANT,
          netMetricManager,
          x -> x.getTransmittedBytes().get(iface),
          TYPE,
          TRANSMIT,
          IFACE_NAME,
          iface);
      metricService.createAutoGauge(
          RECEIVED_PACKETS,
          MetricLevel.IMPORTANT,
          netMetricManager,
          x -> x.getReceivedPackets().get(iface),
          TYPE,
          RECEIVE,
          IFACE_NAME,
          iface);
      metricService.createAutoGauge(
          TRANSMITTED_PACKETS,
          MetricLevel.IMPORTANT,
          netMetricManager,
          x -> x.getTransmittedPackets().get(iface),
          TYPE,
          TRANSMIT,
          IFACE_NAME,
          iface);
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    Set<String> ifaceSet = netMetricManager.getIfaceSet();
    for (String iface : ifaceSet) {
      metricService.remove(MetricType.AUTO_GAUGE, RECEIVED_BYTES, TYPE, RECEIVE, IFACE_NAME, iface);
      metricService.remove(MetricType.AUTO_GAUGE, TRANSMIT, TYPE, TRANSMIT, IFACE_NAME, iface);
      metricService.remove(
          MetricType.AUTO_GAUGE, RECEIVED_PACKETS, TYPE, RECEIVE, IFACE_NAME, iface);
      metricService.remove(
          MetricType.AUTO_GAUGE, TRANSMITTED_PACKETS, TYPE, TRANSMIT, IFACE_NAME, iface);
    }
  }
}
