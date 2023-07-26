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
import org.apache.iotdb.metrics.utils.SystemMetric;
import org.apache.iotdb.metrics.utils.SystemTag;

import java.util.Set;

public class NetMetrics implements IMetricSet {
  private final INetMetricManager netMetricManager = INetMetricManager.getNetMetricManager();
  private static final String RECEIVE = "receive";
  private static final String TRANSMIT = "transmit";

  private final String processName;

  public NetMetrics(String processName) {
    this.processName = processName;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    // metrics for net
    Set<String> ifaceSet = netMetricManager.getIfaceSet();
    for (String iface : ifaceSet) {
      metricService.createAutoGauge(
          SystemMetric.RECEIVED_BYTES.toString(),
          MetricLevel.IMPORTANT,
          netMetricManager,
          x -> x.getReceivedByte().getOrDefault(iface, 0L).doubleValue(),
          SystemTag.TYPE.toString(),
          RECEIVE,
          SystemTag.IFACE_NAME.toString(),
          iface);
      metricService.createAutoGauge(
          SystemMetric.TRANSMITTED_BYTES.toString(),
          MetricLevel.IMPORTANT,
          netMetricManager,
          x -> x.getTransmittedBytes().getOrDefault(iface, 0L).doubleValue(),
          SystemTag.TYPE.toString(),
          TRANSMIT,
          SystemTag.IFACE_NAME.toString(),
          iface);
      metricService.createAutoGauge(
          SystemMetric.RECEIVED_PACKETS.toString(),
          MetricLevel.IMPORTANT,
          netMetricManager,
          x -> x.getReceivedPackets().getOrDefault(iface, 0L).doubleValue(),
          SystemTag.TYPE.toString(),
          RECEIVE,
          SystemTag.IFACE_NAME.toString(),
          iface);
      metricService.createAutoGauge(
          SystemMetric.TRANSMITTED_PACKETS.toString(),
          MetricLevel.IMPORTANT,
          netMetricManager,
          x -> x.getTransmittedPackets().getOrDefault(iface, 0L).doubleValue(),
          SystemTag.TYPE.toString(),
          TRANSMIT,
          SystemTag.IFACE_NAME.toString(),
          iface);
    }
    metricService.createAutoGauge(
        SystemMetric.CONNECTION_NUM.toString(),
        MetricLevel.NORMAL,
        netMetricManager,
        INetMetricManager::getConnectionNum,
        SystemTag.PROCESS_NAME.toString(),
        this.processName);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    Set<String> ifaceSet = netMetricManager.getIfaceSet();
    for (String iface : ifaceSet) {
      metricService.remove(
          MetricType.AUTO_GAUGE,
          SystemMetric.RECEIVED_BYTES.toString(),
          SystemTag.TYPE.toString(),
          RECEIVE,
          SystemTag.IFACE_NAME.toString(),
          iface);
      metricService.remove(
          MetricType.AUTO_GAUGE,
          TRANSMIT,
          SystemTag.TYPE.toString(),
          TRANSMIT,
          SystemTag.IFACE_NAME.toString(),
          iface);
      metricService.remove(
          MetricType.AUTO_GAUGE,
          SystemMetric.RECEIVED_PACKETS.toString(),
          SystemTag.TYPE.toString(),
          RECEIVE,
          SystemTag.IFACE_NAME.toString(),
          iface);
      metricService.remove(
          MetricType.AUTO_GAUGE,
          SystemMetric.TRANSMITTED_PACKETS.toString(),
          SystemTag.TYPE.toString(),
          TRANSMIT,
          SystemTag.IFACE_NAME.toString(),
          iface);
    }
    metricService.remove(
        MetricType.AUTO_GAUGE,
        SystemMetric.CONNECTION_NUM.toString(),
        SystemTag.PROCESS_NAME.toString(),
        this.processName);
  }
}
