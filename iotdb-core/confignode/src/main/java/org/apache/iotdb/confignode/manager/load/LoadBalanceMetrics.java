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

package org.apache.iotdb.confignode.manager.load;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Objects;

/**
 * Cluster-level load balance metrics, emitted by the ConfigNode leader only.
 *
 * <p>Currently exposes the standard deviation of disk usage rate across all DataNodes (sampled
 * through heartbeat), which reflects how balanced the disk usage is and is used to observe the
 * effect of the LOAD BALANCE feature. A smaller value means more balanced disk usage.
 */
public class LoadBalanceMetrics implements IMetricSet {

  private final IManager configManager;

  public LoadBalanceMetrics(IManager configManager) {
    this.configManager = configManager;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.DATA_NODE_DISK_USAGE_RATE_STD.toString(),
        MetricLevel.CORE,
        configManager,
        LoadBalanceMetrics::getDataNodeDiskUsageRateStd);
  }

  /**
   * Only the leader maintains the up-to-date heartbeat samples for the whole cluster, so this
   * metric is meaningful on the leader only. Non-leader ConfigNodes report 0.
   */
  private static double getDataNodeDiskUsageRateStd(IManager configManager) {
    if (!configManager.getConsensusManager().isLeader()) {
      return 0d;
    }
    return configManager.getLoadManager().getDataNodeDiskUsageRateStd();
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(MetricType.AUTO_GAUGE, Metric.DATA_NODE_DISK_USAGE_RATE_STD.toString());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LoadBalanceMetrics that = (LoadBalanceMetrics) o;
    return configManager.equals(that.configManager);
  }

  @Override
  public int hashCode() {
    return Objects.hash(configManager);
  }
}
