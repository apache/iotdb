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

package org.apache.iotdb.confignode.persistence.metric;

import org.apache.iotdb.confignode.persistence.NodeInfo;
import org.apache.iotdb.db.service.metrics.enums.Metric;
import org.apache.iotdb.db.service.metrics.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Objects;

import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.METRIC_STATUS_REGISTER;
import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.METRIC_TAG_TOTAL;

public class NodeInfoMetrics implements IMetricSet {

  private NodeInfo nodeInfo;

  public NodeInfoMetrics(NodeInfo nodeInfo) {
    this.nodeInfo = nodeInfo;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService.getOrCreateAutoGauge(
        Metric.CONFIG_NODE.toString(),
        MetricLevel.CORE,
        this,
        o -> nodeInfo.getRegisteredConfigNodeCount(),
        Tag.NAME.toString(),
        METRIC_TAG_TOTAL,
        Tag.STATUS.toString(),
        METRIC_STATUS_REGISTER);
    metricService.getOrCreateAutoGauge(
        Metric.DATA_NODE.toString(),
        MetricLevel.CORE,
        this,
        o -> nodeInfo.getRegisteredDataNodeCount(),
        Tag.NAME.toString(),
        METRIC_TAG_TOTAL,
        Tag.STATUS.toString(),
        METRIC_STATUS_REGISTER);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.GAUGE,
        Metric.CONFIG_NODE.toString(),
        Tag.NAME.toString(),
        METRIC_TAG_TOTAL,
        Tag.STATUS.toString(),
        METRIC_STATUS_REGISTER);
    metricService.remove(
        MetricType.GAUGE,
        Metric.DATA_NODE.toString(),
        Tag.NAME.toString(),
        METRIC_TAG_TOTAL,
        Tag.STATUS.toString(),
        METRIC_STATUS_REGISTER);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    NodeInfoMetrics that = (NodeInfoMetrics) o;
    return Objects.equals(nodeInfo, that.nodeInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodeInfo);
  }
}
