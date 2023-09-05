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

package org.apache.iotdb.confignode.manager.node;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.NodeType;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Objects;

/** Monitoring cluster Nodes Metrics. */
public class NodeMetrics implements IMetricSet {

  private final NodeManager nodeManager;

  public NodeMetrics(NodeManager nodeManager) {
    this.nodeManager = nodeManager;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    for (NodeStatus status : NodeStatus.values()) {
      metricService.createAutoGauge(
          Metric.NODE_NUM.toString(),
          MetricLevel.CORE,
          nodeManager,
          manager -> manager.filterConfigNodeThroughStatus(status).size(),
          Tag.TYPE.toString(),
          NodeType.ConfigNode.getNodeType(),
          Tag.STATUS.toString(),
          status.getStatus());

      metricService.createAutoGauge(
          Metric.NODE_NUM.toString(),
          MetricLevel.CORE,
          nodeManager,
          manager -> manager.filterDataNodeThroughStatus(status).size(),
          Tag.TYPE.toString(),
          NodeType.DataNode.getNodeType(),
          Tag.STATUS.toString(),
          status.getStatus());
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    for (NodeStatus status : NodeStatus.values()) {
      metricService.remove(
          MetricType.AUTO_GAUGE,
          Metric.NODE_NUM.toString(),
          Tag.TYPE.toString(),
          NodeType.ConfigNode.getNodeType(),
          Tag.STATUS.toString(),
          status.getStatus());

      metricService.remove(
          MetricType.AUTO_GAUGE,
          Metric.NODE_NUM.toString(),
          Tag.TYPE.toString(),
          NodeType.DataNode.getNodeType(),
          Tag.STATUS.toString(),
          status.getStatus());
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NodeMetrics that = (NodeMetrics) o;
    return nodeManager.equals(that.nodeManager);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodeManager);
  }
}
