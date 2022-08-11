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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.NodeManager;
import org.apache.iotdb.db.service.metrics.MetricsService;
import org.apache.iotdb.db.service.metrics.enums.Metric;
import org.apache.iotdb.db.service.metrics.enums.Tag;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.MetricLevel;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** This class collates metrics about loadManager */
public class LoadManagerMetrics {

  private final IManager configManager;
  Map<Integer, Integer> idToCountMap = new ConcurrentHashMap<>();

  public LoadManagerMetrics(IManager configManager) {
    this.configManager = configManager;
  }

  public void addMetrics() {
    addNodeMetrics();
    addLeaderCount();
  }

  private int getRunningConfigNodesNum() {
    List<TConfigNodeLocation> allConfigNodes =
        configManager.getLoadManager().getOnlineConfigNodes();
    if (allConfigNodes == null) {
      return 0;
    }
    for (TConfigNodeLocation configNodeLocation : allConfigNodes) {
      String name = NodeUrlUtils.convertTEndPointUrl(configNodeLocation.getInternalEndPoint());

      MetricsService.getInstance()
          .getMetricManager()
          .getOrCreateGauge(
              Metric.CLUSTER_NODE_STATUS.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              name,
              Tag.TYPE.toString(),
              "ConfigNode")
          .set(1);
    }
    return allConfigNodes.size();
  }

  private int getRunningDataNodesNum() {
    List<TDataNodeConfiguration> allDataNodes =
        configManager.getLoadManager().getOnlineDataNodes(-1);
    if (allDataNodes == null) {
      return 0;
    }
    for (TDataNodeConfiguration dataNodeInfo : allDataNodes) {
      TDataNodeLocation dataNodeLocation = dataNodeInfo.getLocation();
      String name = NodeUrlUtils.convertTEndPointUrl(dataNodeLocation.getClientRpcEndPoint());

      MetricsService.getInstance()
          .getMetricManager()
          .getOrCreateGauge(
              Metric.CLUSTER_NODE_STATUS.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              name,
              Tag.TYPE.toString(),
              "DataNode")
          .set(1);
    }
    return allDataNodes.size();
  }

  private int getUnknownConfigNodesNum() {
    List<TConfigNodeLocation> allConfigNodes =
        configManager.getLoadManager().getUnknownConfigNodes();
    if (allConfigNodes == null) {
      return 0;
    }
    for (TConfigNodeLocation configNodeLocation : allConfigNodes) {
      String name = NodeUrlUtils.convertTEndPointUrl(configNodeLocation.getInternalEndPoint());

      MetricsService.getInstance()
          .getMetricManager()
          .getOrCreateGauge(
              Metric.CLUSTER_NODE_STATUS.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              name,
              Tag.TYPE.toString(),
              "ConfigNode")
          .set(0);
    }
    return allConfigNodes.size();
  }

  private int getUnknownDataNodesNum() {
    List<TDataNodeConfiguration> allDataNodes =
        configManager.getLoadManager().getUnknownDataNodes(-1);
    if (allDataNodes == null) {
      return 0;
    }
    for (TDataNodeConfiguration dataNodeInfo : allDataNodes) {
      TDataNodeLocation dataNodeLocation = dataNodeInfo.getLocation();
      String name = NodeUrlUtils.convertTEndPointUrl(dataNodeLocation.getClientRpcEndPoint());

      MetricsService.getInstance()
          .getMetricManager()
          .getOrCreateGauge(
              Metric.CLUSTER_NODE_STATUS.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              name,
              Tag.TYPE.toString(),
              "DataNode")
          .set(0);
    }
    return allDataNodes.size();
  }

  public void addNodeMetrics() {
    if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()) {
      MetricsService.getInstance()
          .getMetricManager()
          .getOrCreateAutoGauge(
              Metric.CONFIG_NODE.toString(),
              MetricLevel.CORE,
              this,
              o -> getRunningConfigNodesNum(),
              Tag.NAME.toString(),
              "total",
              Tag.STATUS.toString(),
              NodeStatus.Online.toString());

      MetricsService.getInstance()
          .getMetricManager()
          .getOrCreateAutoGauge(
              Metric.DATA_NODE.toString(),
              MetricLevel.CORE,
              this,
              o -> getRunningDataNodesNum(),
              Tag.NAME.toString(),
              "total",
              Tag.STATUS.toString(),
              NodeStatus.Online.toString());

      MetricsService.getInstance()
          .getMetricManager()
          .getOrCreateAutoGauge(
              Metric.CONFIG_NODE.toString(),
              MetricLevel.CORE,
              this,
              o -> getUnknownConfigNodesNum(),
              Tag.NAME.toString(),
              "total",
              Tag.STATUS.toString(),
              NodeStatus.Unknown.toString());

      MetricsService.getInstance()
          .getMetricManager()
          .getOrCreateAutoGauge(
              Metric.DATA_NODE.toString(),
              MetricLevel.CORE,
              this,
              o -> getUnknownDataNodesNum(),
              Tag.NAME.toString(),
              "total",
              Tag.STATUS.toString(),
              NodeStatus.Unknown.toString());
    }
  }

  /**
   * Get the LeaderCount of each DataNodeId
   *
   * @return Map<DataNodeId, LeaderCount>
   */
  public Map<Integer, Integer> getLeadershipCountByDatanode() {
    Map<Integer, Integer> idToCountMap = new ConcurrentHashMap<>();

    configManager
        .getLoadManager()
        .getAllLeadership()
        .forEach((consensusGroupId, nodeId) -> idToCountMap.merge(nodeId, 1, Integer::sum));
    return idToCountMap;
  }

  public void addLeaderCount() {
    Map<Integer, Integer> idToCountMap = getLeadershipCountByDatanode();
    getNodeManager()
        .getRegisteredDataNodes(-1)
        .forEach(
            dataNodeInfo -> {
              TDataNodeLocation dataNodeLocation = dataNodeInfo.getLocation();
              int dataNodeId = dataNodeLocation.getDataNodeId();
              if (idToCountMap.containsKey(dataNodeId)) {
                String name =
                    NodeUrlUtils.convertTEndPointUrl(dataNodeLocation.getClientRpcEndPoint());

                MetricsService.getInstance()
                        .getMetricManager()
                        .getOrCreateAutoGauge(
                                Metric.CLUSTER_NODE_LEADER_COUNT.toString(),
                                MetricLevel.IMPORTANT,
                                idToCountMap,
                                o -> get(dataNodeId),
                                Tag.NAME.toString(),
                                name);
              }
            });
  }

  public void removeMetrics() {
    MetricsService.getInstance()
        .getMetricManager()
        .removeGauge(
            Metric.CONFIG_NODE.toString(),
            Tag.NAME.toString(),
            "total",
            Tag.STATUS.toString(),
            NodeStatus.Online.toString());
    MetricsService.getInstance()
        .getMetricManager()
        .removeGauge(
            Metric.DATA_NODE.toString(),
            Tag.NAME.toString(),
            "total",
            Tag.STATUS.toString(),
            NodeStatus.Online.toString());
    MetricsService.getInstance()
        .getMetricManager()
        .removeGauge(
            Metric.CONFIG_NODE.toString(),
            Tag.NAME.toString(),
            "total",
            Tag.STATUS.toString(),
            NodeStatus.Unknown.toString());
    MetricsService.getInstance()
        .getMetricManager()
        .removeGauge(
            Metric.DATA_NODE.toString(),
            Tag.NAME.toString(),
            "total",
            Tag.STATUS.toString(),
            NodeStatus.Unknown.toString());
  }

  private LoadManager getLoadManager() {
    return configManager.getLoadManager();
  }

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }
}
