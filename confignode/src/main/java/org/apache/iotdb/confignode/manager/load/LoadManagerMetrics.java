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
import org.apache.iotdb.confignode.manager.PartitionManager;
import org.apache.iotdb.db.service.metrics.MetricService;
import org.apache.iotdb.db.service.metrics.enums.Metric;
import org.apache.iotdb.db.service.metrics.enums.Tag;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.METRIC_STATUS_ONLINE;
import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.METRIC_STATUS_UNKNOWN;
import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.METRIC_TAG_TOTAL;

/** This class collates metrics about loadManager */
public class LoadManagerMetrics {

  private final IManager configManager;

  public LoadManagerMetrics(IManager configManager) {
    this.configManager = configManager;
  }

  public void addMetrics() {
    addNodeMetrics();
    addLeaderCount();
  }

  private int getRunningConfigNodesNum() {
    List<TConfigNodeLocation> runningConfigNodes =
        getNodeManager().filterConfigNodeThroughStatus(NodeStatus.Running);
    if (runningConfigNodes == null) {
      return 0;
    }
    for (TConfigNodeLocation configNodeLocation : runningConfigNodes) {
      String name = NodeUrlUtils.convertTEndPointUrl(configNodeLocation.getInternalEndPoint());

      MetricService.getInstance()
          .getOrCreateGauge(
              Metric.CLUSTER_NODE_STATUS.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              name,
              Tag.TYPE.toString(),
              "ConfigNode")
          .set(1);
    }
    return runningConfigNodes.size();
  }

  private int getRunningDataNodesNum() {
    List<TDataNodeConfiguration> runningDataNodes =
        getNodeManager().filterDataNodeThroughStatus(NodeStatus.Running);
    if (runningDataNodes == null) {
      return 0;
    }
    for (TDataNodeConfiguration dataNodeInfo : runningDataNodes) {
      TDataNodeLocation dataNodeLocation = dataNodeInfo.getLocation();
      String name = NodeUrlUtils.convertTEndPointUrl(dataNodeLocation.getClientRpcEndPoint());

      MetricService.getInstance()
          .getOrCreateGauge(
              Metric.CLUSTER_NODE_STATUS.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              name,
              Tag.TYPE.toString(),
              "DataNode")
          .set(1);
    }
    return runningDataNodes.size();
  }

  private int getUnknownConfigNodesNum() {
    List<TConfigNodeLocation> unknownConfigNodes =
        getNodeManager().filterConfigNodeThroughStatus(NodeStatus.Unknown);
    if (unknownConfigNodes == null) {
      return 0;
    }
    for (TConfigNodeLocation configNodeLocation : unknownConfigNodes) {
      String name = NodeUrlUtils.convertTEndPointUrl(configNodeLocation.getInternalEndPoint());

      MetricService.getInstance()
          .getOrCreateGauge(
              Metric.CLUSTER_NODE_STATUS.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              name,
              Tag.TYPE.toString(),
              "ConfigNode")
          .set(0);
    }
    return unknownConfigNodes.size();
  }

  private int getUnknownDataNodesNum() {
    List<TDataNodeConfiguration> unknownDataNodes =
        getNodeManager().filterDataNodeThroughStatus(NodeStatus.Unknown);
    if (unknownDataNodes == null) {
      return 0;
    }
    for (TDataNodeConfiguration dataNodeInfo : unknownDataNodes) {
      TDataNodeLocation dataNodeLocation = dataNodeInfo.getLocation();
      String name = NodeUrlUtils.convertTEndPointUrl(dataNodeLocation.getClientRpcEndPoint());

      MetricService.getInstance()
          .getOrCreateGauge(
              Metric.CLUSTER_NODE_STATUS.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              name,
              Tag.TYPE.toString(),
              "DataNode")
          .set(0);
    }
    return unknownDataNodes.size();
  }

  public void addNodeMetrics() {
    MetricService.getInstance()
        .getOrCreateAutoGauge(
            Metric.CONFIG_NODE.toString(),
            MetricLevel.CORE,
            this,
            o -> getRunningConfigNodesNum(),
            Tag.NAME.toString(),
            METRIC_TAG_TOTAL,
            Tag.STATUS.toString(),
            METRIC_STATUS_ONLINE);

    MetricService.getInstance()
        .getOrCreateAutoGauge(
            Metric.DATA_NODE.toString(),
            MetricLevel.CORE,
            this,
            o -> getRunningDataNodesNum(),
            Tag.NAME.toString(),
            METRIC_TAG_TOTAL,
            Tag.STATUS.toString(),
            METRIC_STATUS_ONLINE);

    MetricService.getInstance()
        .getOrCreateAutoGauge(
            Metric.CONFIG_NODE.toString(),
            MetricLevel.CORE,
            this,
            o -> getUnknownConfigNodesNum(),
            Tag.NAME.toString(),
            METRIC_TAG_TOTAL,
            Tag.STATUS.toString(),
            METRIC_STATUS_UNKNOWN);

    MetricService.getInstance()
        .getOrCreateAutoGauge(
            Metric.DATA_NODE.toString(),
            MetricLevel.CORE,
            this,
            o -> getUnknownDataNodesNum(),
            Tag.NAME.toString(),
            METRIC_TAG_TOTAL,
            Tag.STATUS.toString(),
            METRIC_STATUS_UNKNOWN);
  }

  /**
   * Get the LeaderCount of Specific DataNodeId
   *
   * @return Integer
   */
  public Integer getLeadershipCountByDatanode(int dataNodeId) {
    Map<Integer, Integer> idToCountMap = new ConcurrentHashMap<>();

    getPartitionManager()
        .getAllLeadership()
        .forEach((consensusGroupId, nodeId) -> idToCountMap.merge(nodeId, 1, Integer::sum));
    return idToCountMap.get(dataNodeId);
  }

  public void addLeaderCount() {
    getNodeManager()
        .getRegisteredDataNodes()
        .forEach(
            dataNodeInfo -> {
              TDataNodeLocation dataNodeLocation = dataNodeInfo.getLocation();
              int dataNodeId = dataNodeLocation.getDataNodeId();
              String name =
                  NodeUrlUtils.convertTEndPointUrl(dataNodeLocation.getClientRpcEndPoint());

              MetricService.getInstance()
                  .getOrCreateAutoGauge(
                      Metric.CLUSTER_NODE_LEADER_COUNT.toString(),
                      MetricLevel.IMPORTANT,
                      this,
                      o -> getLeadershipCountByDatanode(dataNodeId),
                      Tag.NAME.toString(),
                      name);
            });
  }

  public void removeMetrics() {
    MetricService.getInstance()
        .remove(
            MetricType.GAUGE,
            Metric.CONFIG_NODE.toString(),
            Tag.NAME.toString(),
            METRIC_TAG_TOTAL,
            Tag.STATUS.toString(),
            METRIC_STATUS_ONLINE);
    MetricService.getInstance()
        .remove(
            MetricType.GAUGE,
            Metric.DATA_NODE.toString(),
            Tag.NAME.toString(),
            METRIC_TAG_TOTAL,
            Tag.STATUS.toString(),
            METRIC_STATUS_ONLINE);
    MetricService.getInstance()
        .remove(
            MetricType.GAUGE,
            Metric.CONFIG_NODE.toString(),
            Tag.NAME.toString(),
            METRIC_TAG_TOTAL,
            Tag.STATUS.toString(),
            METRIC_STATUS_UNKNOWN);
    MetricService.getInstance()
        .remove(
            MetricType.GAUGE,
            Metric.DATA_NODE.toString(),
            Tag.NAME.toString(),
            METRIC_TAG_TOTAL,
            Tag.STATUS.toString(),
            METRIC_STATUS_UNKNOWN);
  }

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }

  private PartitionManager getPartitionManager() {
    return configManager.getPartitionManager();
  }
}
