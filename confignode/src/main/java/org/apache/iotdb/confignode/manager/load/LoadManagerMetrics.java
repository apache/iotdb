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
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.METRIC_CONFIG_NODE;
import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.METRIC_DATA_NODE;
import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.METRIC_STATUS_ONLINE;
import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.METRIC_STATUS_REGISTER;
import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.METRIC_STATUS_UNKNOWN;
import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.METRIC_TAG_TOTAL;

/** This class collates metrics about loadManager */
public class LoadManagerMetrics implements IMetricSet {

  private final IManager configManager;

  public LoadManagerMetrics(IManager configManager) {
    this.configManager = configManager;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.CONFIG_NODE.toString(),
        MetricLevel.CORE,
        this,
        o -> getRegisterConfigNodesNum(metricService),
        Tag.NAME.toString(),
        METRIC_TAG_TOTAL,
        Tag.STATUS.toString(),
        METRIC_STATUS_REGISTER);

    metricService.createAutoGauge(
        Metric.DATA_NODE.toString(),
        MetricLevel.CORE,
        this,
        o -> getRegisterDataNodesNum(metricService),
        Tag.NAME.toString(),
        METRIC_TAG_TOTAL,
        Tag.STATUS.toString(),
        METRIC_STATUS_REGISTER);

    metricService.createAutoGauge(
        Metric.CONFIG_NODE.toString(),
        MetricLevel.CORE,
        this,
        o -> getRunningConfigNodesNum(metricService),
        Tag.NAME.toString(),
        METRIC_TAG_TOTAL,
        Tag.STATUS.toString(),
        METRIC_STATUS_ONLINE);

    metricService.createAutoGauge(
        Metric.DATA_NODE.toString(),
        MetricLevel.CORE,
        this,
        o -> getRunningDataNodesNum(metricService),
        Tag.NAME.toString(),
        METRIC_TAG_TOTAL,
        Tag.STATUS.toString(),
        METRIC_STATUS_ONLINE);

    metricService.createAutoGauge(
        Metric.CONFIG_NODE.toString(),
        MetricLevel.CORE,
        this,
        o -> getUnknownConfigNodesNum(metricService),
        Tag.NAME.toString(),
        METRIC_TAG_TOTAL,
        Tag.STATUS.toString(),
        METRIC_STATUS_UNKNOWN);

    metricService.createAutoGauge(
        Metric.DATA_NODE.toString(),
        MetricLevel.CORE,
        this,
        o -> getUnknownDataNodesNum(metricService),
        Tag.NAME.toString(),
        METRIC_TAG_TOTAL,
        Tag.STATUS.toString(),
        METRIC_STATUS_UNKNOWN);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.CONFIG_NODE.toString(),
        Tag.NAME.toString(),
        METRIC_TAG_TOTAL,
        Tag.STATUS.toString(),
        METRIC_STATUS_REGISTER);

    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.DATA_NODE.toString(),
        Tag.NAME.toString(),
        METRIC_TAG_TOTAL,
        Tag.STATUS.toString(),
        METRIC_STATUS_REGISTER);

    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.CONFIG_NODE.toString(),
        Tag.NAME.toString(),
        METRIC_TAG_TOTAL,
        Tag.STATUS.toString(),
        METRIC_STATUS_ONLINE);

    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.DATA_NODE.toString(),
        Tag.NAME.toString(),
        METRIC_TAG_TOTAL,
        Tag.STATUS.toString(),
        METRIC_STATUS_ONLINE);

    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.CONFIG_NODE.toString(),
        Tag.NAME.toString(),
        METRIC_TAG_TOTAL,
        Tag.STATUS.toString(),
        METRIC_STATUS_UNKNOWN);

    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.DATA_NODE.toString(),
        Tag.NAME.toString(),
        METRIC_TAG_TOTAL,
        Tag.STATUS.toString(),
        METRIC_STATUS_UNKNOWN);

    getNodeManager()
        .getRegisteredDataNodes()
        .forEach(
            dataNodeInfo -> {
              TDataNodeLocation dataNodeLocation = dataNodeInfo.getLocation();
              String name =
                  NodeUrlUtils.convertTEndPointUrl(dataNodeLocation.getClientRpcEndPoint());

              metricService.remove(
                  MetricType.GAUGE,
                  Metric.CLUSTER_NODE_LEADER_COUNT.toString(),
                  Tag.NAME.toString(),
                  name);
            });
  }

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }

  private PartitionManager getPartitionManager() {
    return configManager.getPartitionManager();
  }

  private LoadManager getLoadManager() {
    return configManager.getLoadManager();
  }

  private int getRegisterConfigNodesNum(AbstractMetricService metricService) {
    return getNodeManager().getRegisteredConfigNodes().size();
  }

  private int getRegisterDataNodesNum(AbstractMetricService metricService) {
    List<TDataNodeConfiguration> dataNodeConfigurations = getNodeManager().getRegisteredDataNodes();
    Map<Integer, Integer> idToCountMap = new ConcurrentHashMap<>();

    getLoadManager()
        .getLatestRegionLeaderMap()
        .forEach((consensusGroupId, nodeId) -> idToCountMap.merge(nodeId, 1, Integer::sum));
    for (TDataNodeConfiguration dataNodeInfo : dataNodeConfigurations) {
      TDataNodeLocation dataNodeLocation = dataNodeInfo.getLocation();
      int dataNodeId = dataNodeLocation.getDataNodeId();
      String name = NodeUrlUtils.convertTEndPointUrl(dataNodeLocation.getClientRpcEndPoint());
      metricService
          .getOrCreateGauge(
              Metric.CLUSTER_NODE_LEADER_COUNT.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              name)
          .set(idToCountMap.getOrDefault(dataNodeId, 0));
    }
    return dataNodeConfigurations.size();
  }

  private int getRunningConfigNodesNum(AbstractMetricService metricService) {
    List<TConfigNodeLocation> runningConfigNodes =
        getNodeManager().filterConfigNodeThroughStatus(NodeStatus.Running);
    if (runningConfigNodes == null) {
      return 0;
    }
    for (TConfigNodeLocation configNodeLocation : runningConfigNodes) {
      String name = NodeUrlUtils.convertTEndPointUrl(configNodeLocation.getInternalEndPoint());

      metricService
          .getOrCreateGauge(
              Metric.CLUSTER_NODE_STATUS.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              name,
              Tag.TYPE.toString(),
              METRIC_CONFIG_NODE)
          .set(1);
    }
    return runningConfigNodes.size();
  }

  private int getRunningDataNodesNum(AbstractMetricService metricService) {
    List<TDataNodeConfiguration> runningDataNodes =
        getNodeManager().filterDataNodeThroughStatus(NodeStatus.Running);
    if (runningDataNodes == null) {
      return 0;
    }
    for (TDataNodeConfiguration dataNodeInfo : runningDataNodes) {
      TDataNodeLocation dataNodeLocation = dataNodeInfo.getLocation();
      String name = NodeUrlUtils.convertTEndPointUrl(dataNodeLocation.getClientRpcEndPoint());

      metricService
          .getOrCreateGauge(
              Metric.CLUSTER_NODE_STATUS.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              name,
              Tag.TYPE.toString(),
              METRIC_DATA_NODE)
          .set(1);
    }
    return runningDataNodes.size();
  }

  private int getUnknownConfigNodesNum(AbstractMetricService metricService) {
    List<TConfigNodeLocation> unknownConfigNodes =
        getNodeManager().filterConfigNodeThroughStatus(NodeStatus.Unknown);
    if (unknownConfigNodes == null) {
      return 0;
    }
    for (TConfigNodeLocation configNodeLocation : unknownConfigNodes) {
      String name = NodeUrlUtils.convertTEndPointUrl(configNodeLocation.getInternalEndPoint());

      metricService
          .getOrCreateGauge(
              Metric.CLUSTER_NODE_STATUS.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              name,
              Tag.TYPE.toString(),
              METRIC_CONFIG_NODE)
          .set(0);
    }
    return unknownConfigNodes.size();
  }

  private int getUnknownDataNodesNum(AbstractMetricService metricService) {
    List<TDataNodeConfiguration> unknownDataNodes =
        getNodeManager().filterDataNodeThroughStatus(NodeStatus.Unknown);
    if (unknownDataNodes == null) {
      return 0;
    }
    for (TDataNodeConfiguration dataNodeInfo : unknownDataNodes) {
      TDataNodeLocation dataNodeLocation = dataNodeInfo.getLocation();
      String name = NodeUrlUtils.convertTEndPointUrl(dataNodeLocation.getClientRpcEndPoint());

      metricService
          .getOrCreateGauge(
              Metric.CLUSTER_NODE_STATUS.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              name,
              Tag.TYPE.toString(),
              METRIC_DATA_NODE)
          .set(0);
    }
    return unknownDataNodes.size();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LoadManagerMetrics that = (LoadManagerMetrics) o;
    return Objects.equals(configManager, that.configManager);
  }

  @Override
  public int hashCode() {
    return Objects.hash(configManager);
  }
}
