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

package org.apache.iotdb.confignode.manager.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.confignode.exception.DatabaseNotExistsException;
import org.apache.iotdb.confignode.manager.ClusterSchemaManager;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

public class PartitionMetrics implements IMetricSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionMetrics.class);

  private final IManager configManager;

  public PartitionMetrics(IManager configManager) {
    this.configManager = configManager;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    bindRegionPartitionMetrics(metricService);
    bindDataNodePartitionMetrics();
    bindDatabasePartitionMetrics(metricService);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    unbindRegionPartitionMetrics(metricService);
    unbindDataNodePartitionMetrics();
    unbindDatabasePartitionMetrics(metricService);
  }

  private void bindRegionPartitionMetrics(AbstractMetricService metricService) {
    for (RegionStatus status : RegionStatus.values()) {
      // Count the number of SchemaRegions
      metricService.createAutoGauge(
          Metric.REGION_NUM.toString(),
          MetricLevel.CORE,
          getPartitionManager(),
          partitionManager ->
              partitionManager.countRegionWithSpecifiedStatus(
                  TConsensusGroupType.SchemaRegion, status),
          Tag.TYPE.toString(),
          TConsensusGroupType.SchemaRegion.toString(),
          Tag.STATUS.toString(),
          status.getStatus());

      // Count the number of DataRegions
      metricService.createAutoGauge(
          Metric.REGION_NUM.toString(),
          MetricLevel.CORE,
          getPartitionManager(),
          partitionManager ->
              partitionManager.countRegionWithSpecifiedStatus(
                  TConsensusGroupType.DataRegion, status),
          Tag.TYPE.toString(),
          TConsensusGroupType.DataRegion.toString(),
          Tag.STATUS.toString(),
          status.getStatus());
    }
  }

  private void unbindRegionPartitionMetrics(AbstractMetricService metricService) {
    for (RegionStatus status : RegionStatus.values()) {
      // Remove the number of SchemaRegions
      metricService.remove(
          MetricType.AUTO_GAUGE,
          Metric.REGION_NUM.toString(),
          Tag.TYPE.toString(),
          TConsensusGroupType.SchemaRegion.toString(),
          Tag.STATUS.toString(),
          status.getStatus());

      // Remove the number of DataRegions
      metricService.remove(
          MetricType.AUTO_GAUGE,
          Metric.REGION_NUM.toString(),
          Tag.TYPE.toString(),
          TConsensusGroupType.DataRegion.toString(),
          Tag.STATUS.toString(),
          status.getStatus());
    }
  }

  public static void bindDataNodePartitionMetrics(IManager configManager, int dataNodeId) {
    MetricService metricService = MetricService.getInstance();
    NodeManager nodeManager = configManager.getNodeManager();
    PartitionManager partitionManager = configManager.getPartitionManager();
    LoadManager loadManager = configManager.getLoadManager();

    String dataNodeName =
        NodeUrlUtils.convertTEndPointUrl(
            nodeManager.getRegisteredDataNode(dataNodeId).getLocation().getClientRpcEndPoint());

    // Count the number of Regions in the specified DataNode
    metricService.createAutoGauge(
        Metric.REGION_NUM_IN_DATA_NODE.toString(),
        MetricLevel.CORE,
        partitionManager,
        obj -> obj.getRegionCount(dataNodeId, TConsensusGroupType.SchemaRegion),
        Tag.NAME.toString(),
        dataNodeName,
        Tag.TYPE.toString(),
        TConsensusGroupType.SchemaRegion.toString());
    metricService.createAutoGauge(
        Metric.REGION_NUM_IN_DATA_NODE.toString(),
        MetricLevel.CORE,
        partitionManager,
        obj -> obj.getRegionCount(dataNodeId, TConsensusGroupType.DataRegion),
        Tag.NAME.toString(),
        dataNodeName,
        Tag.TYPE.toString(),
        TConsensusGroupType.DataRegion.toString());

    // Count the number of RegionGroup-leaders in the specified DataNode
    metricService.createAutoGauge(
        Metric.REGION_GROUP_LEADER_NUM_IN_DATA_NODE.toString(),
        MetricLevel.CORE,
        loadManager,
        obj -> obj.getRegionGroupLeaderCount(dataNodeId, TConsensusGroupType.SchemaRegion),
        Tag.NAME.toString(),
        dataNodeName,
        Tag.TYPE.toString(),
        TConsensusGroupType.SchemaRegion.toString());
    metricService.createAutoGauge(
        Metric.REGION_GROUP_LEADER_NUM_IN_DATA_NODE.toString(),
        MetricLevel.CORE,
        loadManager,
        obj -> obj.getRegionGroupLeaderCount(dataNodeId, TConsensusGroupType.DataRegion),
        Tag.NAME.toString(),
        dataNodeName,
        Tag.TYPE.toString(),
        TConsensusGroupType.DataRegion.toString());
  }

  private void bindDataNodePartitionMetrics() {
    List<TDataNodeConfiguration> registerDataNodes = getNodeManager().getRegisteredDataNodes();
    for (TDataNodeConfiguration dataNodeConfiguration : registerDataNodes) {
      int dataNodeId = dataNodeConfiguration.getLocation().getDataNodeId();
      bindDataNodePartitionMetrics(configManager, dataNodeId);
    }
  }

  public static void unbindDataNodePartitionMetrics(String dataNodeName) {
    MetricService metricService = MetricService.getInstance();

    // Remove the number of Regions in the specified DataNode
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.REGION_NUM_IN_DATA_NODE.toString(),
        Tag.NAME.toString(),
        dataNodeName,
        Tag.TYPE.toString(),
        TConsensusGroupType.SchemaRegion.toString());
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.REGION_NUM_IN_DATA_NODE.toString(),
        Tag.NAME.toString(),
        dataNodeName,
        Tag.TYPE.toString(),
        TConsensusGroupType.DataRegion.toString());

    // Remove the number of RegionGroup-leaders in the specified DataNode
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.REGION_GROUP_LEADER_NUM_IN_DATA_NODE.toString(),
        Tag.NAME.toString(),
        dataNodeName,
        Tag.TYPE.toString(),
        TConsensusGroupType.SchemaRegion.toString());
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.REGION_GROUP_LEADER_NUM_IN_DATA_NODE.toString(),
        Tag.NAME.toString(),
        dataNodeName,
        Tag.TYPE.toString(),
        TConsensusGroupType.DataRegion.toString());
  }

  private void unbindDataNodePartitionMetrics() {
    List<TDataNodeConfiguration> registerDataNodes = getNodeManager().getRegisteredDataNodes();
    for (TDataNodeConfiguration dataNodeConfiguration : registerDataNodes) {
      String dataNodeName =
          NodeUrlUtils.convertTEndPointUrl(
              dataNodeConfiguration.getLocation().getClientRpcEndPoint());
      unbindDataNodePartitionMetrics(dataNodeName);
    }
  }

  public static void bindDatabasePartitionMetrics(IManager configManager, String database) {
    MetricService metricService = MetricService.getInstance();
    PartitionManager partitionManager = configManager.getPartitionManager();

    // Count the number of SeriesSlots in the specified Database
    metricService.createAutoGauge(
        Metric.SERIES_SLOT_NUM_IN_DATABASE.toString(),
        MetricLevel.CORE,
        partitionManager,
        manager -> manager.getAssignedSeriesPartitionSlotsCount(database),
        Tag.NAME.toString(),
        database);

    // Count the number of RegionGroups in the specified Database
    metricService.createAutoGauge(
        Metric.REGION_GROUP_NUM_IN_DATABASE.toString(),
        MetricLevel.CORE,
        partitionManager,
        manager -> {
          try {
            return manager.getRegionGroupCount(database, TConsensusGroupType.SchemaRegion);
          } catch (DatabaseNotExistsException e) {
            LOGGER.warn("Error when counting SchemaRegionGroups in Database: {}", database, e);
            return -1;
          }
        },
        Tag.NAME.toString(),
        database,
        Tag.TYPE.toString(),
        TConsensusGroupType.SchemaRegion.toString());
    metricService.createAutoGauge(
        Metric.REGION_GROUP_NUM_IN_DATABASE.toString(),
        MetricLevel.CORE,
        partitionManager,
        manager -> {
          try {
            return manager.getRegionGroupCount(database, TConsensusGroupType.DataRegion);
          } catch (DatabaseNotExistsException e) {
            LOGGER.warn("Error when counting DataRegionGroups in Database: {}", database, e);
            return -1;
          }
        },
        Tag.NAME.toString(),
        database,
        Tag.TYPE.toString(),
        TConsensusGroupType.DataRegion.toString());
  }

  private void bindDatabasePartitionMetrics(AbstractMetricService metricService) {
    // Count the number of Databases
    metricService.createAutoGauge(
        Metric.DATABASE_NUM.toString(),
        MetricLevel.CORE,
        getClusterSchemaManager(),
        clusterSchemaManager -> clusterSchemaManager.getDatabaseNames().size());

    List<String> databases = getClusterSchemaManager().getDatabaseNames();
    for (String database : databases) {
      bindDatabasePartitionMetrics(configManager, database);
    }
  }

  public static void unbindDatabasePartitionMetrics(String database) {
    MetricService metricService = MetricService.getInstance();

    // Remove the number of SeriesSlots in the specified Database
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SERIES_SLOT_NUM_IN_DATABASE.toString(),
        Tag.NAME.toString(),
        database);

    // Remove number of RegionGroups in the specified Database
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.REGION_GROUP_NUM_IN_DATABASE.toString(),
        Tag.NAME.toString(),
        database,
        Tag.TYPE.toString(),
        TConsensusGroupType.SchemaRegion.toString());
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.REGION_GROUP_NUM_IN_DATABASE.toString(),
        Tag.NAME.toString(),
        database,
        Tag.TYPE.toString(),
        TConsensusGroupType.DataRegion.toString());
  }

  private void unbindDatabasePartitionMetrics(AbstractMetricService metricService) {
    // Remove the number of Databases
    metricService.remove(MetricType.AUTO_GAUGE, Metric.DATABASE_NUM.toString());

    List<String> databases = getClusterSchemaManager().getDatabaseNames();
    for (String database : databases) {
      unbindDatabasePartitionMetrics(database);
    }
  }

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }

  private ClusterSchemaManager getClusterSchemaManager() {
    return configManager.getClusterSchemaManager();
  }

  private PartitionManager getPartitionManager() {
    return configManager.getPartitionManager();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PartitionMetrics that = (PartitionMetrics) o;
    return configManager.equals(that.configManager);
  }

  @Override
  public int hashCode() {
    return Objects.hash(configManager);
  }
}
