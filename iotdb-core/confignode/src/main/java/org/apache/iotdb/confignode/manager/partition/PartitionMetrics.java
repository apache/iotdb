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
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.confignode.exception.DatabaseNotExistsException;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
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
  private static final String DATA = "data";
  private static final String SCHEMA = "schema";

  private final IManager configManager;

  public PartitionMetrics(IManager configManager) {
    this.configManager = configManager;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    bindRegionPartitionMetrics(metricService);
    bindDataNodePartitionMetrics(metricService);
    bindDatabaseRelatedMetrics(metricService);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    unbindRegionPartitionMetrics(metricService);
    unbindDataNodePartitionMetrics(metricService);
    unbindDatabaseRelatedMetrics(metricService);
  }

  // region DataNode Partition Metrics

  private void bindDataNodePartitionMetrics(AbstractMetricService metricService) {
    List<TDataNodeConfiguration> registerDataNodes = getNodeManager().getRegisteredDataNodes();
    for (TDataNodeConfiguration dataNodeConfiguration : registerDataNodes) {
      int dataNodeId = dataNodeConfiguration.getLocation().getDataNodeId();
      bindDataNodePartitionMetricsWhenUpdate(metricService, configManager, dataNodeId);
    }
  }

  private void unbindDataNodePartitionMetrics(AbstractMetricService metricService) {
    List<TDataNodeConfiguration> registerDataNodes = getNodeManager().getRegisteredDataNodes();
    for (TDataNodeConfiguration dataNodeConfiguration : registerDataNodes) {
      String dataNodeName =
          NodeUrlUtils.convertTEndPointUrl(
              dataNodeConfiguration.getLocation().getClientRpcEndPoint());
      unbindDataNodePartitionMetricsWhenUpdate(metricService, dataNodeName);
    }
  }

  public static void bindDataNodePartitionMetricsWhenUpdate(
      AbstractMetricService metricService, IManager configManager, int dataNodeId) {
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

    // Count the number of scatter width in the specified DataNode
    metricService.createAutoGauge(
        Metric.SCATTER_WIDTH_NUM_IN_DATA_NODE.toString(),
        MetricLevel.CORE,
        partitionManager,
        obj -> obj.countDataNodeScatterWidth(dataNodeId, TConsensusGroupType.SchemaRegion),
        Tag.NAME.toString(),
        dataNodeName,
        Tag.TYPE.toString(),
        TConsensusGroupType.SchemaRegion.toString());
    metricService.createAutoGauge(
        Metric.SCATTER_WIDTH_NUM_IN_DATA_NODE.toString(),
        MetricLevel.CORE,
        partitionManager,
        obj -> obj.countDataNodeScatterWidth(dataNodeId, TConsensusGroupType.DataRegion),
        Tag.NAME.toString(),
        dataNodeName,
        Tag.TYPE.toString(),
        TConsensusGroupType.DataRegion.toString());
  }

  public static void unbindDataNodePartitionMetricsWhenUpdate(
      AbstractMetricService metricService, String dataNodeName) {
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

    // Remove the number of scatter width in the specified DataNode
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SCATTER_WIDTH_NUM_IN_DATA_NODE.toString(),
        Tag.NAME.toString(),
        dataNodeName,
        Tag.TYPE.toString(),
        TConsensusGroupType.SchemaRegion.toString());
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SCATTER_WIDTH_NUM_IN_DATA_NODE.toString(),
        Tag.NAME.toString(),
        dataNodeName,
        Tag.TYPE.toString(),
        TConsensusGroupType.DataRegion.toString());
  }

  // endregion

  // region Region Partition Metrics

  private void bindRegionPartitionMetrics(AbstractMetricService metricService) {
    for (RegionStatus status : RegionStatus.values()) {
      // Count the number of SchemaRegions
      metricService.createAutoGauge(
          Metric.REGION_NUM.toString(),
          MetricLevel.CORE,
          getLoadManager(),
          loadManager ->
              loadManager.countRegionWithSpecifiedStatus(TConsensusGroupType.SchemaRegion, status),
          Tag.TYPE.toString(),
          TConsensusGroupType.SchemaRegion.toString(),
          Tag.STATUS.toString(),
          status.getStatus());

      // Count the number of DataRegions
      metricService.createAutoGauge(
          Metric.REGION_NUM.toString(),
          MetricLevel.CORE,
          getLoadManager(),
          loadManager ->
              loadManager.countRegionWithSpecifiedStatus(TConsensusGroupType.DataRegion, status),
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

  // endregion

  // region Database Partition Metrics

  private void bindDatabaseRelatedMetrics(AbstractMetricService metricService) {
    ClusterSchemaManager clusterSchemaManager = getClusterSchemaManager();
    // Count the number of Databases
    metricService.createAutoGauge(
        Metric.DATABASE_NUM.toString(),
        MetricLevel.CORE,
        clusterSchemaManager,
        c -> c.getDatabaseNames(null).size());

    List<String> databases = clusterSchemaManager.getDatabaseNames(null);
    for (String database : databases) {
      int dataReplicationFactor = 1;
      int schemaReplicationFactor = 1;
      try {
        dataReplicationFactor =
            clusterSchemaManager.getReplicationFactor(database, TConsensusGroupType.DataRegion);
        schemaReplicationFactor =
            clusterSchemaManager.getReplicationFactor(database, TConsensusGroupType.SchemaRegion);
      } catch (DatabaseNotExistsException e) {
        // ignore
      }
      bindDatabaseRelatedMetricsWhenUpdate(
          metricService, configManager, database, dataReplicationFactor, schemaReplicationFactor);
    }
  }

  private void unbindDatabaseRelatedMetrics(AbstractMetricService metricService) {
    // Remove the number of Databases
    metricService.remove(MetricType.AUTO_GAUGE, Metric.DATABASE_NUM.toString());

    List<String> databases = getClusterSchemaManager().getDatabaseNames(null);
    for (String database : databases) {
      unbindDatabaseRelatedMetricsWhenUpdate(metricService, database);
    }
  }

  public static void bindDatabaseRelatedMetricsWhenUpdate(
      AbstractMetricService metricService,
      IManager configManager,
      String database,
      int dataReplicationFactor,
      int schemaReplicationFactor) {
    bindDatabasePartitionMetricsWhenUpdate(metricService, configManager, database);
    bindDatabaseReplicationFactorMetricsWhenUpdate(
        metricService, database, dataReplicationFactor, schemaReplicationFactor);
  }

  private static void bindDatabasePartitionMetricsWhenUpdate(
      AbstractMetricService metricService, IManager configManager, String database) {
    PartitionManager partitionManager = configManager.getPartitionManager();

    // Count the number of SeriesSlots in the specified Database
    metricService.createAutoGauge(
        Metric.SERIES_SLOT_NUM_IN_DATABASE.toString(),
        MetricLevel.CORE,
        partitionManager,
        manager -> manager.getAssignedSeriesPartitionSlotsCount(database),
        Tag.NAME.toString(),
        database);

    // Count the number of TimeSlots in the specified Database
    metricService.createAutoGauge(
        Metric.TIME_SLOT_NUM_IN_DATABASE.toString(),
        MetricLevel.CORE,
        partitionManager,
        manager -> manager.getAssignedTimePartitionSlotsCount(database),
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
            LOGGER.info("Error when counting SchemaRegionGroups in Database: {}", database, e);
            return 0;
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
            LOGGER.info("Error when counting DataRegionGroups in Database: {}", database, e);
            return 0;
          }
        },
        Tag.NAME.toString(),
        database,
        Tag.TYPE.toString(),
        TConsensusGroupType.DataRegion.toString());
  }

  public static void unbindDatabaseRelatedMetricsWhenUpdate(
      AbstractMetricService metricService, String database) {
    // Remove the number of SeriesSlots in the specified Database
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SERIES_SLOT_NUM_IN_DATABASE.toString(),
        Tag.NAME.toString(),
        database);

    // Remove the number of TimeSlots in the specified Database
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.TIME_SLOT_NUM_IN_DATABASE.toString(),
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

    // Remove database replication factor metric
    metricService.remove(
        MetricType.GAUGE,
        Metric.REPLICATION_FACTOR.toString(),
        Tag.TYPE.toString(),
        DATA,
        Tag.DATABASE.toString(),
        database);
    metricService.remove(
        MetricType.GAUGE,
        Metric.REPLICATION_FACTOR.toString(),
        Tag.TYPE.toString(),
        SCHEMA,
        Tag.DATABASE.toString(),
        database);
  }

  public static void bindDatabaseReplicationFactorMetricsWhenUpdate(
      AbstractMetricService metricService,
      String database,
      int dataReplicationFactor,
      int schemaReplicationFactor) {
    metricService
        .getOrCreateGauge(
            Metric.REPLICATION_FACTOR.toString(),
            MetricLevel.CORE,
            Tag.TYPE.toString(),
            DATA,
            Tag.DATABASE.toString(),
            database)
        .set(dataReplicationFactor);
    metricService
        .getOrCreateGauge(
            Metric.REPLICATION_FACTOR.toString(),
            MetricLevel.CORE,
            Tag.TYPE.toString(),
            SCHEMA,
            Tag.DATABASE.toString(),
            database)
        .set(schemaReplicationFactor);
  }

  // endregion

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }

  private ClusterSchemaManager getClusterSchemaManager() {
    return configManager.getClusterSchemaManager();
  }

  private LoadManager getLoadManager() {
    return configManager.getLoadManager();
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
