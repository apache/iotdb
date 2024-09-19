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

package org.apache.iotdb.confignode.manager.schema;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.TsTableInternalRPCUtil;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.client.CnToDnRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.database.CountDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.read.database.GetDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.read.table.ShowTablePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetAllSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetAllTemplateSetInfoPlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetPathsSetTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetTemplateSetInfoPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.AdjustMaxRegionGroupNumPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetDataReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetSchemaReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTimePartitionIntervalPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeEnrichedPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.AddTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTablePropertiesPlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.DropSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.ExtendSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.PreUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.RollbackPreUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.UnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.response.database.CountDatabaseResp;
import org.apache.iotdb.confignode.consensus.response.database.DatabaseSchemaResp;
import org.apache.iotdb.confignode.consensus.response.partition.PathInfoResp;
import org.apache.iotdb.confignode.consensus.response.table.ShowTableResp;
import org.apache.iotdb.confignode.consensus.response.template.AllTemplateSetInfoResp;
import org.apache.iotdb.confignode.consensus.response.template.TemplateInfoResp;
import org.apache.iotdb.confignode.consensus.response.template.TemplateSetInfoResp;
import org.apache.iotdb.confignode.exception.DatabaseNotExistsException;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.manager.partition.PartitionMetrics;
import org.apache.iotdb.confignode.persistence.schema.ClusterSchemaInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPathsSetTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTemplateResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTableResp;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.db.schemaengine.template.TemplateInternalRPCUpdateType;
import org.apache.iotdb.db.schemaengine.template.TemplateInternalRPCUtil;
import org.apache.iotdb.db.schemaengine.template.alter.TemplateExtendInfo;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTemplateReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.SchemaConstant.ROOT;

/** The ClusterSchemaManager Manages cluster schemaengine read and write requests. */
public class ClusterSchemaManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterSchemaManager.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final double SCHEMA_REGION_PER_DATA_NODE = CONF.getSchemaRegionPerDataNode();
  private static final double DATA_REGION_PER_DATA_NODE = CONF.getDataRegionPerDataNode();

  private final IManager configManager;
  private final ClusterSchemaInfo clusterSchemaInfo;
  private final ClusterSchemaQuotaStatistics schemaQuotaStatistics;
  private final ReentrantLock createDatabaseLock = new ReentrantLock();

  private static final String CONSENSUS_READ_ERROR =
      "Failed in the read API executing the consensus layer due to: ";

  private static final String CONSENSUS_WRITE_ERROR =
      "Failed in the write API executing the consensus layer due to: ";

  public ClusterSchemaManager(
      IManager configManager,
      ClusterSchemaInfo clusterSchemaInfo,
      ClusterSchemaQuotaStatistics schemaQuotaStatistics) {
    this.configManager = configManager;
    this.clusterSchemaInfo = clusterSchemaInfo;
    this.schemaQuotaStatistics = schemaQuotaStatistics;
  }

  // ======================================================
  // Consensus read/write interfaces
  // ======================================================

  /** Set Database */
  public TSStatus setDatabase(
      final DatabaseSchemaPlan databaseSchemaPlan, final boolean isGeneratedByPipe) {
    TSStatus result;

    if (getPartitionManager().isDatabasePreDeleted(databaseSchemaPlan.getSchema().getName())) {
      return RpcUtils.getStatus(
          TSStatusCode.METADATA_ERROR,
          String.format(
              "Some other task is deleting database %s", databaseSchemaPlan.getSchema().getName()));
    }

    createDatabaseLock.lock();
    try {
      clusterSchemaInfo.isDatabaseNameValid(databaseSchemaPlan.getSchema().getName());
      if (!databaseSchemaPlan.getSchema().getName().equals(SchemaConstant.SYSTEM_DATABASE)) {
        clusterSchemaInfo.checkDatabaseLimit();
      }
      // Cache DatabaseSchema
      result =
          getConsensusManager()
              .write(
                  isGeneratedByPipe
                      ? new PipeEnrichedPlan(databaseSchemaPlan)
                      : databaseSchemaPlan);
      // set ttl
      if (databaseSchemaPlan.getSchema().isSetTTL()) {
        result = configManager.getTTLManager().setTTL(databaseSchemaPlan, isGeneratedByPipe);
      }
      // Bind Database metrics
      PartitionMetrics.bindDatabaseRelatedMetricsWhenUpdate(
          MetricService.getInstance(),
          configManager,
          databaseSchemaPlan.getSchema().getName(),
          databaseSchemaPlan.getSchema().getDataReplicationFactor(),
          databaseSchemaPlan.getSchema().getSchemaReplicationFactor());
      // Adjust the maximum RegionGroup number of each Database
      adjustMaxRegionGroupNum();
    } catch (final ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      result = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      result.setMessage(e.getMessage());
    } catch (final MetadataException metadataException) {
      // Reject if Database already set
      result = new TSStatus(metadataException.getErrorCode());
      result.setMessage(metadataException.getMessage());
    } finally {
      createDatabaseLock.unlock();
    }

    return result;
  }

  /** Alter Database */
  public TSStatus alterDatabase(DatabaseSchemaPlan databaseSchemaPlan, boolean isGeneratedByPipe) {
    TSStatus result;
    TDatabaseSchema databaseSchema = databaseSchemaPlan.getSchema();

    if (!isDatabaseExist(databaseSchema.getName())) {
      // Reject if Database doesn't exist
      result = new TSStatus(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode());
      result.setMessage(
          "Failed to alter database. The Database " + databaseSchema.getName() + " doesn't exist.");
      return result;
    }

    if (databaseSchema.isSetMinSchemaRegionGroupNum()) {
      // Validate alter SchemaRegionGroupNum
      int minSchemaRegionGroupNum =
          getMinRegionGroupNum(databaseSchema.getName(), TConsensusGroupType.SchemaRegion);
      if (databaseSchema.getMinSchemaRegionGroupNum() <= minSchemaRegionGroupNum) {
        result = new TSStatus(TSStatusCode.DATABASE_CONFIG_ERROR.getStatusCode());
        result.setMessage(
            String.format(
                "Failed to alter database. The SchemaRegionGroupNum could only be increased. "
                    + "Current SchemaRegionGroupNum: %d, Alter SchemaRegionGroupNum: %d",
                minSchemaRegionGroupNum, databaseSchema.getMinSchemaRegionGroupNum()));
        return result;
      }
    }
    if (databaseSchema.isSetMinDataRegionGroupNum()) {
      // Validate alter DataRegionGroupNum
      int minDataRegionGroupNum =
          getMinRegionGroupNum(databaseSchema.getName(), TConsensusGroupType.DataRegion);
      if (databaseSchema.getMinDataRegionGroupNum() <= minDataRegionGroupNum) {
        result = new TSStatus(TSStatusCode.DATABASE_CONFIG_ERROR.getStatusCode());
        result.setMessage(
            String.format(
                "Failed to alter database. The DataRegionGroupNum could only be increased. "
                    + "Current DataRegionGroupNum: %d, Alter DataRegionGroupNum: %d",
                minDataRegionGroupNum, databaseSchema.getMinDataRegionGroupNum()));
        return result;
      }
    }

    // Alter DatabaseSchema
    try {
      result =
          getConsensusManager()
              .write(
                  isGeneratedByPipe
                      ? new PipeEnrichedPlan(databaseSchemaPlan)
                      : databaseSchemaPlan);
      PartitionMetrics.bindDatabaseReplicationFactorMetricsWhenUpdate(
          MetricService.getInstance(),
          databaseSchemaPlan.getSchema().getName(),
          databaseSchemaPlan.getSchema().getDataReplicationFactor(),
          databaseSchemaPlan.getSchema().getSchemaReplicationFactor());
      return result;
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      result = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      result.setMessage(e.getMessage());
      return result;
    }
  }

  /** Delete DatabaseSchema. */
  public TSStatus deleteDatabase(DeleteDatabasePlan deleteDatabasePlan, boolean isGeneratedByPipe) {
    TSStatus result;
    try {
      result =
          getConsensusManager()
              .write(
                  isGeneratedByPipe
                      ? new PipeEnrichedPlan(deleteDatabasePlan)
                      : deleteDatabasePlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      result = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      result.setMessage(e.getMessage());
    }
    if (result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      adjustMaxRegionGroupNum();
    }
    return result;
  }

  /**
   * Count Databases by specified path pattern. Notice: including pre-deleted Database.
   *
   * <p>Notice: Only invoke this interface in ConfigManager
   *
   * @return CountDatabaseResp
   */
  public CountDatabaseResp countMatchedDatabases(CountDatabasePlan countDatabasePlan) {
    try {
      return (CountDatabaseResp) getConsensusManager().read(countDatabasePlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_READ_ERROR, e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      CountDatabaseResp response = new CountDatabaseResp();
      response.setStatus(res);
      return response;
    }
  }

  /**
   * Get DatabaseSchemas by specified path pattern. Notice: including pre-deleted Database
   *
   * <p>Notice: Only invoke this interface in ConfigManager
   *
   * @return DatabaseSchemaResp
   */
  public DatabaseSchemaResp getMatchedDatabaseSchema(GetDatabasePlan getDatabasePlan) {
    DatabaseSchemaResp resp;
    try {
      resp = (DatabaseSchemaResp) getConsensusManager().read(getDatabasePlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_READ_ERROR, e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      resp = new DatabaseSchemaResp();
      resp.setStatus(res);
    }
    List<String> preDeletedDatabaseList = new ArrayList<>();
    for (String database : resp.getSchemaMap().keySet()) {
      if (getPartitionManager().isDatabasePreDeleted(database)) {
        preDeletedDatabaseList.add(database);
      }
    }
    for (String preDeletedDatabase : preDeletedDatabaseList) {
      resp.getSchemaMap().remove(preDeletedDatabase);
    }
    return resp;
  }

  /** Only used in cluster tool show Databases. */
  public TShowDatabaseResp showDatabase(GetDatabasePlan getDatabasePlan) {
    DatabaseSchemaResp databaseSchemaResp;
    try {
      databaseSchemaResp = (DatabaseSchemaResp) getConsensusManager().read(getDatabasePlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_READ_ERROR, e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      databaseSchemaResp = new DatabaseSchemaResp();
      databaseSchemaResp.setStatus(res);
    }
    if (databaseSchemaResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // Return immediately if some Database doesn't exist
      return new TShowDatabaseResp().setStatus(databaseSchemaResp.getStatus());
    }

    Map<String, TDatabaseInfo> infoMap = new ConcurrentHashMap<>();
    for (TDatabaseSchema databaseSchema : databaseSchemaResp.getSchemaMap().values()) {
      String database = databaseSchema.getName();
      TDatabaseInfo databaseInfo = new TDatabaseInfo();
      databaseInfo.setName(database);
      databaseInfo.setSchemaReplicationFactor(databaseSchema.getSchemaReplicationFactor());
      databaseInfo.setDataReplicationFactor(databaseSchema.getDataReplicationFactor());
      databaseInfo.setTimePartitionOrigin(databaseSchema.getTimePartitionOrigin());
      databaseInfo.setTimePartitionInterval(databaseSchema.getTimePartitionInterval());
      databaseInfo.setMinSchemaRegionNum(
          getMinRegionGroupNum(database, TConsensusGroupType.SchemaRegion));
      databaseInfo.setMaxSchemaRegionNum(
          getMaxRegionGroupNum(database, TConsensusGroupType.SchemaRegion));
      databaseInfo.setMinDataRegionNum(
          getMinRegionGroupNum(database, TConsensusGroupType.DataRegion));
      databaseInfo.setMaxDataRegionNum(
          getMaxRegionGroupNum(database, TConsensusGroupType.DataRegion));

      try {
        databaseInfo.setSchemaRegionNum(
            getPartitionManager().getRegionGroupCount(database, TConsensusGroupType.SchemaRegion));
        databaseInfo.setDataRegionNum(
            getPartitionManager().getRegionGroupCount(database, TConsensusGroupType.DataRegion));
      } catch (DatabaseNotExistsException e) {
        // Skip pre-deleted Database
        LOGGER.warn(
            "The Database: {} doesn't exist. Maybe it has been pre-deleted.",
            databaseSchema.getName());
        continue;
      }

      infoMap.put(database, databaseInfo);
    }

    return new TShowDatabaseResp().setDatabaseInfoMap(infoMap).setStatus(StatusUtils.OK);
  }

  public Map<String, Long> getTTLInfoForUpgrading() {
    List<String> databases = getDatabaseNames();
    Map<String, Long> infoMap = new ConcurrentHashMap<>();
    for (String database : databases) {
      try {
        final TDatabaseSchema databaseSchema = getDatabaseSchemaByName(database);
        long ttl = databaseSchema.isSetTTL() ? databaseSchema.getTTL() : -1;
        if (ttl < 0 || ttl == Long.MAX_VALUE) {
          continue;
        }
        infoMap.put(database, ttl);
      } catch (DatabaseNotExistsException e) {
        LOGGER.warn("Database: {} doesn't exist", databases, e);
      }
    }
    return infoMap;
  }

  public TSStatus setSchemaReplicationFactor(
      SetSchemaReplicationFactorPlan setSchemaReplicationFactorPlan) {
    // TODO: Inform DataNodes
    try {
      return getConsensusManager().write(setSchemaReplicationFactorPlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      TSStatus result = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      result.setMessage(e.getMessage());
      return result;
    }
  }

  public TSStatus setDataReplicationFactor(
      SetDataReplicationFactorPlan setDataReplicationFactorPlan) {
    // TODO: Inform DataNodes
    try {
      return getConsensusManager().write(setDataReplicationFactorPlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      TSStatus result = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      result.setMessage(e.getMessage());
      return result;
    }
  }

  public TSStatus setTimePartitionInterval(
      SetTimePartitionIntervalPlan setTimePartitionIntervalPlan) {
    // TODO: Inform DataNodes
    try {
      return getConsensusManager().write(setTimePartitionIntervalPlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      TSStatus result = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      result.setMessage(e.getMessage());
      return result;
    }
  }

  /**
   * Only leader use this interface. Adjust the maxSchemaRegionGroupNum and maxDataRegionGroupNum of
   * each Database based on existing cluster resources
   */
  public synchronized void adjustMaxRegionGroupNum() {
    // Get all DatabaseSchemas
    Map<String, TDatabaseSchema> databaseSchemaMap =
        getMatchedDatabaseSchemasByName(getDatabaseNames());
    if (databaseSchemaMap.isEmpty()) {
      // Skip when there are no Databases
      return;
    }

    int dataNodeNum = getNodeManager().getRegisteredDataNodeCount();
    int databaseNum = databaseSchemaMap.size();

    for (TDatabaseSchema databaseSchema : databaseSchemaMap.values()) {
      if (!isDatabaseExist(databaseSchema.getName())
          || databaseSchema.getName().equals(SchemaConstant.SYSTEM_DATABASE)) {
        // filter the pre deleted database and the system database
        databaseNum--;
      }
    }

    AdjustMaxRegionGroupNumPlan adjustMaxRegionGroupNumPlan = new AdjustMaxRegionGroupNumPlan();
    for (TDatabaseSchema databaseSchema : databaseSchemaMap.values()) {
      if (databaseSchema.getName().equals(SchemaConstant.SYSTEM_DATABASE)) {
        // filter the system database
        continue;
      }

      try {
        // Adjust maxSchemaRegionGroupNum for each Database.
        // All Databases share the DataNodes equally.
        // The allocated SchemaRegionGroups will not be shrunk.
        int allocatedSchemaRegionGroupCount;
        try {
          allocatedSchemaRegionGroupCount =
              getPartitionManager()
                  .getRegionGroupCount(databaseSchema.getName(), TConsensusGroupType.SchemaRegion);
        } catch (DatabaseNotExistsException e) {
          // ignore the pre deleted database
          continue;
        }

        int maxSchemaRegionGroupNum =
            calcMaxRegionGroupNum(
                databaseSchema.getMinSchemaRegionGroupNum(),
                SCHEMA_REGION_PER_DATA_NODE,
                dataNodeNum,
                databaseNum,
                databaseSchema.getSchemaReplicationFactor(),
                allocatedSchemaRegionGroupCount);
        LOGGER.info(
            "[AdjustRegionGroupNum] The maximum number of SchemaRegionGroups for Database: {} is adjusted to: {}",
            databaseSchema.getName(),
            maxSchemaRegionGroupNum);

        // Adjust maxDataRegionGroupNum for each Database.
        // All Databases share the DataNodes equally.
        // The allocated DataRegionGroups will not be shrunk.
        int allocatedDataRegionGroupCount =
            getPartitionManager()
                .getRegionGroupCount(databaseSchema.getName(), TConsensusGroupType.DataRegion);
        int maxDataRegionGroupNum =
            calcMaxRegionGroupNum(
                databaseSchema.getMinDataRegionGroupNum(),
                DATA_REGION_PER_DATA_NODE,
                dataNodeNum,
                databaseNum,
                databaseSchema.getDataReplicationFactor(),
                allocatedDataRegionGroupCount);
        LOGGER.info(
            "[AdjustRegionGroupNum] The maximum number of DataRegionGroups for Database: {} is adjusted to: {}",
            databaseSchema.getName(),
            maxDataRegionGroupNum);

        adjustMaxRegionGroupNumPlan.putEntry(
            databaseSchema.getName(), new Pair<>(maxSchemaRegionGroupNum, maxDataRegionGroupNum));
      } catch (DatabaseNotExistsException e) {
        LOGGER.warn("Adjust maxRegionGroupNum failed because Database doesn't exist", e);
      }
    }
    try {
      getConsensusManager().write(adjustMaxRegionGroupNumPlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
    }
  }

  public static int calcMaxRegionGroupNum(
      int minRegionGroupNum,
      double resourceWeight,
      int resource,
      int databaseNum,
      int replicationFactor,
      int allocatedRegionGroupCount) {
    return Math.max(
        // The maxRegionGroupNum should be great or equal to the minRegionGroupNum
        minRegionGroupNum,
        Math.max(
            (int)
                // Use Math.ceil here to ensure that the maxRegionGroupNum
                // will be increased as long as the number of cluster DataNodes is increased
                Math.ceil(
                    // The maxRegionGroupNum of the current Database is expected to be:
                    // (resourceWeight * resource) / (createdDatabaseNum * replicationFactor)
                    resourceWeight * resource / (databaseNum * replicationFactor)),
            // The maxRegionGroupNum should be great or equal to the allocatedRegionGroupCount
            allocatedRegionGroupCount));
  }

  // ======================================================
  // Leader scheduling interfaces
  // ======================================================

  /**
   * Check if the specified Database exists
   *
   * @param database The specified Database
   * @return True if the DatabaseSchema is exists and the Database is not pre-deleted
   */
  public boolean isDatabaseExist(String database) {
    return getPartitionManager().isDatabaseExist(database);
  }

  /**
   * Only leader use this interface. Get all Databases name
   *
   * @return List<DatabaseName>, all Databases' name
   */
  public List<String> getDatabaseNames() {
    return clusterSchemaInfo.getDatabaseNames().stream()
        .filter(this::isDatabaseExist)
        .collect(Collectors.toList());
  }

  /**
   * Only leader use this interface. Get the specified Database's schemaengine
   *
   * @param database DatabaseName
   * @return The specific DatabaseSchema
   * @throws DatabaseNotExistsException When the specific Database doesn't exist
   */
  public TDatabaseSchema getDatabaseSchemaByName(String database)
      throws DatabaseNotExistsException {
    if (!isDatabaseExist(database)) {
      throw new DatabaseNotExistsException(database);
    }
    return clusterSchemaInfo.getMatchedDatabaseSchemaByName(database);
  }

  /**
   * Only leader use this interface.
   *
   * @return The DatabaseName of the specified Device. Empty String if not exists.
   */
  public String getDatabaseNameByDevice(IDeviceID deviceID) {
    List<String> databases = getDatabaseNames();
    for (String database : databases) {
      if (PathUtils.isStartWith(deviceID, database)) {
        return database;
      }
    }
    return "";
  }

  /**
   * Only leader use this interface. Get the specified Databases' schemaengine
   *
   * @param rawPathList List<DatabaseName>
   * @return the matched DatabaseSchemas
   */
  public Map<String, TDatabaseSchema> getMatchedDatabaseSchemasByName(
      final List<String> rawPathList) {
    final Map<String, TDatabaseSchema> result = new ConcurrentHashMap<>();
    clusterSchemaInfo
        .getMatchedDatabaseSchemasByName(rawPathList)
        .forEach(
            (database, databaseSchema) -> {
              if (isDatabaseExist(database)) {
                result.put(database, databaseSchema);
              }
            });
    return result;
  }

  /**
   * Only leader use this interface. Get the specified Databases' schemaengine
   *
   * @param prefix prefix full path
   * @return the matched DatabaseSchemas
   */
  public Map<String, TDatabaseSchema> getMatchedDatabaseSchemasByPrefix(PartialPath prefix) {
    Map<String, TDatabaseSchema> result = new ConcurrentHashMap<>();
    clusterSchemaInfo
        .getMatchedDatabaseSchemasByPrefix(prefix)
        .forEach(
            (database, databaseSchema) -> {
              if (isDatabaseExist(database)) {
                result.put(database, databaseSchema);
              }
            });
    return result;
  }

  /**
   * Only leader use this interface. Get the replication factor of specified Database
   *
   * @param database DatabaseName
   * @param consensusGroupType SchemaRegion or DataRegion
   * @return SchemaReplicationFactor or DataReplicationFactor
   * @throws DatabaseNotExistsException When the specific Database doesn't exist
   */
  public int getReplicationFactor(String database, TConsensusGroupType consensusGroupType)
      throws DatabaseNotExistsException {
    TDatabaseSchema databaseSchema = getDatabaseSchemaByName(database);
    return TConsensusGroupType.SchemaRegion.equals(consensusGroupType)
        ? databaseSchema.getSchemaReplicationFactor()
        : databaseSchema.getDataReplicationFactor();
  }

  /**
   * Only leader use this interface. Get the maxRegionGroupNum of specified Database.
   *
   * @param database DatabaseName
   * @param consensusGroupType SchemaRegion or DataRegion
   * @return minSchemaRegionGroupNum or minDataRegionGroupNum
   */
  public int getMinRegionGroupNum(String database, TConsensusGroupType consensusGroupType) {
    return clusterSchemaInfo.getMinRegionGroupNum(database, consensusGroupType);
  }

  /**
   * Only leader use this interface. Get the maxRegionGroupNum of specified Database.
   *
   * @param database DatabaseName
   * @param consensusGroupType SchemaRegion or DataRegion
   * @return maxSchemaRegionGroupNum or maxDataRegionGroupNum
   */
  public int getMaxRegionGroupNum(String database, TConsensusGroupType consensusGroupType) {
    return clusterSchemaInfo.getMaxRegionGroupNum(database, consensusGroupType);
  }

  /**
   * create schemaengine template
   *
   * @param createSchemaTemplatePlan CreateSchemaTemplatePlan
   * @return TSStatus
   */
  public TSStatus createTemplate(CreateSchemaTemplatePlan createSchemaTemplatePlan) {
    try {
      return getConsensusManager().write(createSchemaTemplatePlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return res;
    }
  }

  /**
   * show schemaengine template
   *
   * @return TGetAllTemplatesResp
   */
  public TGetAllTemplatesResp getAllTemplates() {
    GetAllSchemaTemplatePlan getAllSchemaTemplatePlan = new GetAllSchemaTemplatePlan();
    TemplateInfoResp templateResp;
    try {
      templateResp = (TemplateInfoResp) getConsensusManager().read(getAllSchemaTemplatePlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_READ_ERROR, e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      templateResp = new TemplateInfoResp();
      templateResp.setStatus(res);
    }
    TGetAllTemplatesResp resp = new TGetAllTemplatesResp();
    resp.setStatus(templateResp.getStatus());
    if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && templateResp.getTemplateList() != null) {
      List<ByteBuffer> list = new ArrayList<>();
      templateResp.getTemplateList().forEach(template -> list.add(template.serialize()));
      resp.setTemplateList(list);
    }
    return resp;
  }

  /** show nodes in schemaengine template */
  public TGetTemplateResp getTemplate(String req) {
    GetSchemaTemplatePlan getSchemaTemplatePlan = new GetSchemaTemplatePlan(req);
    TemplateInfoResp templateResp;
    try {
      templateResp = (TemplateInfoResp) getConsensusManager().read(getSchemaTemplatePlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_READ_ERROR, e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      templateResp = new TemplateInfoResp();
      templateResp.setStatus(res);
    }
    TGetTemplateResp resp = new TGetTemplateResp();
    if (templateResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && templateResp.getTemplateList() != null
        && !templateResp.getTemplateList().isEmpty()) {
      ByteBuffer byteBuffer = templateResp.getTemplateList().get(0).serialize();
      resp.setTemplate(byteBuffer);
    }
    resp.setStatus(templateResp.getStatus());
    return resp;
  }

  /** Get template by id. Only leader uses this interface. */
  public Template getTemplate(int id) throws MetadataException {
    return clusterSchemaInfo.getTemplate(id);
  }

  /** show path set template xx */
  public TGetPathsSetTemplatesResp getPathsSetTemplate(String templateName, PathPatternTree scope) {
    GetPathsSetTemplatePlan getPathsSetTemplatePlan =
        new GetPathsSetTemplatePlan(templateName, scope);
    PathInfoResp pathInfoResp;
    try {
      pathInfoResp = (PathInfoResp) getConsensusManager().read(getPathsSetTemplatePlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_READ_ERROR, e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      pathInfoResp = new PathInfoResp();
      pathInfoResp.setStatus(res);
    }
    if (pathInfoResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      TGetPathsSetTemplatesResp resp = new TGetPathsSetTemplatesResp();
      resp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
      resp.setPathList(pathInfoResp.getPathList());
      return resp;
    } else {
      return new TGetPathsSetTemplatesResp(pathInfoResp.getStatus());
    }
  }

  /**
   * get all template set and pre-set info to sync to a registering dataNodes, the pre unset
   * template info won't be taken
   */
  public byte[] getAllTemplateSetInfo() {
    try {
      AllTemplateSetInfoResp resp =
          (AllTemplateSetInfoResp) getConsensusManager().read(new GetAllTemplateSetInfoPlan());
      return resp.getTemplateInfo();
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_READ_ERROR, e);
      return new byte[0];
    }
  }

  public TemplateSetInfoResp getTemplateSetInfo(List<PartialPath> patternList) {
    try {
      return (TemplateSetInfoResp)
          getConsensusManager().read(new GetTemplateSetInfoPlan(patternList));
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_READ_ERROR, e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      TemplateSetInfoResp response = new TemplateSetInfoResp();
      response.setStatus(res);
      return response;
    }
  }

  public Pair<TSStatus, Template> checkIsTemplateSetOnPath(String templateName, String path) {
    GetSchemaTemplatePlan getSchemaTemplatePlan = new GetSchemaTemplatePlan(templateName);
    TemplateInfoResp templateResp;
    try {
      templateResp = (TemplateInfoResp) getConsensusManager().read(getSchemaTemplatePlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_READ_ERROR, e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      templateResp = new TemplateInfoResp();
      templateResp.setStatus(res);
    }
    if (templateResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      if (templateResp.getTemplateList() == null || templateResp.getTemplateList().isEmpty()) {
        return new Pair<>(
            RpcUtils.getStatus(
                TSStatusCode.UNDEFINED_TEMPLATE.getStatusCode(),
                String.format("Undefined template name: %s", templateName)),
            null);
      }
    } else {
      return new Pair<>(templateResp.getStatus(), null);
    }

    GetPathsSetTemplatePlan getPathsSetTemplatePlan =
        new GetPathsSetTemplatePlan(templateName, SchemaConstant.ALL_MATCH_SCOPE);
    PathInfoResp pathInfoResp;
    try {
      pathInfoResp = (PathInfoResp) getConsensusManager().read(getPathsSetTemplatePlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_READ_ERROR, e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      pathInfoResp = new PathInfoResp();
      pathInfoResp.setStatus(res);
    }
    if (pathInfoResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      List<String> templateSetPathList = pathInfoResp.getPathList();
      if (templateSetPathList == null
          || templateSetPathList.isEmpty()
          || !pathInfoResp.getPathList().contains(path)) {
        return new Pair<>(
            RpcUtils.getStatus(
                TSStatusCode.TEMPLATE_NOT_SET.getStatusCode(),
                String.format("No template on %s", path)),
            null);
      } else {
        return new Pair<>(templateResp.getStatus(), templateResp.getTemplateList().get(0));
      }
    } else {
      return new Pair<>(pathInfoResp.getStatus(), null);
    }
  }

  public TSStatus preUnsetSchemaTemplate(int templateId, PartialPath path) {
    try {
      return getConsensusManager().write(new PreUnsetSchemaTemplatePlan(templateId, path));
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      TSStatus result = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      result.setMessage(e.getMessage());
      return result;
    }
  }

  public TSStatus rollbackPreUnsetSchemaTemplate(int templateId, PartialPath path) {
    try {
      return getConsensusManager().write(new RollbackPreUnsetSchemaTemplatePlan(templateId, path));
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      TSStatus result = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      result.setMessage(e.getMessage());
      return result;
    }
  }

  public TSStatus unsetSchemaTemplateInBlackList(
      int templateId, PartialPath path, boolean isGeneratedByPipe) {
    try {
      return getConsensusManager()
          .write(
              isGeneratedByPipe
                  ? new PipeEnrichedPlan(new UnsetSchemaTemplatePlan(templateId, path))
                  : new UnsetSchemaTemplatePlan(templateId, path));
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      TSStatus result = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      result.setMessage(e.getMessage());
      return result;
    }
  }

  public synchronized TSStatus dropSchemaTemplate(String templateName) {

    // check template existence
    GetSchemaTemplatePlan getSchemaTemplatePlan = new GetSchemaTemplatePlan(templateName);
    TemplateInfoResp templateInfoResp;
    try {
      templateInfoResp = (TemplateInfoResp) getConsensusManager().read(getSchemaTemplatePlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_READ_ERROR, e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      templateInfoResp = new TemplateInfoResp();
      templateInfoResp.setStatus(res);
    }
    if (templateInfoResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return templateInfoResp.getStatus();
    } else if (templateInfoResp.getTemplateList() == null
        || templateInfoResp.getTemplateList().isEmpty()) {
      return RpcUtils.getStatus(
          TSStatusCode.UNDEFINED_TEMPLATE.getStatusCode(),
          String.format("Undefined template name: %s", templateName));
    }

    // check is template set on some path, block all template set operation
    GetPathsSetTemplatePlan getPathsSetTemplatePlan =
        new GetPathsSetTemplatePlan(templateName, SchemaConstant.ALL_MATCH_SCOPE);
    PathInfoResp pathInfoResp;
    try {
      pathInfoResp = (PathInfoResp) getConsensusManager().read(getPathsSetTemplatePlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_READ_ERROR, e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      pathInfoResp = new PathInfoResp();
      pathInfoResp.setStatus(res);
    }
    if (pathInfoResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return pathInfoResp.getStatus();
    } else if (pathInfoResp.getPathList() != null && !pathInfoResp.getPathList().isEmpty()) {
      return RpcUtils.getStatus(
          TSStatusCode.METADATA_ERROR.getStatusCode(),
          String.format(
              "Template [%s] has been set on MTree, cannot be dropped now.", templateName));
    }

    // execute drop template
    try {
      return getConsensusManager().write(new DropSchemaTemplatePlan(templateName));
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      TSStatus result = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      result.setMessage(e.getMessage());
      return result;
    }
  }

  public synchronized TSStatus extendSchemaTemplate(
      TemplateExtendInfo templateExtendInfo, boolean isGeneratedByPipe) {
    if (templateExtendInfo.getEncodings() != null) {
      for (int i = 0; i < templateExtendInfo.getDataTypes().size(); i++) {
        try {
          SchemaUtils.checkDataTypeWithEncoding(
              templateExtendInfo.getDataTypes().get(i), templateExtendInfo.getEncodings().get(i));
        } catch (MetadataException e) {
          return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
        }
      }
    }

    TemplateInfoResp resp =
        clusterSchemaInfo.getTemplate(
            new GetSchemaTemplatePlan(templateExtendInfo.getTemplateName()));
    if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return resp.getStatus();
    }

    Template template = resp.getTemplateList().get(0);
    List<String> intersectionMeasurements =
        templateExtendInfo.updateAsDifferenceAndGetIntersection(template.getSchemaMap().keySet());

    if (templateExtendInfo.isEmpty()) {
      if (intersectionMeasurements.isEmpty()) {
        return RpcUtils.SUCCESS_STATUS;
      } else {
        return RpcUtils.getStatus(
            TSStatusCode.MEASUREMENT_ALREADY_EXISTS_IN_TEMPLATE,
            String.format(
                "Measurement %s already exist in schemaengine template %s",
                intersectionMeasurements, template.getName()));
      }
    }

    ExtendSchemaTemplatePlan extendSchemaTemplatePlan =
        new ExtendSchemaTemplatePlan(templateExtendInfo);
    TSStatus status;
    try {
      status =
          getConsensusManager()
              .write(
                  isGeneratedByPipe
                      ? new PipeEnrichedPlan(extendSchemaTemplatePlan)
                      : extendSchemaTemplatePlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      status = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
    }
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return status;
    }

    template =
        clusterSchemaInfo
            .getTemplate(new GetSchemaTemplatePlan(templateExtendInfo.getTemplateName()))
            .getTemplateList()
            .get(0);

    TUpdateTemplateReq updateTemplateReq = new TUpdateTemplateReq();
    updateTemplateReq.setType(TemplateInternalRPCUpdateType.UPDATE_TEMPLATE_INFO.toByte());
    updateTemplateReq.setTemplateInfo(
        TemplateInternalRPCUtil.generateUpdateTemplateInfoBytes(template));

    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();

    DataNodeAsyncRequestContext<TUpdateTemplateReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnRequestType.UPDATE_TEMPLATE, updateTemplateReq, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    for (Map.Entry<Integer, TSStatus> entry : statusMap.entrySet()) {
      if (entry.getValue().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            "Failed to sync template {} extension info to DataNode {}",
            template.getName(),
            dataNodeLocationMap.get(entry.getKey()));
        return RpcUtils.getStatus(
            TSStatusCode.EXECUTE_STATEMENT_ERROR,
            String.format(
                "Failed to sync template %s extension info to DataNode %s",
                template.getName(), dataNodeLocationMap.get(entry.getKey())));
      }
    }

    if (intersectionMeasurements.isEmpty()) {
      return RpcUtils.SUCCESS_STATUS;
    } else {
      return RpcUtils.getStatus(
          TSStatusCode.MEASUREMENT_ALREADY_EXISTS_IN_TEMPLATE,
          String.format(
              "Measurement %s already exist in schemaengine template %s",
              intersectionMeasurements, template.getName()));
    }
  }

  // region table management

  public TShowTableResp showTables(final String database) {
    try {
      return ((ShowTableResp) configManager.getConsensusManager().read(new ShowTablePlan(database)))
          .convertToTShowTableResp();
    } catch (final ConsensusException e) {
      LOGGER.warn("Failed in the read API executing the consensus layer due to: ", e);
      final TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return new TShowTableResp(res);
    }
  }

  public byte[] getAllTableInfoForDataNodeActivation() {
    return TsTableInternalRPCUtil.serializeTableInitializationInfo(
        clusterSchemaInfo.getAllUsingTables(), clusterSchemaInfo.getAllPreCreateTables());
  }

  // endregion

  /**
   * Only leader use this interface. Get the remain schema quota of specified schema region.
   *
   * @return pair of <series quota, device quota>, -1 means no limit
   */
  public Pair<Long, Long> getSchemaQuotaRemain() {
    boolean isDeviceLimit = schemaQuotaStatistics.getDeviceThreshold() != -1;
    boolean isSeriesLimit = schemaQuotaStatistics.getSeriesThreshold() != -1;
    if (isSeriesLimit || isDeviceLimit) {
      Set<Integer> schemaPartitionSet = getPartitionManager().getAllSchemaPartition();
      return new Pair<>(
          isSeriesLimit ? schemaQuotaStatistics.getSeriesQuotaRemain(schemaPartitionSet) : -1L,
          isDeviceLimit ? schemaQuotaStatistics.getDeviceQuotaRemain(schemaPartitionSet) : -1L);
    } else {
      return new Pair<>(-1L, -1L);
    }
  }

  public void updateTimeSeriesUsage(Map<Integer, Long> seriesUsage) {
    schemaQuotaStatistics.updateTimeSeriesUsage(seriesUsage);
  }

  public void updateDeviceUsage(Map<Integer, Long> deviceUsage) {
    schemaQuotaStatistics.updateDeviceUsage(deviceUsage);
  }

  public void updateSchemaQuotaConfiguration(long seriesThreshold, long deviceThreshold) {
    schemaQuotaStatistics.setDeviceThreshold(deviceThreshold);
    schemaQuotaStatistics.setSeriesThreshold(seriesThreshold);
  }

  public Optional<TsTable> getTableIfExists(final String database, final String tableName) {
    return clusterSchemaInfo.getTsTableIfExists(database, tableName);
  }

  public synchronized Pair<TSStatus, TsTable> tableColumnCheckForColumnExtension(
      final String database,
      final String tableName,
      final List<TsTableColumnSchema> columnSchemaList) {
    final TsTable originalTable = getTableIfExists(database, tableName).orElse(null);

    if (Objects.isNull(originalTable)) {
      return new Pair<>(
          RpcUtils.getStatus(
              TSStatusCode.TABLE_NOT_EXISTS,
              String.format(
                  "Table '%s.%s' does not exist",
                  database.substring(ROOT.length() + 1), tableName)),
          null);
    }

    final TsTable expandedTable = TsTable.deserialize(ByteBuffer.wrap(originalTable.serialize()));

    final String errorMsg =
        String.format(
            "Column '%s' already exist",
            columnSchemaList.stream()
                .map(TsTableColumnSchema::getColumnName)
                .collect(Collectors.joining(", ")));
    columnSchemaList.removeIf(
        columnSchema -> {
          if (Objects.isNull(originalTable.getColumnSchema(columnSchema.getColumnName()))) {
            expandedTable.addColumnSchema(columnSchema);
            return false;
          }
          return true;
        });

    if (columnSchemaList.isEmpty()) {
      return new Pair<>(RpcUtils.getStatus(TSStatusCode.COLUMN_ALREADY_EXISTS, errorMsg), null);
    }
    return new Pair<>(RpcUtils.SUCCESS_STATUS, expandedTable);
  }

  public synchronized TSStatus addTableColumn(
      final String database,
      final String tableName,
      final List<TsTableColumnSchema> columnSchemaList) {
    final AddTableColumnPlan addTableColumnPlan =
        new AddTableColumnPlan(database, tableName, columnSchemaList, false);
    try {
      return getConsensusManager().write(addTableColumnPlan);
    } catch (final ConsensusException e) {
      LOGGER.warn(e.getMessage(), e);
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  public synchronized TSStatus rollbackAddTableColumn(
      final String database,
      final String tableName,
      final List<TsTableColumnSchema> columnSchemaList) {
    final AddTableColumnPlan addTableColumnPlan =
        new AddTableColumnPlan(database, tableName, columnSchemaList, true);
    try {
      return getConsensusManager().write(addTableColumnPlan);
    } catch (final ConsensusException e) {
      LOGGER.warn(e.getMessage(), e);
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  public synchronized TSStatus setTableProperties(
      final String database, final String tableName, final Map<String, String> properties) {
    final SetTablePropertiesPlan setTablePropertiesPlan =
        new SetTablePropertiesPlan(database, tableName, properties);
    try {
      return getConsensusManager().write(setTablePropertiesPlan);
    } catch (final ConsensusException e) {
      LOGGER.warn(e.getMessage(), e);
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  public synchronized Pair<TSStatus, TsTable> updateTableProperties(
      final String database,
      final String tableName,
      final Map<String, String> originalProperties,
      final Map<String, String> updatedProperties) {
    final TsTable originalTable = getTableIfExists(database, tableName).orElse(null);

    if (Objects.isNull(originalTable)) {
      return new Pair<>(
          RpcUtils.getStatus(
              TSStatusCode.TABLE_NOT_EXISTS,
              String.format(
                  "Table '%s.%s' does not exist",
                  database.substring(ROOT.length() + 1), tableName)),
          null);
    }

    updatedProperties
        .keySet()
        .removeIf(
            key ->
                Objects.equals(
                    updatedProperties.get(key), originalTable.getPropValue(key).orElse(null)));
    if (updatedProperties.isEmpty()) {
      return new Pair<>(RpcUtils.SUCCESS_STATUS, null);
    }

    final TsTable updatedTable = TsTable.deserialize(ByteBuffer.wrap(originalTable.serialize()));
    updatedProperties.forEach(
        (k, v) -> {
          originalProperties.put(k, originalTable.getPropValue(k).orElse(null));
          updatedTable.addProp(k, v);
        });

    return new Pair<>(RpcUtils.SUCCESS_STATUS, updatedTable);
  }

  public void clearSchemaQuotaCache() {
    schemaQuotaStatistics.clear();
  }

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }

  private PartitionManager getPartitionManager() {
    return configManager.getPartitionManager();
  }

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }
}
