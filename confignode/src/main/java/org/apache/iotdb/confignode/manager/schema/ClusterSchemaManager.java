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
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.database.CountDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.read.database.GetDatabasePlan;
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
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTimePartitionIntervalPlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.DropSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.ExtendSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.PreUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.RollbackPreUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.UnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.response.database.CountDatabaseResp;
import org.apache.iotdb.confignode.consensus.response.database.DatabaseSchemaResp;
import org.apache.iotdb.confignode.consensus.response.partition.PathInfoResp;
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
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.template.TemplateInternalRPCUpdateType;
import org.apache.iotdb.db.metadata.template.TemplateInternalRPCUtil;
import org.apache.iotdb.db.metadata.template.alter.TemplateExtendInfo;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTemplateReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MAX_DATABASE_NAME_LENGTH;

/** The ClusterSchemaManager Manages cluster schema read and write requests. */
public class ClusterSchemaManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterSchemaManager.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final double SCHEMA_REGION_PER_DATA_NODE = CONF.getSchemaRegionPerDataNode();
  private static final double DATA_REGION_PER_DATA_NODE = CONF.getDataRegionPerDataNode();

  private final IManager configManager;
  private final ClusterSchemaInfo clusterSchemaInfo;
  private final ClusterSchemaQuotaStatistics schemaQuotaStatistics;

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
  public TSStatus setDatabase(DatabaseSchemaPlan databaseSchemaPlan) {
    TSStatus result;
    if (databaseSchemaPlan.getSchema().getName().length() > MAX_DATABASE_NAME_LENGTH) {
      IllegalPathException illegalPathException =
          new IllegalPathException(
              databaseSchemaPlan.getSchema().getName(),
              "the length of database name shall not exceed " + MAX_DATABASE_NAME_LENGTH);
      return RpcUtils.getStatus(
          illegalPathException.getErrorCode(), illegalPathException.getMessage());
    }

    if (getPartitionManager().isDatabasePreDeleted(databaseSchemaPlan.getSchema().getName())) {
      return RpcUtils.getStatus(
          TSStatusCode.METADATA_ERROR,
          String.format(
              "Some other task is deleting database %s", databaseSchemaPlan.getSchema().getName()));
    }

    try {
      clusterSchemaInfo.isDatabaseNameValid(databaseSchemaPlan.getSchema().getName());
    } catch (MetadataException metadataException) {
      // Reject if StorageGroup already set
      if (metadataException instanceof IllegalPathException) {
        result = new TSStatus(TSStatusCode.ILLEGAL_PATH.getStatusCode());
      } else {
        result = new TSStatus(TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode());
      }
      result.setMessage(metadataException.getMessage());
      return result;
    }

    // Cache DatabaseSchema
    result = getConsensusManager().write(databaseSchemaPlan).getStatus();

    // Bind Database metrics
    PartitionMetrics.bindDatabasePartitionMetrics(
        MetricService.getInstance(), configManager, databaseSchemaPlan.getSchema().getName());

    // Adjust the maximum RegionGroup number of each Database
    adjustMaxRegionGroupNum();

    return result;
  }

  /** Alter Database */
  public TSStatus alterDatabase(DatabaseSchemaPlan databaseSchemaPlan) {
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
    return getConsensusManager().write(databaseSchemaPlan).getStatus();
  }

  /** Delete DatabaseSchema. */
  public TSStatus deleteDatabase(DeleteDatabasePlan deleteDatabasePlan) {
    TSStatus result = getConsensusManager().write(deleteDatabasePlan).getStatus();
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
    return (CountDatabaseResp) getConsensusManager().read(countDatabasePlan).getDataset();
  }

  /**
   * Get DatabaseSchemas by specified path pattern. Notice: including pre-deleted Database
   *
   * <p>Notice: Only invoke this interface in ConfigManager
   *
   * @return DatabaseSchemaResp
   */
  public DatabaseSchemaResp getMatchedDatabaseSchema(GetDatabasePlan getStorageGroupPlan) {
    DatabaseSchemaResp resp =
        (DatabaseSchemaResp) getConsensusManager().read(getStorageGroupPlan).getDataset();
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
  public TShowDatabaseResp showDatabase(GetDatabasePlan getStorageGroupPlan) {
    DatabaseSchemaResp databaseSchemaResp =
        (DatabaseSchemaResp) getConsensusManager().read(getStorageGroupPlan).getDataset();
    if (databaseSchemaResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // Return immediately if some Database doesn't exist
      return new TShowDatabaseResp().setStatus(databaseSchemaResp.getStatus());
    }

    Map<String, TDatabaseInfo> infoMap = new ConcurrentHashMap<>();
    for (TDatabaseSchema databaseSchema : databaseSchemaResp.getSchemaMap().values()) {
      String database = databaseSchema.getName();
      TDatabaseInfo databaseInfo = new TDatabaseInfo();
      databaseInfo.setName(database);
      databaseInfo.setTTL(databaseSchema.getTTL());
      databaseInfo.setSchemaReplicationFactor(databaseSchema.getSchemaReplicationFactor());
      databaseInfo.setDataReplicationFactor(databaseSchema.getDataReplicationFactor());
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

  public Map<String, Long> getAllTTLInfo() {
    List<String> databases = getDatabaseNames();
    Map<String, Long> infoMap = new ConcurrentHashMap<>();
    for (String database : databases) {
      try {
        infoMap.put(database, getTTL(database));
      } catch (DatabaseNotExistsException e) {
        LOGGER.warn("Database: {} doesn't exist", databases, e);
      }
    }
    return infoMap;
  }

  /**
   * Update TTL for the specific StorageGroup or all databases in a path
   *
   * @param setTTLPlan setTTLPlan
   * @return SUCCESS_STATUS if successfully update the TTL, STORAGE_GROUP_NOT_EXIST if the path
   *     doesn't exist
   */
  public TSStatus setTTL(SetTTLPlan setTTLPlan) {

    Map<String, TDatabaseSchema> storageSchemaMap =
        clusterSchemaInfo.getMatchedDatabaseSchemasByOneName(setTTLPlan.getDatabasePathPattern());

    if (storageSchemaMap.isEmpty()) {
      return RpcUtils.getStatus(
          TSStatusCode.DATABASE_NOT_EXIST,
          "Path [" + new PartialPath(setTTLPlan.getDatabasePathPattern()) + "] does not exist");
    }

    // Map<DataNodeId, TDataNodeLocation>
    Map<Integer, TDataNodeLocation> dataNodeLocationMap = new ConcurrentHashMap<>();
    // Map<DataNodeId, StorageGroupPatterns>
    Map<Integer, List<String>> dnlToSgMap = new ConcurrentHashMap<>();
    for (String storageGroup : storageSchemaMap.keySet()) {
      // Get related DataNodes
      Set<TDataNodeLocation> dataNodeLocations =
          getPartitionManager()
              .getDatabaseRelatedDataNodes(storageGroup, TConsensusGroupType.DataRegion);

      for (TDataNodeLocation dataNodeLocation : dataNodeLocations) {
        dataNodeLocationMap.putIfAbsent(dataNodeLocation.getDataNodeId(), dataNodeLocation);
        dnlToSgMap
            .computeIfAbsent(dataNodeLocation.getDataNodeId(), empty -> new ArrayList<>())
            .add(storageGroup);
      }
    }

    AsyncClientHandler<TSetTTLReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.SET_TTL);
    dnlToSgMap
        .keySet()
        .forEach(
            dataNodeId -> {
              TSetTTLReq setTTLReq =
                  new TSetTTLReq(dnlToSgMap.get(dataNodeId), setTTLPlan.getTTL());
              clientHandler.putRequest(dataNodeId, setTTLReq);
              clientHandler.putDataNodeLocation(dataNodeId, dataNodeLocationMap.get(dataNodeId));
            });
    // TODO: Check response
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);

    return getConsensusManager().write(setTTLPlan).getStatus();
  }

  public TSStatus setSchemaReplicationFactor(
      SetSchemaReplicationFactorPlan setSchemaReplicationFactorPlan) {
    // TODO: Inform DataNodes
    return getConsensusManager().write(setSchemaReplicationFactorPlan).getStatus();
  }

  public TSStatus setDataReplicationFactor(
      SetDataReplicationFactorPlan setDataReplicationFactorPlan) {
    // TODO: Inform DataNodes
    return getConsensusManager().write(setDataReplicationFactorPlan).getStatus();
  }

  public TSStatus setTimePartitionInterval(
      SetTimePartitionIntervalPlan setTimePartitionIntervalPlan) {
    // TODO: Inform DataNodes
    return getConsensusManager().write(setTimePartitionIntervalPlan).getStatus();
  }

  /**
   * Only leader use this interface. Adjust the maxSchemaRegionGroupNum and maxDataRegionGroupNum of
   * each StorageGroup based on existing cluster resources
   */
  public synchronized void adjustMaxRegionGroupNum() {
    // Get all StorageGroupSchemas
    Map<String, TDatabaseSchema> databaseSchemaMap =
        getMatchedDatabaseSchemasByName(getDatabaseNames());
    if (databaseSchemaMap.size() == 0) {
      // Skip when there are no StorageGroups
      return;
    }

    int dataNodeNum = getNodeManager().getRegisteredDataNodeCount();
    int databaseNum = databaseSchemaMap.size();

    for (TDatabaseSchema databaseSchema : databaseSchemaMap.values()) {
      if (!isDatabaseExist(databaseSchema.getName())) {
        // filter the pre deleted database
        databaseNum--;
      }
    }

    AdjustMaxRegionGroupNumPlan adjustMaxRegionGroupNumPlan = new AdjustMaxRegionGroupNumPlan();
    for (TDatabaseSchema databaseSchema : databaseSchemaMap.values()) {
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
        LOGGER.warn("Adjust maxRegionGroupNum failed because StorageGroup doesn't exist", e);
      }
    }
    getConsensusManager().write(adjustMaxRegionGroupNumPlan);
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
                    // The maxRegionGroupNum of the current StorageGroup is expected to be:
                    // (resourceWeight * resource) / (createdStorageGroupNum * replicationFactor)
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
   * Only leader use this interface. Get the specified Database's schema
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
   * Only leader use this interface. Get the specified Databases' schema
   *
   * @param rawPathList List<DatabaseName>
   * @return the matched DatabaseSchemas
   */
  public Map<String, TDatabaseSchema> getMatchedDatabaseSchemasByName(List<String> rawPathList) {
    Map<String, TDatabaseSchema> result = new ConcurrentHashMap<>();
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
   * Only leader use this interface. Get the TTL of specified Database
   *
   * @param database DatabaseName
   * @throws DatabaseNotExistsException When the specified Database doesn't exist
   */
  public long getTTL(String database) throws DatabaseNotExistsException {
    return getDatabaseSchemaByName(database).getTTL();
  }

  /**
   * Only leader use this interface. Get the replication factor of specified Database
   *
   * @param database DatabaseName
   * @param consensusGroupType SchemaRegion or DataRegion
   * @return SchemaReplicationFactor or DataReplicationFactor
   * @throws DatabaseNotExistsException When the specific StorageGroup doesn't exist
   */
  public int getReplicationFactor(String database, TConsensusGroupType consensusGroupType)
      throws DatabaseNotExistsException {
    TDatabaseSchema storageGroupSchema = getDatabaseSchemaByName(database);
    return TConsensusGroupType.SchemaRegion.equals(consensusGroupType)
        ? storageGroupSchema.getSchemaReplicationFactor()
        : storageGroupSchema.getDataReplicationFactor();
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
   * create schema template
   *
   * @param createSchemaTemplatePlan CreateSchemaTemplatePlan
   * @return TSStatus
   */
  public TSStatus createTemplate(CreateSchemaTemplatePlan createSchemaTemplatePlan) {
    return getConsensusManager().write(createSchemaTemplatePlan).getStatus();
  }

  /**
   * show schema template
   *
   * @return TGetAllTemplatesResp
   */
  public TGetAllTemplatesResp getAllTemplates() {
    GetAllSchemaTemplatePlan getAllSchemaTemplatePlan = new GetAllSchemaTemplatePlan();
    TemplateInfoResp templateResp =
        (TemplateInfoResp) getConsensusManager().read(getAllSchemaTemplatePlan).getDataset();
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

  /** show nodes in schema template */
  public TGetTemplateResp getTemplate(String req) {
    GetSchemaTemplatePlan getSchemaTemplatePlan = new GetSchemaTemplatePlan(req);
    TemplateInfoResp templateResp =
        (TemplateInfoResp) getConsensusManager().read(getSchemaTemplatePlan).getDataset();
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

  /** show path set template xx */
  public TGetPathsSetTemplatesResp getPathsSetTemplate(String templateName) {
    GetPathsSetTemplatePlan getPathsSetTemplatePlan = new GetPathsSetTemplatePlan(templateName);
    PathInfoResp pathInfoResp =
        (PathInfoResp) getConsensusManager().read(getPathsSetTemplatePlan).getDataset();
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
    AllTemplateSetInfoResp resp =
        (AllTemplateSetInfoResp)
            getConsensusManager().read(new GetAllTemplateSetInfoPlan()).getDataset();
    return resp.getTemplateInfo();
  }

  public TemplateSetInfoResp getTemplateSetInfo(List<PartialPath> patternList) {
    return (TemplateSetInfoResp)
        getConsensusManager().read(new GetTemplateSetInfoPlan(patternList)).getDataset();
  }

  public Pair<TSStatus, Template> checkIsTemplateSetOnPath(String templateName, String path) {
    GetSchemaTemplatePlan getSchemaTemplatePlan = new GetSchemaTemplatePlan(templateName);
    TemplateInfoResp templateResp =
        (TemplateInfoResp) getConsensusManager().read(getSchemaTemplatePlan).getDataset();
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

    GetPathsSetTemplatePlan getPathsSetTemplatePlan = new GetPathsSetTemplatePlan(templateName);
    PathInfoResp pathInfoResp =
        (PathInfoResp) getConsensusManager().read(getPathsSetTemplatePlan).getDataset();
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
    return getConsensusManager()
        .write(new PreUnsetSchemaTemplatePlan(templateId, path))
        .getStatus();
  }

  public TSStatus rollbackPreUnsetSchemaTemplate(int templateId, PartialPath path) {
    return getConsensusManager()
        .write(new RollbackPreUnsetSchemaTemplatePlan(templateId, path))
        .getStatus();
  }

  public TSStatus unsetSchemaTemplateInBlackList(int templateId, PartialPath path) {
    return getConsensusManager().write(new UnsetSchemaTemplatePlan(templateId, path)).getStatus();
  }

  public synchronized TSStatus dropSchemaTemplate(String templateName) {

    // check template existence
    GetSchemaTemplatePlan getSchemaTemplatePlan = new GetSchemaTemplatePlan(templateName);
    TemplateInfoResp templateInfoResp =
        (TemplateInfoResp) getConsensusManager().read(getSchemaTemplatePlan).getDataset();
    if (templateInfoResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return templateInfoResp.getStatus();
    } else if (templateInfoResp.getTemplateList() == null
        || templateInfoResp.getTemplateList().isEmpty()) {
      return RpcUtils.getStatus(
          TSStatusCode.UNDEFINED_TEMPLATE.getStatusCode(),
          String.format("Undefined template name: %s", templateName));
    }

    // check is template set on some path, block all template set operation
    GetPathsSetTemplatePlan getPathsSetTemplatePlan = new GetPathsSetTemplatePlan(templateName);
    PathInfoResp pathInfoResp =
        (PathInfoResp) getConsensusManager().read(getPathsSetTemplatePlan).getDataset();
    if (pathInfoResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return pathInfoResp.getStatus();
    } else if (pathInfoResp.getPathList() != null && !pathInfoResp.getPathList().isEmpty()) {
      return RpcUtils.getStatus(
          TSStatusCode.METADATA_ERROR.getStatusCode(),
          String.format(
              "Template [%s] has been set on MTree, cannot be dropped now.", templateName));
    }

    // execute drop template
    return getConsensusManager().write(new DropSchemaTemplatePlan(templateName)).getStatus();
  }

  public synchronized TSStatus extendSchemaTemplate(TemplateExtendInfo templateExtendInfo) {
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
                "Measurement %s already exist in schema template %s",
                intersectionMeasurements, template.getName()));
      }
    }

    ExtendSchemaTemplatePlan extendSchemaTemplatePlan =
        new ExtendSchemaTemplatePlan(templateExtendInfo);
    TSStatus status = getConsensusManager().write(extendSchemaTemplatePlan).getStatus();
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

    AsyncClientHandler<TUpdateTemplateReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.UPDATE_TEMPLATE, updateTemplateReq, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
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
              "Measurement %s already exist in schema template %s",
              intersectionMeasurements, template.getName()));
    }
  }

  public long getSchemaQuotaCount() {
    return schemaQuotaStatistics.getSchemaQuotaCount(getPartitionManager().getAllSchemaPartition());
  }

  public void updateSchemaQuota(Map<Integer, Long> schemaCountMap) {
    schemaQuotaStatistics.updateCount(schemaCountMap);
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
