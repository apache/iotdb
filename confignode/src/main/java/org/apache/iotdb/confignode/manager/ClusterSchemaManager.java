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
package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.database.CountDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.read.database.GetDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.CheckTemplateSettablePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetAllSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetAllTemplateSetInfoPlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetPathsSetTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetTemplateSetInfoPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.AdjustMaxRegionGroupNumPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetDataReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetSchemaReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetTimePartitionIntervalPlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.DropSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.PreUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.RollbackPreUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.SetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.UnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.response.database.DatabaseSchemaResp;
import org.apache.iotdb.confignode.consensus.response.partition.PathInfoResp;
import org.apache.iotdb.confignode.consensus.response.template.AllTemplateSetInfoResp;
import org.apache.iotdb.confignode.consensus.response.template.TemplateInfoResp;
import org.apache.iotdb.confignode.consensus.response.template.TemplateSetInfoResp;
import org.apache.iotdb.confignode.exception.DatabaseNotExistsException;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.observer.NodeStatisticsEvent;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.manager.partition.PartitionMetrics;
import org.apache.iotdb.confignode.persistence.schema.ClusterSchemaInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPathsSetTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTemplateResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.template.TemplateInternalRPCUpdateType;
import org.apache.iotdb.db.metadata.template.TemplateInternalRPCUtil;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTemplateReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MAX_DATABASE_NAME_LENGTH;

/** The ClusterSchemaManager Manages cluster schema read and write requests. */
public class ClusterSchemaManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterSchemaManager.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final double SCHEMA_REGION_PER_DATA_NODE = CONF.getSchemaRegionPerDataNode();
  private static final double DATA_REGION_PER_PROCESSOR = CONF.getDataRegionPerProcessor();

  private final IManager configManager;
  private final ClusterSchemaInfo clusterSchemaInfo;

  public ClusterSchemaManager(IManager configManager, ClusterSchemaInfo clusterSchemaInfo) {
    this.configManager = configManager;
    this.clusterSchemaInfo = clusterSchemaInfo;
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

    try {
      clusterSchemaInfo.checkContainsStorageGroup(databaseSchemaPlan.getSchema().getName());
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
        configManager, databaseSchemaPlan.getSchema().getName());

    // Adjust the maximum RegionGroup number of each Database
    adjustMaxRegionGroupNum();

    return result;
  }

  /** Alter Database */
  public TSStatus alterDatabase(DatabaseSchemaPlan databaseSchemaPlan) {
    TSStatus result;
    boolean isDatabaseExisted;
    TDatabaseSchema storageGroupSchema = databaseSchemaPlan.getSchema();

    try {
      isDatabaseExisted = clusterSchemaInfo.isDatabaseExisted(storageGroupSchema.getName());
    } catch (IllegalPathException e) {
      // Reject if DatabaseName is illegal
      result = new TSStatus(TSStatusCode.ILLEGAL_PATH.getStatusCode());
      result.setMessage("Failed to alter database. " + e.getMessage());
      return result;
    }

    if (!isDatabaseExisted) {
      // Reject if Database doesn't exist
      result = new TSStatus(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode());
      result.setMessage(
          "Failed to alter database. The Database "
              + storageGroupSchema.getName()
              + " doesn't exist.");
      return result;
    }

    if (storageGroupSchema.isSetMinSchemaRegionGroupNum()) {
      // Validate alter SchemaRegionGroupNum
      int minSchemaRegionGroupNum =
          getMinRegionGroupNum(storageGroupSchema.getName(), TConsensusGroupType.SchemaRegion);
      if (storageGroupSchema.getMinSchemaRegionGroupNum() <= minSchemaRegionGroupNum) {
        result = new TSStatus(TSStatusCode.DATABASE_CONFIG_ERROR.getStatusCode());
        result.setMessage(
            String.format(
                "Failed to alter database. The SchemaRegionGroupNum could only be increased. "
                    + "Current SchemaRegionGroupNum: %d, Alter SchemaRegionGroupNum: %d",
                minSchemaRegionGroupNum, storageGroupSchema.getMinSchemaRegionGroupNum()));
        return result;
      }
    }
    if (storageGroupSchema.isSetMinDataRegionGroupNum()) {
      // Validate alter DataRegionGroupNum
      int minDataRegionGroupNum =
          getMinRegionGroupNum(storageGroupSchema.getName(), TConsensusGroupType.DataRegion);
      if (storageGroupSchema.getMinDataRegionGroupNum() <= minDataRegionGroupNum) {
        result = new TSStatus(TSStatusCode.DATABASE_CONFIG_ERROR.getStatusCode());
        result.setMessage(
            String.format(
                "Failed to alter database. The DataRegionGroupNum could only be increased. "
                    + "Current DataRegionGroupNum: %d, Alter DataRegionGroupNum: %d",
                minDataRegionGroupNum, storageGroupSchema.getMinDataRegionGroupNum()));
        return result;
      }
    }

    // Alter DatabaseSchema
    return getConsensusManager().write(databaseSchemaPlan).getStatus();
  }

  /** Delete StorageGroup synchronized to protect the safety of adjustMaxRegionGroupNum */
  public synchronized TSStatus deleteStorageGroup(DeleteDatabasePlan deleteDatabasePlan) {
    TSStatus result = getConsensusManager().write(deleteDatabasePlan).getStatus();
    // Adjust the maximum RegionGroup number of each StorageGroup after deleting the storage group
    if (result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      adjustMaxRegionGroupNum();
    }
    return result;
  }

  /**
   * Count StorageGroups by specific path pattern
   *
   * @return CountStorageGroupResp
   */
  public DataSet countMatchedStorageGroups(CountDatabasePlan countDatabasePlan) {
    return getConsensusManager().read(countDatabasePlan).getDataset();
  }

  /**
   * Get StorageGroupSchemas by specific path pattern
   *
   * @return StorageGroupSchemaDataSet
   */
  public DataSet getMatchedStorageGroupSchema(GetDatabasePlan getStorageGroupPlan) {
    return getConsensusManager().read(getStorageGroupPlan).getDataset();
  }

  /** Only used in cluster tool show StorageGroup */
  public TShowDatabaseResp showStorageGroup(GetDatabasePlan getStorageGroupPlan) {
    DatabaseSchemaResp databaseSchemaResp =
        (DatabaseSchemaResp) getMatchedStorageGroupSchema(getStorageGroupPlan);
    if (databaseSchemaResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // Return immediately if some StorageGroups doesn't exist
      return new TShowDatabaseResp().setStatus(databaseSchemaResp.getStatus());
    }

    Map<String, TDatabaseInfo> infoMap = new ConcurrentHashMap<>();
    for (TDatabaseSchema storageGroupSchema : databaseSchemaResp.getSchemaMap().values()) {
      String database = storageGroupSchema.getName();
      TDatabaseInfo storageGroupInfo = new TDatabaseInfo();
      storageGroupInfo.setName(database);
      storageGroupInfo.setTTL(storageGroupSchema.getTTL());
      storageGroupInfo.setSchemaReplicationFactor(storageGroupSchema.getSchemaReplicationFactor());
      storageGroupInfo.setDataReplicationFactor(storageGroupSchema.getDataReplicationFactor());
      storageGroupInfo.setTimePartitionInterval(storageGroupSchema.getTimePartitionInterval());

      try {
        storageGroupInfo.setSchemaRegionNum(
            getPartitionManager().getRegionGroupCount(database, TConsensusGroupType.SchemaRegion));
        storageGroupInfo.setDataRegionNum(
            getPartitionManager().getRegionGroupCount(database, TConsensusGroupType.DataRegion));
        storageGroupInfo.setMinSchemaRegionNum(
            getMinRegionGroupNum(database, TConsensusGroupType.SchemaRegion));
        storageGroupInfo.setMaxSchemaRegionNum(
            getMaxRegionGroupNum(database, TConsensusGroupType.SchemaRegion));
        storageGroupInfo.setMinDataRegionNum(
            getMinRegionGroupNum(database, TConsensusGroupType.DataRegion));
        storageGroupInfo.setMaxDataRegionNum(
            getMaxRegionGroupNum(database, TConsensusGroupType.DataRegion));
      } catch (DatabaseNotExistsException e) {
        // Return immediately if some StorageGroups doesn't exist
        return new TShowDatabaseResp()
            .setStatus(
                new TSStatus(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode())
                    .setMessage(e.getMessage()));
      }

      infoMap.put(database, storageGroupInfo);
    }

    return new TShowDatabaseResp().setDatabaseInfoMap(infoMap).setStatus(StatusUtils.OK);
  }

  public Map<String, Long> getAllTTLInfo() {
    DatabaseSchemaResp databaseSchemaResp =
        (DatabaseSchemaResp)
            getMatchedStorageGroupSchema(new GetDatabasePlan(Arrays.asList("root", "**")));
    Map<String, Long> infoMap = new ConcurrentHashMap<>();
    if (databaseSchemaResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // Return immediately if some StorageGroups doesn't exist
      return infoMap;
    }
    for (TDatabaseSchema storageGroupSchema : databaseSchemaResp.getSchemaMap().values()) {
      infoMap.put(storageGroupSchema.getName(), storageGroupSchema.getTTL());
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
        clusterSchemaInfo.getMatchedStorageGroupSchemasByOneName(
            setTTLPlan.getStorageGroupPathPattern());

    if (storageSchemaMap.isEmpty()) {
      return RpcUtils.getStatus(
          TSStatusCode.DATABASE_NOT_EXIST,
          "Path [" + new PartialPath(setTTLPlan.getStorageGroupPathPattern()) + "] does not exist");
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
    int totalCpuCoreNum = getNodeManager().getTotalCpuCoreCount();
    int databaseNum = databaseSchemaMap.size();

    for (TDatabaseSchema databaseSchema : databaseSchemaMap.values()) {
      if (!getPartitionManager().isDatabaseExisted(databaseSchema.getName())) {
        // filter the pre deleted database
        databaseNum--;
      }
    }

    AdjustMaxRegionGroupNumPlan adjustMaxRegionGroupNumPlan = new AdjustMaxRegionGroupNumPlan();
    for (TDatabaseSchema databaseSchema : databaseSchemaMap.values()) {
      try {
        // Adjust maxSchemaRegionGroupNum for each StorageGroup.
        // All StorageGroups share the DataNodes equally.
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

        // Adjust maxDataRegionGroupNum for each StorageGroup.
        // All StorageGroups divide the total cpu cores equally.
        // The allocated DataRegionGroups will not be shrunk.
        int allocatedDataRegionGroupCount =
            getPartitionManager()
                .getRegionGroupCount(databaseSchema.getName(), TConsensusGroupType.DataRegion);
        int maxDataRegionGroupNum =
            calcMaxRegionGroupNum(
                databaseSchema.getMinDataRegionGroupNum(),
                DATA_REGION_PER_PROCESSOR,
                totalCpuCoreNum,
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
      int storageGroupNum,
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
                    resourceWeight * resource / (double) (storageGroupNum * replicationFactor)),
            // The maxRegionGroupNum should be great or equal to the allocatedRegionGroupCount
            allocatedRegionGroupCount));
  }

  // ======================================================
  // Leader scheduling interfaces
  // ======================================================

  /**
   * Only leader use this interface. Get all Databases name
   *
   * @return List<DatabaseName>, all Databases' name
   */
  public List<String> getDatabaseNames() {
    return clusterSchemaInfo.getDatabaseNames();
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
    return clusterSchemaInfo.getMatchedDatabaseSchemaByName(database);
  }

  /**
   * Only leader use this interface. Get the specified Databases' schema
   *
   * @param rawPathList List<DatabaseName>
   * @return the matched DatabaseSchemas
   */
  public Map<String, TDatabaseSchema> getMatchedDatabaseSchemasByName(List<String> rawPathList) {
    return clusterSchemaInfo.getMatchedDatabaseSchemasByName(rawPathList);
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

  /** mount template */
  public synchronized TSStatus setSchemaTemplate(String templateName, String path) {
    // check whether the template can be set on given path
    CheckTemplateSettablePlan checkTemplateSettablePlan =
        new CheckTemplateSettablePlan(templateName, path);
    TemplateInfoResp resp =
        (TemplateInfoResp) getConsensusManager().read(checkTemplateSettablePlan).getDataset();
    if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return resp.getStatus();
    }

    Template template = resp.getTemplateList().get(0);

    // prepare req
    TUpdateTemplateReq req = new TUpdateTemplateReq();
    req.setType(TemplateInternalRPCUpdateType.ADD_TEMPLATE_SET_INFO.toByte());
    req.setTemplateInfo(TemplateInternalRPCUtil.generateAddTemplateSetInfoBytes(template, path));

    // sync template set info to all dataNodes
    TSStatus status;
    List<TDataNodeConfiguration> allDataNodes =
        configManager.getNodeManager().getRegisteredDataNodes();
    for (TDataNodeConfiguration dataNodeInfo : allDataNodes) {
      status =
          SyncDataNodeClientPool.getInstance()
              .sendSyncRequestToDataNodeWithRetry(
                  dataNodeInfo.getLocation().getInternalEndPoint(),
                  req,
                  DataNodeRequestType.UPDATE_TEMPLATE);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // roll back the synced cache on dataNodes
        return status.setSubStatus(rollbackTemplateSetInfoSync(template.getId(), path));
      }
    }

    // execute set operation on configNode
    SetSchemaTemplatePlan setSchemaTemplatePlan = new SetSchemaTemplatePlan(templateName, path);
    status = getConsensusManager().write(setSchemaTemplatePlan).getStatus();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return status;
    } else {
      // roll back the synced cache on dataNodes
      return status.setSubStatus(rollbackTemplateSetInfoSync(template.getId(), path));
    }
  }

  private List<TSStatus> rollbackTemplateSetInfoSync(int templateId, String path) {
    // construct the rollbackReq
    TUpdateTemplateReq rollbackReq = new TUpdateTemplateReq();
    rollbackReq.setType(TemplateInternalRPCUpdateType.INVALIDATE_TEMPLATE_SET_INFO.toByte());
    rollbackReq.setTemplateInfo(
        TemplateInternalRPCUtil.generateInvalidateTemplateSetInfoBytes(templateId, path));

    // get all dataNodes
    List<TDataNodeConfiguration> allDataNodes =
        configManager.getNodeManager().getRegisteredDataNodes();

    // send rollbackReq
    TSStatus status;
    List<TSStatus> failedRollbackStatusList = new ArrayList<>();
    for (TDataNodeConfiguration dataNodeInfo : allDataNodes) {
      status =
          SyncDataNodeClientPool.getInstance()
              .sendSyncRequestToDataNodeWithRetry(
                  dataNodeInfo.getLocation().getInternalEndPoint(),
                  rollbackReq,
                  DataNodeRequestType.UPDATE_TEMPLATE);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        failedRollbackStatusList.add(status);
      }
    }
    return failedRollbackStatusList;
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
   * get all template set info to sync to all dataNodes, the pre unset template info won't be taken
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

  /**
   * When some Nodes' states changed during a heartbeat loop, the eventbus in LoadManager will post
   * the different NodeStatstics event to SyncManager and ClusterSchemaManager.
   *
   * @param nodeStatisticsEvent nodeStatistics that changed in a heartbeat loop
   */
  @Subscribe
  @AllowConcurrentEvents
  public void handleNodeStatistics(NodeStatisticsEvent nodeStatisticsEvent) {
    // TODO
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
