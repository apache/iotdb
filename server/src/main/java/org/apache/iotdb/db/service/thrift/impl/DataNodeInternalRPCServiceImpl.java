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

package org.apache.iotdb.db.service.thrift.impl;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.udf.service.UDFExecutableManager;
import org.apache.iotdb.commons.udf.service.UDFRegistrationService;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.consensus.exception.PeerNotInConsensusGroupException;
import org.apache.iotdb.db.auth.AuthorizerManager;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.engine.cache.BloomFilterCache;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.metadata.cache.DataNodeSchemaCache;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.metadata.template.ClusterTemplateManager;
import org.apache.iotdb.db.metadata.template.TemplateInternalRPCUpdateType;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceInfo;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceState;
import org.apache.iotdb.db.mpp.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.SchemaValidator;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.service.DataNode;
import org.apache.iotdb.db.service.RegionMigrateService;
import org.apache.iotdb.db.service.metrics.MetricService;
import org.apache.iotdb.db.service.metrics.enums.Metric;
import org.apache.iotdb.db.service.metrics.enums.Tag;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.mpp.rpc.thrift.IDataNodeRPCService;
import org.apache.iotdb.mpp.rpc.thrift.TAddConsensusGroup;
import org.apache.iotdb.mpp.rpc.thrift.TCancelFragmentInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TCancelPlanFragmentReq;
import org.apache.iotdb.mpp.rpc.thrift.TCancelQueryReq;
import org.apache.iotdb.mpp.rpc.thrift.TCancelResp;
import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateFunctionRequest;
import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TDisableDataNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropFunctionRequest;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStateReq;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceStateResp;
import org.apache.iotdb.mpp.rpc.thrift.THeartbeatReq;
import org.apache.iotdb.mpp.rpc.thrift.THeartbeatResp;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidatePermissionCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TMigrateRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TRegionLeaderChangeReq;
import org.apache.iotdb.mpp.rpc.thrift.TRegionRouteReq;
import org.apache.iotdb.mpp.rpc.thrift.TSchemaFetchRequest;
import org.apache.iotdb.mpp.rpc.thrift.TSchemaFetchResponse;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceResp;
import org.apache.iotdb.mpp.rpc.thrift.TSendPlanNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendPlanNodeResp;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateConfigNodeGroupReq;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTemplateReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.exception.NotImplementedException;

import com.google.common.collect.ImmutableList;
import io.airlift.concurrent.SetThreadName;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DataNodeInternalRPCServiceImpl implements IDataNodeRPCService.Iface {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DataNodeInternalRPCServiceImpl.class);
  private final SchemaEngine schemaEngine = SchemaEngine.getInstance();
  private final StorageEngineV2 storageEngine = StorageEngineV2.getInstance();

  public DataNodeInternalRPCServiceImpl() {
    super();
  }

  @Override
  public TSendFragmentInstanceResp sendFragmentInstance(TSendFragmentInstanceReq req) {
    LOGGER.info("receive FragmentInstance to group[{}]", req.getConsensusGroupId());

    // deserialize ConsensusGroupId
    ConsensusGroupId groupId;
    try {
      groupId = ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    } catch (Throwable t) {
      LOGGER.error("Deserialize ConsensusGroupId failed. ", t);
      TSendFragmentInstanceResp resp = new TSendFragmentInstanceResp(false);
      resp.setMessage("Deserialize ConsensusGroupId failed: " + t.getMessage());
      return resp;
    }

    // We deserialize here instead of the underlying state machine because parallelism is possible
    // here but not at the underlying state machine
    FragmentInstance fragmentInstance;
    try {
      fragmentInstance = FragmentInstance.deserializeFrom(req.fragmentInstance.body);
    } catch (Throwable t) {
      LOGGER.error("Deserialize FragmentInstance failed.", t);
      TSendFragmentInstanceResp resp = new TSendFragmentInstanceResp(false);
      resp.setMessage("Deserialize FragmentInstance failed: " + t.getMessage());
      return resp;
    }

    // execute fragment instance in state machine
    ConsensusReadResponse readResponse;
    try (SetThreadName threadName = new SetThreadName(fragmentInstance.getId().getFullId())) {
      if (groupId instanceof DataRegionId) {
        readResponse = DataRegionConsensusImpl.getInstance().read(groupId, fragmentInstance);
      } else {
        readResponse = SchemaRegionConsensusImpl.getInstance().read(groupId, fragmentInstance);
      }
      TSendFragmentInstanceResp resp = new TSendFragmentInstanceResp();
      if (!readResponse.isSuccess()) {
        LOGGER.error(
            "Execute FragmentInstance in ConsensusGroup {} failed.",
            req.getConsensusGroupId(),
            readResponse.getException());
        resp.setAccepted(false);
        resp.setMessage(
            "Execute FragmentInstance failed: "
                + (readResponse.getException() == null
                    ? ""
                    : readResponse.getException().getMessage()));
      } else {
        FragmentInstanceInfo info = (FragmentInstanceInfo) readResponse.getDataset();
        resp.setAccepted(!info.getState().isFailed());
        resp.setMessage(info.getMessage());
      }
      return resp;
    } catch (Throwable t) {
      LOGGER.error(
          "Execute FragmentInstance in ConsensusGroup {} failed.", req.getConsensusGroupId(), t);
      TSendFragmentInstanceResp resp = new TSendFragmentInstanceResp(false);
      resp.setMessage("Execute FragmentInstance failed: " + t.getMessage());
      return resp;
    }
  }

  @Override
  public TSendPlanNodeResp sendPlanNode(TSendPlanNodeReq req) {
    LOGGER.info("receive PlanNode to group[{}]", req.getConsensusGroupId());
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    TSendPlanNodeResp response = new TSendPlanNodeResp();
    ConsensusWriteResponse writeResponse;

    PlanNode planNode = PlanNodeType.deserialize(req.planNode.body);
    boolean hasFailedMeasurement = false;
    String partialInsertMessage = null;
    if (planNode instanceof InsertNode) {
      InsertNode insertNode = (InsertNode) planNode;
      try {
        SchemaValidator.validate(insertNode);
      } catch (SemanticException e) {
        response.setAccepted(false);
        response.setMessage(e.getMessage());
        return response;
      }
      hasFailedMeasurement = insertNode.hasFailedMeasurements();
      if (hasFailedMeasurement) {
        partialInsertMessage =
            String.format(
                "Fail to insert measurements %s caused by %s",
                insertNode.getFailedMeasurements(), insertNode.getFailedMessages());
        LOGGER.warn(partialInsertMessage);
      }
    }
    if (groupId instanceof DataRegionId) {
      writeResponse = DataRegionConsensusImpl.getInstance().write(groupId, planNode);
    } else {
      writeResponse = SchemaRegionConsensusImpl.getInstance().write(groupId, planNode);
    }
    // TODO need consider more status
    if (writeResponse.getStatus() != null) {
      response.setAccepted(
          !hasFailedMeasurement
              && TSStatusCode.SUCCESS_STATUS.getStatusCode()
                  == writeResponse.getStatus().getCode());
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != writeResponse.getStatus().getCode()) {
        response.setMessage(writeResponse.getStatus().message);
        response.setStatus(writeResponse.getStatus());
      } else if (hasFailedMeasurement) {
        response.setMessage(partialInsertMessage);
        response.setStatus(
            RpcUtils.getStatus(TSStatusCode.METADATA_ERROR.getStatusCode(), partialInsertMessage));
      } else {
        response.setMessage(writeResponse.getStatus().message);
      }
    } else {
      LOGGER.error(
          "Something wrong happened while calling consensus layer's write API.",
          writeResponse.getException());
      response.setAccepted(false);
      response.setMessage(writeResponse.getException().getMessage());
    }

    return response;
  }

  @Override
  public TFragmentInstanceStateResp fetchFragmentInstanceState(TFetchFragmentInstanceStateReq req) {
    FragmentInstanceId instanceId = FragmentInstanceId.fromThrift(req.fragmentInstanceId);
    FragmentInstanceInfo info = FragmentInstanceManager.getInstance().getInstanceInfo(instanceId);
    if (info != null) {
      TFragmentInstanceStateResp resp = new TFragmentInstanceStateResp(info.getState().toString());
      resp.setFailedMessages(ImmutableList.of(info.getMessage()));
      return resp;
    } else {
      return new TFragmentInstanceStateResp(FragmentInstanceState.NO_SUCH_INSTANCE.toString());
    }
  }

  @Override
  public TCancelResp cancelQuery(TCancelQueryReq req) {
    try (SetThreadName threadName = new SetThreadName(req.getQueryId())) {
      LOGGER.info("start cancelling query.");
      List<FragmentInstanceId> taskIds =
          req.getFragmentInstanceIds().stream()
              .map(FragmentInstanceId::fromThrift)
              .collect(Collectors.toList());
      for (FragmentInstanceId taskId : taskIds) {
        FragmentInstanceManager.getInstance().cancelTask(taskId);
      }
      LOGGER.info("finish cancelling query.");
      return new TCancelResp(true);
    }
  }

  @Override
  public TCancelResp cancelPlanFragment(TCancelPlanFragmentReq req) {
    throw new NotImplementedException();
  }

  @Override
  public TCancelResp cancelFragmentInstance(TCancelFragmentInstanceReq req) {
    throw new NotImplementedException();
  }

  @Override
  public TSchemaFetchResponse fetchSchema(TSchemaFetchRequest req) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TSStatus createSchemaRegion(TCreateSchemaRegionReq req) {
    TSStatus tsStatus;
    try {
      PartialPath storageGroupPartitionPath = new PartialPath(req.getStorageGroup());
      TRegionReplicaSet regionReplicaSet = req.getRegionReplicaSet();
      SchemaRegionId schemaRegionId = new SchemaRegionId(regionReplicaSet.getRegionId().getId());
      schemaEngine.createSchemaRegion(storageGroupPartitionPath, schemaRegionId);
      List<Peer> peers = new ArrayList<>();
      for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
        TEndPoint endpoint =
            new TEndPoint(
                dataNodeLocation.getSchemaRegionConsensusEndPoint().getIp(),
                dataNodeLocation.getSchemaRegionConsensusEndPoint().getPort());
        peers.add(new Peer(schemaRegionId, endpoint));
      }
      ConsensusGenericResponse consensusGenericResponse =
          SchemaRegionConsensusImpl.getInstance().createPeer(schemaRegionId, peers);
      if (consensusGenericResponse.isSuccess()) {
        tsStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } else {
        tsStatus = new TSStatus(TSStatusCode.CREATE_REGION_ERROR.getStatusCode());
        tsStatus.setMessage(consensusGenericResponse.getException().getMessage());
      }
    } catch (IllegalPathException e1) {
      LOGGER.error(
          "Create Schema Region {} failed because path is illegal.", req.getStorageGroup());
      tsStatus = new TSStatus(TSStatusCode.PATH_ILLEGAL.getStatusCode());
      tsStatus.setMessage("Create Schema Region failed because storageGroup path is illegal.");
    } catch (MetadataException e2) {
      LOGGER.error(
          "Create Schema Region {} failed because {}", req.getStorageGroup(), e2.getMessage());
      tsStatus = new TSStatus(TSStatusCode.CREATE_REGION_ERROR.getStatusCode());
      tsStatus.setMessage(
          String.format("Create Schema Region failed because of %s", e2.getMessage()));
    }
    return tsStatus;
  }

  @Override
  public TSStatus createDataRegion(TCreateDataRegionReq req) {
    TSStatus tsStatus;
    try {
      TRegionReplicaSet regionReplicaSet = req.getRegionReplicaSet();
      DataRegionId dataRegionId = new DataRegionId(regionReplicaSet.getRegionId().getId());
      storageEngine.createDataRegion(dataRegionId, req.storageGroup, req.ttl);
      List<Peer> peers = new ArrayList<>();
      for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
        TEndPoint endpoint =
            new TEndPoint(
                dataNodeLocation.getDataRegionConsensusEndPoint().getIp(),
                dataNodeLocation.getDataRegionConsensusEndPoint().getPort());
        peers.add(new Peer(dataRegionId, endpoint));
      }
      ConsensusGenericResponse consensusGenericResponse =
          DataRegionConsensusImpl.getInstance().createPeer(dataRegionId, peers);
      if (consensusGenericResponse.isSuccess()) {
        tsStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } else {
        tsStatus = new TSStatus(TSStatusCode.CREATE_REGION_ERROR.getStatusCode());
        tsStatus.setMessage(consensusGenericResponse.getException().getMessage());
      }
    } catch (DataRegionException e) {
      LOGGER.error(
          "Create Data Region {} failed because {}", req.getStorageGroup(), e.getMessage());
      tsStatus = new TSStatus(TSStatusCode.CREATE_REGION_ERROR.getStatusCode());
      tsStatus.setMessage(String.format("Create Data Region failed because of %s", e.getMessage()));
    }
    return tsStatus;
  }

  @Override
  public TSStatus invalidatePartitionCache(TInvalidateCacheReq req) {
    ClusterPartitionFetcher.getInstance().invalidAllCache();
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus invalidateSchemaCache(TInvalidateCacheReq req) {
    DataNodeSchemaCache.getInstance().cleanUp();
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public THeartbeatResp getDataNodeHeartBeat(THeartbeatReq req) throws TException {
    THeartbeatResp resp = new THeartbeatResp();
    resp.setHeartbeatTimestamp(req.getHeartbeatTimestamp());

    // Judging leader if necessary
    if (req.isNeedJudgeLeader()) {
      resp.setJudgedLeaders(getJudgedLeaders());
    }

    // Sampling load if necessary
    if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()
        && req.isNeedSamplingLoad()) {
      long cpuLoad =
          MetricService.getInstance()
              .getOrCreateGauge(
                  Metric.SYS_CPU_LOAD.toString(), MetricLevel.CORE, Tag.NAME.toString(), "system")
              .value();
      if (cpuLoad != 0) {
        resp.setCpu((short) cpuLoad);
      }
      long usedMemory = getMemory("jvm.memory.used.bytes");
      long maxMemory = getMemory("jvm.memory.max.bytes");
      if (usedMemory != 0 && maxMemory != 0) {
        resp.setMemory((short) (usedMemory * 100 / maxMemory));
      }
    }
    return resp;
  }

  @Override
  public TSStatus updateRegionCache(TRegionRouteReq req) throws TException {
    boolean result = ClusterPartitionFetcher.getInstance().updateRegionCache(req);
    if (result) {
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } else {
      return RpcUtils.getStatus(TSStatusCode.CACHE_UPDATE_FAIL);
    }
  }

  private Map<TConsensusGroupId, Boolean> getJudgedLeaders() {
    Map<TConsensusGroupId, Boolean> result = new HashMap<>();
    if (DataRegionConsensusImpl.getInstance() != null) {
      DataRegionConsensusImpl.getInstance()
          .getAllConsensusGroupIds()
          .forEach(
              groupId -> {
                result.put(
                    groupId.convertToTConsensusGroupId(),
                    DataRegionConsensusImpl.getInstance().isLeader(groupId));
              });
    }

    if (SchemaRegionConsensusImpl.getInstance() != null) {
      SchemaRegionConsensusImpl.getInstance()
          .getAllConsensusGroupIds()
          .forEach(
              groupId -> {
                result.put(
                    groupId.convertToTConsensusGroupId(),
                    SchemaRegionConsensusImpl.getInstance().isLeader(groupId));
              });
    }
    return result;
  }

  private long getMemory(String gaugeName) {
    long result = 0;
    try {
      //
      List<String> heapIds = Arrays.asList("PS Eden Space", "PS Old Eden", "Ps Survivor Space");
      List<String> noHeapIds = Arrays.asList("Code Cache", "Compressed Class Space", "Metaspace");

      for (String id : heapIds) {
        Gauge gauge =
            MetricService.getInstance()
                .getOrCreateGauge(gaugeName, MetricLevel.IMPORTANT, "id", id, "area", "heap");
        result += gauge.value();
      }
      for (String id : noHeapIds) {
        Gauge gauge =
            MetricService.getInstance()
                .getOrCreateGauge(gaugeName, MetricLevel.IMPORTANT, "id", id, "area", "noheap");
        result += gauge.value();
      }
    } catch (Exception e) {
      LOGGER.error("Failed to get memory from metric because {}", e.getMessage());
      return 0;
    }
    return result;
  }

  @Override
  public TSStatus invalidatePermissionCache(TInvalidatePermissionCacheReq req) {
    if (AuthorizerManager.getInstance().invalidateCache(req.getUsername(), req.getRoleName())) {
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    }
    return RpcUtils.getStatus(TSStatusCode.INVALIDATE_PERMISSION_CACHE_ERROR);
  }

  @Override
  public TSStatus merge() throws TException {
    try {
      storageEngine.mergeAll();
    } catch (StorageEngineException e) {
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus flush(TFlushReq req) throws TException {
    return storageEngine.operateFlush(req);
  }

  @Override
  public TSStatus clearCache() throws TException {
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    BloomFilterCache.getInstance().clear();
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus loadConfiguration() throws TException {
    try {
      IoTDBDescriptor.getInstance().loadHotModifiedProps();
    } catch (QueryProcessException e) {
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus setTTL(TSetTTLReq req) throws TException {
    return storageEngine.setTTL(req);
  }

  @Override
  public TSStatus updateConfigNodeGroup(TUpdateConfigNodeGroupReq req) {
    List<TConfigNodeLocation> configNodeLocations = req.getConfigNodeLocations();
    if (configNodeLocations != null) {
      ConfigNodeInfo.getInstance()
          .updateConfigNodeList(
              configNodeLocations
                  .parallelStream()
                  .map(TConfigNodeLocation::getInternalEndPoint)
                  .collect(Collectors.toList()));
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus updateTemplate(TUpdateTemplateReq req) throws TException {
    switch (TemplateInternalRPCUpdateType.getType(req.type)) {
      case ADD_TEMPLATE_SET_INFO:
        ClusterTemplateManager.getInstance().updateTemplateSetInfo(req.getTemplateInfo());
        break;
      case INVALIDATE_TEMPLATE_SET_INFO:
        ClusterTemplateManager.getInstance().invalidateTemplateSetInfo(req.getTemplateInfo());
        break;
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus deleteRegion(TConsensusGroupId tconsensusGroupId) {
    ConsensusGroupId consensusGroupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(tconsensusGroupId);
    if (consensusGroupId instanceof DataRegionId) {
      ConsensusGenericResponse response =
          DataRegionConsensusImpl.getInstance().deletePeer(consensusGroupId);
      if (!response.isSuccess()
          && !(response.getException() instanceof PeerNotInConsensusGroupException)) {
        return RpcUtils.getStatus(
            TSStatusCode.DELETE_REGION_ERROR, response.getException().getMessage());
      }
      StorageEngineV2.getInstance().deleteDataRegion((DataRegionId) consensusGroupId);
    } else {
      ConsensusGenericResponse response =
          SchemaRegionConsensusImpl.getInstance().deletePeer(consensusGroupId);
      if (!response.isSuccess()
          && !(response.getException() instanceof PeerNotInConsensusGroupException)) {
        return RpcUtils.getStatus(
            TSStatusCode.DELETE_REGION_ERROR, response.getException().getMessage());
      }
      try {
        SchemaEngine.getInstance().deleteSchemaRegion((SchemaRegionId) consensusGroupId);
      } catch (MetadataException e) {
        LOGGER.error("{}: MetaData error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
        return RpcUtils.getStatus(TSStatusCode.METADATA_ERROR, e.getMessage());
      }
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully");
  }

  public TSStatus changeRegionLeader(TRegionLeaderChangeReq req) throws TException {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    TConsensusGroupId tgId = req.getRegionId();
    ConsensusGroupId regionId = ConsensusGroupId.Factory.createFromTConsensusGroupId(tgId);
    TEndPoint newNode = getConsensusEndPoint(req.getNewLeaderNode(), regionId);
    Peer newLeaderPeer = new Peer(regionId, newNode);
    if (!isLeader(regionId)) {
      LOGGER.info("region {} is not leader, no need to change leader", regionId);
      return status;
    }
    LOGGER.info("region {} is leader, will change leader", regionId);
    return transferLeader(regionId, newLeaderPeer);
  }

  private TSStatus transferLeader(ConsensusGroupId regionId, Peer newLeaderPeer) {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    ConsensusGenericResponse resp;
    if (regionId instanceof DataRegionId) {
      resp = DataRegionConsensusImpl.getInstance().transferLeader(regionId, newLeaderPeer);
    } else if (regionId instanceof SchemaRegionId) {
      resp = SchemaRegionConsensusImpl.getInstance().transferLeader(regionId, newLeaderPeer);
    } else {
      status.setCode(TSStatusCode.REGION_LEADER_CHANGE_FAILED.getStatusCode());
      status.setMessage("Error Region type. region: " + regionId);
      return status;
    }
    if (!resp.isSuccess()) {
      LOGGER.error("change region {} leader failed", regionId, resp.getException());
      status.setCode(TSStatusCode.REGION_LEADER_CHANGE_FAILED.getStatusCode());
      status.setMessage(resp.getException().getMessage());
      return status;
    }
    status.setMessage("change region " + regionId + " leader succeed");
    return status;
  }

  private boolean isLeader(ConsensusGroupId regionId) {
    if (regionId instanceof DataRegionId) {
      return DataRegionConsensusImpl.getInstance().isLeader(regionId);
    }
    if (regionId instanceof SchemaRegionId) {
      return SchemaRegionConsensusImpl.getInstance().isLeader(regionId);
    }
    LOGGER.error("region {} type is illegal", regionId);
    return false;
  }

  @Override
  public TSStatus addToRegionConsensusGroup(TAddConsensusGroup req) throws TException {
    ConsensusGroupId regionId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getRegionId());
    List<Peer> peers =
        req.getRegionLocations().stream()
            .map(n -> getConsensusEndPoint(n, regionId))
            .map(node -> new Peer(regionId, node))
            .collect(Collectors.toList());
    TSStatus status = createNewRegion(regionId, req.getStorageGroup(), req.getTtl());
    if (!isSucceed(status)) {
      return status;
    }
    return addConsensusGroup(regionId, peers);
  }

  @Override
  public TSStatus removeRegionPeer(TMigrateRegionReq req) throws TException {
    TConsensusGroupId regionId = req.getRegionId();
    String fromNodeIp = req.getFromNode().getInternalEndPoint().getIp();
    boolean submitSucceed = RegionMigrateService.getInstance().submitRemoveRegionPeerTask(req);
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    if (submitSucceed) {
      LOGGER.info(
          "succeed to submit a remove region peer task. region: {}, from {}",
          regionId,
          req.getFromNode().getInternalEndPoint());
      return status;
    }
    status.setCode(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
    status.setMessage(
        "submit region remove region peer task failed, region: "
            + regionId
            + ", from "
            + req.getFromNode().getInternalEndPoint());
    return status;
  }

  @Override
  public TSStatus removeToRegionConsensusGroup(TMigrateRegionReq req) throws TException {
    TConsensusGroupId regionId = req.getRegionId();
    String fromNodeIp = req.getFromNode().getInternalEndPoint().getIp();
    boolean submitSucceed =
        RegionMigrateService.getInstance().submitRemoveRegionConsensusGroupTask(req);
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    if (submitSucceed) {
      LOGGER.info(
          "succeed to submit a remove region consensus group task. region: {}, from {}",
          regionId,
          fromNodeIp);
      return status;
    }
    status.setCode(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
    status.setMessage(
        "submit region remove region consensus group task failed, region: " + regionId);
    return status;
  }

  private TSStatus createNewRegion(ConsensusGroupId regionId, String storageGroup, long ttl) {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    LOGGER.info("start to create new region {}", regionId);
    try {
      if (regionId instanceof DataRegionId) {
        storageEngine.createDataRegion((DataRegionId) regionId, storageGroup, ttl);
      } else {
        schemaEngine.createSchemaRegion(new PartialPath(storageGroup), (SchemaRegionId) regionId);
      }
    } catch (Exception e) {
      LOGGER.error("create new region {} error", regionId, e);
      status.setCode(TSStatusCode.CREATE_REGION_ERROR.getStatusCode());
      status.setMessage("create new region " + regionId + "error,  exception:" + e.getMessage());
      return status;
    }
    status.setMessage("create new region " + regionId + " succeed");
    LOGGER.info("succeed to create new region {}", regionId);
    return status;
  }

  @Override
  public TSStatus createFunction(TCreateFunctionRequest request) {
    try {
      UDFRegistrationService.getInstance()
          .register(
              request.getUdfName(),
              request.getClassName(),
              request.getUris(),
              UDFExecutableManager.getInstance(),
              true);
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  @Override
  public TSStatus dropFunction(TDropFunctionRequest request) {
    try {
      UDFRegistrationService.getInstance().deregister(request.getUdfName());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  @Override
  public TSStatus addRegionPeer(TMigrateRegionReq req) throws TException {
    TConsensusGroupId regionId = req.getRegionId();
    String toNodeIp = req.getToNode().getInternalEndPoint().getIp();
    boolean submitSucceed = RegionMigrateService.getInstance().submitAddRegionPeerTask(req);
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    if (submitSucceed) {
      LOGGER.info(
          "succeed to submit a add region peer task. region: {}, to {}", regionId, toNodeIp);
      return status;
    }
    status.setCode(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
    status.setMessage("submit add region peer task failed, region: " + regionId);
    return status;
  }

  private TEndPoint getConsensusEndPoint(
      TDataNodeLocation nodeLocation, ConsensusGroupId regionId) {
    if (regionId instanceof DataRegionId) {
      return nodeLocation.getDataRegionConsensusEndPoint();
    }
    return nodeLocation.getSchemaRegionConsensusEndPoint();
  }

  private boolean isSucceed(TSStatus status) {
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  private TSStatus addConsensusGroup(ConsensusGroupId regionId, List<Peer> peers) {
    LOGGER.info("Start to add consensus group {} to region {}", peers, regionId);
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    ConsensusGenericResponse resp;
    if (regionId instanceof DataRegionId) {
      resp = DataRegionConsensusImpl.getInstance().createPeer(regionId, peers);
    } else {
      resp = SchemaRegionConsensusImpl.getInstance().createPeer(regionId, peers);
    }
    if (!resp.isSuccess()) {
      LOGGER.error(
          "add peers {} to region {} consensus group error", peers, regionId, resp.getException());
      status.setCode(TSStatusCode.REGION_MIGRATE_FAILED.getStatusCode());
      status.setMessage(resp.getException().getMessage());
      return status;
    }
    LOGGER.info("succeed to add peers {} to region {} consensus group", peers, regionId);
    status.setMessage("add peers to region consensus group " + regionId + "succeed");
    return status;
  }

  @Override
  public TSStatus disableDataNode(TDisableDataNodeReq req) throws TException {
    LOGGER.info("start disable data node in the request: {}", req);
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    status.setMessage("disable datanode succeed");
    // TODO what need to clean?
    ClusterPartitionFetcher.getInstance().invalidAllCache();
    DataNodeSchemaCache.getInstance().cleanUp();
    return status;
  }

  @Override
  public TSStatus stopDataNode() {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    LOGGER.info("stopping Data Node");
    try {
      DataNode.getInstance().stop();
      status.setMessage("stop datanode succeed");
    } catch (Exception e) {
      LOGGER.error("stop Data Node error", e);
      status.setCode(TSStatusCode.DATANODE_STOP_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
    }
    return status;
  }

  public void handleClientExit() {}
}
