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
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.metadata.cache.DataNodeSchemaCache;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
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
import org.apache.iotdb.db.service.metrics.MetricsService;
import org.apache.iotdb.db.service.metrics.enums.Metric;
import org.apache.iotdb.db.service.metrics.enums.Tag;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.mpp.rpc.thrift.IDataNodeRPCService;
import org.apache.iotdb.mpp.rpc.thrift.TCancelFragmentInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TCancelPlanFragmentReq;
import org.apache.iotdb.mpp.rpc.thrift.TCancelQueryReq;
import org.apache.iotdb.mpp.rpc.thrift.TCancelResp;
import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateFunctionRequest;
import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropFunctionRequest;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStateReq;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceStateResp;
import org.apache.iotdb.mpp.rpc.thrift.THeartbeatReq;
import org.apache.iotdb.mpp.rpc.thrift.THeartbeatResp;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidatePermissionCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TMigrateRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TMigrateRegionResp;
import org.apache.iotdb.mpp.rpc.thrift.TSchemaFetchRequest;
import org.apache.iotdb.mpp.rpc.thrift.TSchemaFetchResponse;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceResp;
import org.apache.iotdb.mpp.rpc.thrift.TSendPlanNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendPlanNodeResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.exception.NotImplementedException;

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

public class DataNodeRPCServiceImpl implements IDataNodeRPCService.Iface {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeRPCServiceImpl.class);
  private final SchemaEngine schemaEngine = SchemaEngine.getInstance();
  private final StorageEngineV2 storageEngine = StorageEngineV2.getInstance();

  public DataNodeRPCServiceImpl() {
    super();
  }

  @Override
  public TSendFragmentInstanceResp sendFragmentInstance(TSendFragmentInstanceReq req) {
    LOGGER.info("receive FragmentInstance to group[{}]", req.getConsensusGroupId());
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    ConsensusReadResponse readResponse;
    // We deserialize here instead of the underlying state machine because parallelism is possible
    // here but not at the underlying state machine
    FragmentInstance fragmentInstance = FragmentInstance.deserializeFrom(req.fragmentInstance.body);
    if (groupId instanceof DataRegionId) {
      readResponse = DataRegionConsensusImpl.getInstance().read(groupId, fragmentInstance);
    } else {
      readResponse = SchemaRegionConsensusImpl.getInstance().read(groupId, fragmentInstance);
    }
    if (!readResponse.isSuccess()) {
      LOGGER.error(
          "execute FragmentInstance in ConsensusGroup {} failed because {}",
          req.getConsensusGroupId(),
          readResponse.getException());
      return new TSendFragmentInstanceResp(false);
    }
    FragmentInstanceInfo info = (FragmentInstanceInfo) readResponse.getDataset();
    return new TSendFragmentInstanceResp(!info.getState().isFailed());
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
        LOGGER.warn(
            "Fail to insert measurements {} caused by {}",
            insertNode.getFailedMeasurements(),
            insertNode.getFailedMessages());
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
      response.setMessage(writeResponse.getStatus().message);
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
    try (SetThreadName threadName = new SetThreadName(instanceId.getFullId())) {
      FragmentInstanceInfo info = FragmentInstanceManager.getInstance().getInstanceInfo(instanceId);
      return info != null
          ? new TFragmentInstanceStateResp(info.getState().toString())
          : new TFragmentInstanceStateResp(FragmentInstanceState.NO_SUCH_INSTANCE.toString());
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
          SchemaRegionConsensusImpl.getInstance().addConsensusGroup(schemaRegionId, peers);
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
          DataRegionConsensusImpl.getInstance().addConsensusGroup(dataRegionId, peers);
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
          MetricsService.getInstance()
              .getMetricManager()
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
            MetricsService.getInstance()
                .getMetricManager()
                .getOrCreateGauge(gaugeName, MetricLevel.IMPORTANT, "id", id, "area", "heap");
        result += gauge.value();
      }
      for (String id : noHeapIds) {
        Gauge gauge =
            MetricsService.getInstance()
                .getMetricManager()
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
  public TSStatus flush(TFlushReq req) throws TException {
    return StorageEngineV2.getInstance().operateFlush(req);
  }

  @Override
  public TSStatus setTTL(TSetTTLReq req) throws TException {
    return StorageEngineV2.getInstance().setTTL(req);
  }

  @Override
  public TSStatus deleteRegion(TConsensusGroupId tconsensusGroupId) {
    ConsensusGroupId consensusGroupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(tconsensusGroupId);
    if (consensusGroupId instanceof DataRegionId) {
      ConsensusGenericResponse response =
          DataRegionConsensusImpl.getInstance().removeConsensusGroup(consensusGroupId);
      if (!response.isSuccess()
          && !(response.getException() instanceof PeerNotInConsensusGroupException)) {
        return RpcUtils.getStatus(
            TSStatusCode.DELETE_REGION_ERROR, response.getException().getMessage());
      }
      StorageEngineV2.getInstance().deleteDataRegion((DataRegionId) consensusGroupId);
    } else {
      ConsensusGenericResponse response =
          SchemaRegionConsensusImpl.getInstance().removeConsensusGroup(consensusGroupId);
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

  @Override
  public TMigrateRegionResp migrateRegion(TMigrateRegionReq req) {
    TRegionReplicaSet regionReplicaSet = req.migrateRegion;
    TSStatus tsStatus;
    ConsensusGenericResponse consensusGenericResponse;
    switch (regionReplicaSet.regionId.type) {
      case DataRegion:
        DataRegionId dataRegionId = new DataRegionId(regionReplicaSet.getRegionId().getId());
        List<Peer> newPeers = new ArrayList<>();
        for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
          TEndPoint endpoint =
              new TEndPoint(
                  dataNodeLocation.getDataRegionConsensusEndPoint().getIp(),
                  dataNodeLocation.getDataRegionConsensusEndPoint().getPort());
          newPeers.add(new Peer(dataRegionId, endpoint));
        }
        consensusGenericResponse =
            DataRegionConsensusImpl.getInstance().changePeer(dataRegionId, newPeers);
        break;
      case SchemaRegion:
        SchemaRegionId schemaRegionId = new SchemaRegionId(regionReplicaSet.getRegionId().getId());
        newPeers = new ArrayList<>();
        for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
          TEndPoint endpoint =
              new TEndPoint(
                  dataNodeLocation.getSchemaRegionConsensusEndPoint().getIp(),
                  dataNodeLocation.getSchemaRegionConsensusEndPoint().getPort());
          newPeers.add(new Peer(schemaRegionId, endpoint));
        }
        consensusGenericResponse =
            SchemaRegionConsensusImpl.getInstance().changePeer(schemaRegionId, newPeers);
        break;
      default:
        // unsupported region type
        tsStatus = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
        tsStatus.setMessage("Region type is invalid");
        return new TMigrateRegionResp(tsStatus);
    }

    if (consensusGenericResponse.isSuccess()) {
      tsStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } else {
      tsStatus = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      tsStatus.setMessage(consensusGenericResponse.getException().getMessage());
    }

    return new TMigrateRegionResp(tsStatus);
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

  public void handleClientExit() {}
}
