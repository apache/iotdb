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
import org.apache.iotdb.common.rpc.thrift.THeartbeatReq;
import org.apache.iotdb.common.rpc.thrift.THeartbeatResp;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.metadata.cache.DataNodeSchemaCache;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceInfo;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.mpp.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.analyze.SchemaValidator;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.DeleteRegionNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.mpp.rpc.thrift.InternalService;
import org.apache.iotdb.mpp.rpc.thrift.TCancelFragmentInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TCancelPlanFragmentReq;
import org.apache.iotdb.mpp.rpc.thrift.TCancelQueryReq;
import org.apache.iotdb.mpp.rpc.thrift.TCancelResp;
import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStateReq;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceStateResp;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TMigrateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TMigrateSchemaRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TSchemaFetchRequest;
import org.apache.iotdb.mpp.rpc.thrift.TSchemaFetchResponse;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceResp;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.exception.NotImplementedException;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class InternalServiceImpl implements InternalService.Iface {

  private static final Logger LOGGER = LoggerFactory.getLogger(InternalServiceImpl.class);
  private final SchemaEngine schemaEngine = SchemaEngine.getInstance();
  private final StorageEngineV2 storageEngine = StorageEngineV2.getInstance();

  public InternalServiceImpl() {
    super();
  }

  @Override
  public TSendFragmentInstanceResp sendFragmentInstance(TSendFragmentInstanceReq req) {
    LOGGER.info("receive FragmentInstance to group[{}]", req.getConsensusGroupId());
    QueryType type = QueryType.valueOf(req.queryType);
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    switch (type) {
      case READ:
        ConsensusReadResponse readResponse;
        if (groupId instanceof DataRegionId) {
          readResponse =
              DataRegionConsensusImpl.getInstance()
                  .read(groupId, new ByteBufferConsensusRequest(req.fragmentInstance.body));
        } else {
          readResponse =
              SchemaRegionConsensusImpl.getInstance()
                  .read(groupId, new ByteBufferConsensusRequest(req.fragmentInstance.body));
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
      case WRITE:
        TSendFragmentInstanceResp response = new TSendFragmentInstanceResp();
        ConsensusWriteResponse writeResponse;

        FragmentInstance fragmentInstance =
            FragmentInstance.deserializeFrom(req.fragmentInstance.body);
        PlanNode planNode = fragmentInstance.getFragment().getRoot();
        if (planNode instanceof InsertNode) {
          try {
            SchemaValidator.validate((InsertNode) planNode);
          } catch (SemanticException e) {
            response.setAccepted(false);
            response.setMessage(e.getMessage());
            return response;
          }
        }
        if (groupId instanceof DataRegionId) {
          writeResponse = DataRegionConsensusImpl.getInstance().write(groupId, fragmentInstance);
        } else {
          writeResponse = SchemaRegionConsensusImpl.getInstance().write(groupId, fragmentInstance);
        }
        // TODO need consider more status
        response.setAccepted(
            TSStatusCode.SUCCESS_STATUS.getStatusCode() == writeResponse.getStatus().getCode());
        response.setMessage(writeResponse.getStatus().message);
        return response;
    }
    return null;
  }

  @Override
  public TFragmentInstanceStateResp fetchFragmentInstanceState(TFetchFragmentInstanceStateReq req) {
    FragmentInstanceInfo info =
        FragmentInstanceManager.getInstance()
            .getInstanceInfo(FragmentInstanceId.fromThrift(req.fragmentInstanceId));
    return new TFragmentInstanceStateResp(info.getState().toString());
  }

  @Override
  public TCancelResp cancelQuery(TCancelQueryReq req) throws TException {
    List<FragmentInstanceId> taskIds =
        req.getFragmentInstanceIds().stream()
            .map(FragmentInstanceId::fromThrift)
            .collect(Collectors.toList());
    for (FragmentInstanceId taskId : taskIds) {
      FragmentInstanceManager.getInstance().cancelTask(taskId);
    }
    return new TCancelResp(true);
  }

  @Override
  public TCancelResp cancelPlanFragment(TCancelPlanFragmentReq req) throws TException {
    throw new NotImplementedException();
  }

  @Override
  public TCancelResp cancelFragmentInstance(TCancelFragmentInstanceReq req) throws TException {
    throw new NotImplementedException();
  }

  @Override
  public TSchemaFetchResponse fetchSchema(TSchemaFetchRequest req) throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public TSStatus createSchemaRegion(TCreateSchemaRegionReq req) throws TException {
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
        tsStatus = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
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
      tsStatus = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      tsStatus.setMessage(
          String.format("Create Schema Region failed because of %s", e2.getMessage()));
    }
    return tsStatus;
  }

  @Override
  public TSStatus createDataRegion(TCreateDataRegionReq req) throws TException {
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
        tsStatus = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
        tsStatus.setMessage(consensusGenericResponse.getException().getMessage());
      }
    } catch (DataRegionException e) {
      LOGGER.error(
          "Create Data Region {} failed because {}", req.getStorageGroup(), e.getMessage());
      tsStatus = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      tsStatus.setMessage(String.format("Create Data Region failed because of %s", e.getMessage()));
    }
    return tsStatus;
  }

  @Override
  public TSStatus invalidatePartitionCache(TInvalidateCacheReq req) throws TException {
    ClusterPartitionFetcher.getInstance().invalidAllCache();
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus invalidateSchemaCache(TInvalidateCacheReq req) throws TException {
    DataNodeSchemaCache.getInstance().cleanUp();
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus migrateSchemaRegion(TMigrateSchemaRegionReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus migrateDataRegion(TMigrateDataRegionReq req) throws TException {
    return null;
  }

  @Override
  public THeartbeatResp getHeartBeat(THeartbeatReq req) throws TException {
    // TODO: Return load balancing messages
    return new THeartbeatResp(req.getHeartbeatTimestamp());
  }

  @Override
  public TSStatus deleteRegion(TConsensusGroupId tconsensusGroupId) throws TException {
    long queryIdRaw = SessionManager.getInstance().requestQueryId(false);
    QueryId queryId = new QueryId(String.valueOf(queryIdRaw));
    PlanNodeId planNodeId = queryId.genPlanNodeId();
    DeleteRegionNode deleteRegionNode = new DeleteRegionNode(queryId.genPlanNodeId());
    ConsensusGroupId consensusGroupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(tconsensusGroupId);
    deleteRegionNode.setConsensusGroupId(consensusGroupId);
    deleteRegionNode.setPlanNodeId(planNodeId);
    PlanFragmentId planFragmentId = queryId.genPlanFragmentId();
    FragmentInstanceId fragmentInstanceId = planFragmentId.genFragmentInstanceId();
    PlanFragment planFragment = new PlanFragment(planFragmentId, deleteRegionNode);
    FragmentInstance fragmentInstance =
        new FragmentInstance(planFragment, fragmentInstanceId, null, QueryType.WRITE);
    if (consensusGroupId instanceof DataRegionId) {
      return DataRegionConsensusImpl.getInstance()
          .write(consensusGroupId, fragmentInstance)
          .getStatus();
    } else {
      return SchemaRegionConsensusImpl.getInstance()
          .write(consensusGroupId, fragmentInstance)
          .getStatus();
    }
  }

  public void handleClientExit() {}
}
