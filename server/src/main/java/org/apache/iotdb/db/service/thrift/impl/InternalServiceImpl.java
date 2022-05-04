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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.db.consensus.ConsensusImpl;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceInfo;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.analyze.SchemaValidator;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.mpp.rpc.thrift.InternalService;
import org.apache.iotdb.mpp.rpc.thrift.TCancelFragmentInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TCancelPlanFragmentReq;
import org.apache.iotdb.mpp.rpc.thrift.TCancelQueryReq;
import org.apache.iotdb.mpp.rpc.thrift.TCancelResp;
import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStateReq;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceStateResp;
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

public class InternalServiceImpl implements InternalService.Iface {

  private static final Logger LOGGER = LoggerFactory.getLogger(InternalServiceImpl.class);
  private final SchemaEngine schemaEngine = SchemaEngine.getInstance();
  private final StorageEngineV2 storageEngine = StorageEngineV2.getInstance();
  private final IConsensus consensusImpl = ConsensusImpl.getInstance();

  public InternalServiceImpl() {
    super();
  }

  @Override
  public TSendFragmentInstanceResp sendFragmentInstance(TSendFragmentInstanceReq req) {
    QueryType type = QueryType.valueOf(req.queryType);
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    switch (type) {
      case READ:
        ConsensusReadResponse readResp =
            ConsensusImpl.getInstance()
                .read(groupId, new ByteBufferConsensusRequest(req.fragmentInstance.body));
        FragmentInstanceInfo info = (FragmentInstanceInfo) readResp.getDataset();
        return new TSendFragmentInstanceResp(!info.getState().isFailed());
      case WRITE:
        TSendFragmentInstanceResp response = new TSendFragmentInstanceResp();
        ConsensusWriteResponse resp;

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
        resp = ConsensusImpl.getInstance().write(groupId, fragmentInstance);
        // TODO need consider more status
        response.setAccepted(
            TSStatusCode.SUCCESS_STATUS.getStatusCode() == resp.getStatus().getCode());
        response.setMessage(resp.getStatus().message);
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

    // TODO need to be implemented and currently in order not to print NotImplementedException log,
    // we simply return null
    return new TCancelResp(true);
    //    throw new NotImplementedException();
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
                dataNodeLocation.getConsensusEndPoint().getIp(),
                dataNodeLocation.getConsensusEndPoint().getPort());
        peers.add(new Peer(schemaRegionId, endpoint));
      }
      ConsensusGenericResponse consensusGenericResponse =
          consensusImpl.addConsensusGroup(schemaRegionId, peers);
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
                dataNodeLocation.getConsensusEndPoint().getIp(),
                dataNodeLocation.getConsensusEndPoint().getPort());
        peers.add(new Peer(dataRegionId, endpoint));
      }
      ConsensusGenericResponse consensusGenericResponse =
          consensusImpl.addConsensusGroup(dataRegionId, peers);
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
  public TSStatus migrateSchemaRegion(TMigrateSchemaRegionReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus migrateDataRegion(TMigrateDataRegionReq req) throws TException {
    return null;
  }

  public void handleClientExit() {}
}
