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

import org.apache.iotdb.common.rpc.thrift.EndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.db.consensus.ConsensusImpl;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.CreateDataRegionReq;
import org.apache.iotdb.service.rpc.thrift.CreateSchemaRegionReq;
import org.apache.iotdb.service.rpc.thrift.ManagementIService;
import org.apache.iotdb.service.rpc.thrift.MigrateDataRegionReq;
import org.apache.iotdb.service.rpc.thrift.MigrateSchemaRegionReq;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DataNodeManagementServiceImpl implements ManagementIService.Iface {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeManagementServiceImpl.class);
  private final SchemaEngine schemaEngine = SchemaEngine.getInstance();
  private final StorageEngineV2 storageEngine = StorageEngineV2.getInstance();
  private final IConsensus consensusImpl = ConsensusImpl.getInstance();

  @Override
  public TSStatus createSchemaRegion(CreateSchemaRegionReq req) throws TException {
    TSStatus tsStatus;
    try {
      PartialPath storageGroupPartitionPath = new PartialPath(req.getStorageGroup());
      TRegionReplicaSet regionReplicaSet = req.getRegionReplicaSet();
      SchemaRegionId schemaRegionId =
          (SchemaRegionId)
              ConsensusGroupId.Factory.create(ByteBuffer.wrap(regionReplicaSet.getRegionId()));
      LOGGER.info("SchemaRegionId: " + schemaRegionId.getId());
      schemaEngine.createSchemaRegion(storageGroupPartitionPath, schemaRegionId);
      List<Peer> peers = new ArrayList<>();
      for (EndPoint endPoint : regionReplicaSet.getEndpoint()) {
        Endpoint endpoint = new Endpoint(endPoint.getIp(), endPoint.getPort());
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
  public TSStatus createDataRegion(CreateDataRegionReq req) throws TException {
    TSStatus tsStatus;
    try {
      TRegionReplicaSet regionReplicaSet = req.getRegionReplicaSet();
      DataRegionId dataRegionId =
          (DataRegionId)
              ConsensusGroupId.Factory.create(ByteBuffer.wrap(regionReplicaSet.getRegionId()));
      LOGGER.info("DataRegionId: " + dataRegionId.getId());
      storageEngine.createDataRegion(dataRegionId, req.storageGroup, req.ttl);
      List<Peer> peers = new ArrayList<>();
      for (EndPoint endPoint : regionReplicaSet.getEndpoint()) {
        Endpoint endpoint = new Endpoint(endPoint.getIp(), endPoint.getPort());
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
  public TSStatus migrateSchemaRegion(MigrateSchemaRegionReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus migrateDataRegion(MigrateDataRegionReq req) throws TException {
    return null;
  }

  public void handleClientExit() {}
}
