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
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.mpp.plan.analyze.SchemaValidator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.mpp.rpc.thrift.TSendPlanNodeResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class takes the responsibility of managing regions, executing PlanNode of write type on
 * according regions and controlling the execution concurrency.
 */
public class DataNodeRegionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeRegionManager.class);

  private final SchemaEngine schemaEngine;
  private final StorageEngineV2 storageEngine;

  private final Map<SchemaRegionId, ReentrantReadWriteLock> schemaRegionLockMap =
      new ConcurrentHashMap<>();
  private final Map<DataRegionId, ReentrantReadWriteLock> dataRegionLockMap =
      new ConcurrentHashMap<>();

  public DataNodeRegionManager(SchemaEngine schemaEngine, StorageEngineV2 storageEngine) {
    this.schemaEngine = schemaEngine;
    this.storageEngine = storageEngine;
    schemaEngine
        .getAllSchemaRegions()
        .forEach(
            schemaRegion -> {
              schemaRegionLockMap.put(
                  schemaRegion.getSchemaRegionId(), new ReentrantReadWriteLock(false));
            });

    storageEngine
        .getAllDataRegionIds()
        .forEach(
            dataRegionId -> dataRegionLockMap.put(dataRegionId, new ReentrantReadWriteLock(false)));
  }

  public TSendPlanNodeResp executePlanNode(ConsensusGroupId groupId, PlanNode planNode) {
    if (planNode instanceof InsertNode) {
      return executeDataInsert((DataRegionId) groupId, (InsertNode) planNode);
    } else {
      TSendPlanNodeResp response = new TSendPlanNodeResp();
      ConsensusWriteResponse writeResponse = executePlanNodeInConsensusLayer(groupId, planNode);
      // TODO need consider more status
      if (writeResponse.getStatus() != null) {
        response.setAccepted(
            TSStatusCode.SUCCESS_STATUS.getStatusCode() == writeResponse.getStatus().getCode());
        response.setMessage(writeResponse.getStatus().message);
        response.setStatus(writeResponse.getStatus());
      } else {
        LOGGER.error(
            "Something wrong happened while calling consensus layer's write API.",
            writeResponse.getException());
        response.setAccepted(false);
        response.setMessage(writeResponse.getException().getMessage());
      }
      return response;
    }
  }

  private ConsensusWriteResponse executePlanNodeInConsensusLayer(
      ConsensusGroupId groupId, PlanNode planNode) {
    if (groupId instanceof DataRegionId) {
      return DataRegionConsensusImpl.getInstance().write(groupId, planNode);
    } else {
      return SchemaRegionConsensusImpl.getInstance().write(groupId, planNode);
    }
  }

  private TSendPlanNodeResp executeDataInsert(DataRegionId dataRegionId, InsertNode insertNode) {
    TSendPlanNodeResp response = new TSendPlanNodeResp();
    dataRegionLockMap.get(dataRegionId).readLock().lock();
    try {
      try {
        SchemaValidator.validate(insertNode);
      } catch (SemanticException e) {
        response.setAccepted(false);
        response.setStatus(
            RpcUtils.getStatus(TSStatusCode.METADATA_ERROR.getStatusCode(), e.getMessage()));
        response.setMessage(e.getMessage());
        return response;
      }
      boolean hasFailedMeasurement = insertNode.hasFailedMeasurements();
      String partialInsertMessage = null;
      if (hasFailedMeasurement) {
        partialInsertMessage =
            String.format(
                "Fail to insert measurements %s caused by %s",
                insertNode.getFailedMeasurements(), insertNode.getFailedMessages());
        LOGGER.warn(partialInsertMessage);
      }

      ConsensusWriteResponse writeResponse =
          executePlanNodeInConsensusLayer(dataRegionId, insertNode);

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
              RpcUtils.getStatus(
                  TSStatusCode.METADATA_ERROR.getStatusCode(), partialInsertMessage));
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
    } finally {
      dataRegionLockMap.get(dataRegionId).readLock().unlock();
    }
  }

  public TSStatus executeSchemaPlanNode(SchemaRegionId schemaRegionId, PlanNode planNode) {
    ConsensusWriteResponse writeResponse =
        executePlanNodeInConsensusLayer(schemaRegionId, planNode);
    TSStatus status = writeResponse.getStatus();
    if (status == null) {
      LOGGER.error(
          "Something wrong happened while calling consensus layer's write API.",
          writeResponse.getException());
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
    return status;
  }

  public TSStatus executeDeleteDataForDeleteTimeSeries(
      DataRegionId dataRegionId, PlanNode planNode) {
    dataRegionLockMap.get(dataRegionId).writeLock().lock();
    try {
      ConsensusWriteResponse writeResponse =
          executePlanNodeInConsensusLayer(dataRegionId, planNode);
      TSStatus status = writeResponse.getStatus();
      if (status == null) {
        LOGGER.error(
            "Something wrong happened while calling consensus layer's write API.",
            writeResponse.getException());
        return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
      }
      return status;
    } finally {
      dataRegionLockMap.get(dataRegionId).writeLock().unlock();
    }
  }

  public TSStatus createSchemaRegion(TRegionReplicaSet regionReplicaSet, String storageGroup) {
    TSStatus tsStatus;
    try {
      PartialPath storageGroupPartitionPath = new PartialPath(storageGroup);
      SchemaRegionId schemaRegionId = new SchemaRegionId(regionReplicaSet.getRegionId().getId());
      schemaEngine.createSchemaRegion(storageGroupPartitionPath, schemaRegionId);
      schemaRegionLockMap.put(schemaRegionId, new ReentrantReadWriteLock(false));
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
      LOGGER.error("Create Schema Region {} failed because path is illegal.", storageGroup);
      tsStatus = new TSStatus(TSStatusCode.PATH_ILLEGAL.getStatusCode());
      tsStatus.setMessage("Create Schema Region failed because storageGroup path is illegal.");
    } catch (MetadataException e2) {
      LOGGER.error("Create Schema Region {} failed because {}", storageGroup, e2.getMessage());
      tsStatus = new TSStatus(TSStatusCode.CREATE_REGION_ERROR.getStatusCode());
      tsStatus.setMessage(
          String.format("Create Schema Region failed because of %s", e2.getMessage()));
    }
    return tsStatus;
  }

  public TSStatus createDataRegion(
      TRegionReplicaSet regionReplicaSet, String storageGroup, long ttl) {
    TSStatus tsStatus;
    try {
      DataRegionId dataRegionId = new DataRegionId(regionReplicaSet.getRegionId().getId());
      storageEngine.createDataRegion(dataRegionId, storageGroup, ttl);
      dataRegionLockMap.put(dataRegionId, new ReentrantReadWriteLock(false));
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
      LOGGER.error("Create Data Region {} failed because {}", storageGroup, e.getMessage());
      tsStatus = new TSStatus(TSStatusCode.CREATE_REGION_ERROR.getStatusCode());
      tsStatus.setMessage(String.format("Create Data Region failed because of %s", e.getMessage()));
    }
    return tsStatus;
  }

  public TSStatus createNewRegion(ConsensusGroupId regionId, String storageGroup, long ttl) {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    LOGGER.info("start to create new region {}", regionId);
    try {
      if (regionId instanceof DataRegionId) {
        DataRegionId dataRegionId = (DataRegionId) regionId;
        storageEngine.createDataRegion(dataRegionId, storageGroup, ttl);
        dataRegionLockMap.put(dataRegionId, new ReentrantReadWriteLock(false));
      } else {
        SchemaRegionId schemaRegionId = (SchemaRegionId) regionId;
        schemaEngine.createSchemaRegion(new PartialPath(storageGroup), schemaRegionId);
        schemaRegionLockMap.put(schemaRegionId, new ReentrantReadWriteLock(false));
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

  public TSStatus deleteDataRegion(DataRegionId dataRegionId) {
    storageEngine.deleteDataRegion(dataRegionId);
    dataRegionLockMap.remove(dataRegionId);
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully");
  }

  public TSStatus deleteSchemaRegion(SchemaRegionId schemaRegionId) {
    try {
      schemaEngine.deleteSchemaRegion(schemaRegionId);
      schemaRegionLockMap.remove(schemaRegionId);
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully");
    } catch (MetadataException e) {
      LOGGER.error("{}: MetaData error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils.getStatus(TSStatusCode.METADATA_ERROR, e.getMessage());
    }
  }
}
