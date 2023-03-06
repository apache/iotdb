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
package org.apache.iotdb.db.service;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionMigrateFailedType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.confignode.rpc.thrift.TRegionMigrateResultReportReq;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeClientManager;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.rescon.AbstractPoolManager;
import org.apache.iotdb.mpp.rpc.thrift.TMaintainPeerReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class RegionMigrateService implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(RegionMigrateService.class);

  public static final String REGION_MIGRATE_PROCESS = "[REGION_MIGRATE_PROCESS]";

  private static final int MAX_RETRY_NUM = 5;

  private static final int SLEEP_MILLIS = 5000;

  private RegionMigratePool regionMigratePool;

  private RegionMigrateService() {}

  public static RegionMigrateService getInstance() {
    return Holder.INSTANCE;
  }

  /**
   * Submit AddRegionPeerTask
   *
   * @param req TMaintainPeerReq
   * @return if the submit task succeed
   */
  public synchronized boolean submitAddRegionPeerTask(TMaintainPeerReq req) {

    boolean submitSucceed = true;
    try {
      regionMigratePool.submit(new AddRegionPeerTask(req.getRegionId(), req.getDestNode()));
    } catch (Exception e) {
      LOGGER.error(
          "{}, Submit AddRegionPeerTask error for Region: {}",
          REGION_MIGRATE_PROCESS,
          req.getRegionId(),
          e);
      submitSucceed = false;
    }
    return submitSucceed;
  }

  /**
   * Submit RemoveRegionPeerTask
   *
   * @param req TMaintainPeerReq
   * @return if the submit task succeed
   */
  public synchronized boolean submitRemoveRegionPeerTask(TMaintainPeerReq req) {

    boolean submitSucceed = true;
    try {
      regionMigratePool.submit(new RemoveRegionPeerTask(req.getRegionId(), req.getDestNode()));
    } catch (Exception e) {
      LOGGER.error(
          "{}, Submit RemoveRegionPeer task error for Region: {}",
          REGION_MIGRATE_PROCESS,
          req.getRegionId(),
          e);
      submitSucceed = false;
    }
    return submitSucceed;
  }

  /**
   * Submit DeleteOldRegionPeerTask
   *
   * @param req TMigrateRegionReq
   * @return if the submit task succeed
   */
  public synchronized boolean submitDeleteOldRegionPeerTask(TMaintainPeerReq req) {

    boolean submitSucceed = true;
    try {
      regionMigratePool.submit(new DeleteOldRegionPeerTask(req.getRegionId(), req.getDestNode()));
    } catch (Exception e) {
      LOGGER.error(
          "{}, Submit DeleteOldRegionPeerTask error for Region: {}",
          REGION_MIGRATE_PROCESS,
          req.getRegionId(),
          e);
      submitSucceed = false;
    }
    return submitSucceed;
  }

  @Override
  public void start() throws StartupException {
    regionMigratePool = new RegionMigratePool();
    regionMigratePool.start();
    LOGGER.info("Region migrate service start");
  }

  @Override
  public void stop() {
    if (regionMigratePool != null) {
      regionMigratePool.stop();
    }
    LOGGER.info("Region migrate service stop");
  }

  @Override
  public ServiceType getID() {
    return ServiceType.DATA_NODE_REGION_MIGRATE_SERVICE;
  }

  private static class RegionMigratePool extends AbstractPoolManager {

    private final Logger poolLogger = LoggerFactory.getLogger(RegionMigratePool.class);

    private RegionMigratePool() {
      this.pool = IoTDBThreadPoolFactory.newSingleThreadExecutor("Region-Migrate-Pool");
    }

    @Override
    public Logger getLogger() {
      return poolLogger;
    }

    @Override
    public void start() {
      if (this.pool != null) {
        poolLogger.info("DataNode region migrate pool start");
      }
    }

    @Override
    public String getName() {
      return "migrate region";
    }
  }

  private static class AddRegionPeerTask implements Runnable {

    private static final Logger taskLogger = LoggerFactory.getLogger(AddRegionPeerTask.class);

    // The RegionGroup that shall perform the add peer process
    private final TConsensusGroupId tRegionId;

    // The new DataNode to be added in the RegionGroup
    private final TDataNodeLocation destDataNode;

    public AddRegionPeerTask(TConsensusGroupId tRegionId, TDataNodeLocation destDataNode) {
      this.tRegionId = tRegionId;
      this.destDataNode = destDataNode;
    }

    @Override
    public void run() {
      TSStatus runResult = addPeer();
      if (isFailed(runResult)) {
        reportFailed(tRegionId, destDataNode, TRegionMigrateFailedType.AddPeerFailed, runResult);
        return;
      }

      reportSucceed(tRegionId, "AddPeer");
    }

    private TSStatus addPeer() {
      taskLogger.info(
          "{}, Start to addPeer {} for region {}", REGION_MIGRATE_PROCESS, destDataNode, tRegionId);
      ConsensusGroupId regionId = ConsensusGroupId.Factory.createFromTConsensusGroupId(tRegionId);
      TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      ConsensusGenericResponse resp = null;
      TEndPoint destEndpoint = getConsensusEndPoint(destDataNode, regionId);
      boolean addPeerSucceed = true;
      for (int i = 0; i < MAX_RETRY_NUM; i++) {
        try {
          if (!addPeerSucceed) {
            Thread.sleep(SLEEP_MILLIS);
          }
          resp =
              addRegionPeer(
                  regionId, new Peer(regionId, destDataNode.getDataNodeId(), destEndpoint));
        } catch (Throwable e) {
          addPeerSucceed = false;
          taskLogger.error(
              "{}, executed addPeer {} for region {} error, retry times: {}",
              REGION_MIGRATE_PROCESS,
              destEndpoint,
              regionId,
              i,
              e);
        }
        if (addPeerSucceed && resp != null && resp.isSuccess()) {
          break;
        }
      }

      if (!addPeerSucceed || resp == null || !resp.isSuccess()) {
        String errorMsg =
            String.format(
                "%s, AddPeer for region error after max retry times, peerId: %s, regionId: %s, resp: %s",
                REGION_MIGRATE_PROCESS, destEndpoint, regionId, resp);
        taskLogger.error(errorMsg);
        status.setCode(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
        status.setMessage(errorMsg);
        return status;
      }

      taskLogger.info(
          "{}, Succeed to addPeer {} for region {}",
          REGION_MIGRATE_PROCESS,
          destEndpoint,
          regionId);
      status.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      status.setMessage("addPeer " + destEndpoint + " for region " + regionId + " succeed");
      return status;
    }

    private ConsensusGenericResponse addRegionPeer(ConsensusGroupId regionId, Peer newPeer) {
      ConsensusGenericResponse resp;
      if (regionId instanceof DataRegionId) {
        resp = DataRegionConsensusImpl.getInstance().addPeer(regionId, newPeer);
      } else {
        resp = SchemaRegionConsensusImpl.getInstance().addPeer(regionId, newPeer);
      }
      return resp;
    }
  }

  private static class RemoveRegionPeerTask implements Runnable {

    private static final Logger taskLogger = LoggerFactory.getLogger(RemoveRegionPeerTask.class);

    private final TConsensusGroupId tRegionId;

    private final TDataNodeLocation destDataNode;

    public RemoveRegionPeerTask(TConsensusGroupId tRegionId, TDataNodeLocation destDataNode) {
      this.tRegionId = tRegionId;
      this.destDataNode = destDataNode;
    }

    @Override
    public void run() {
      TSStatus runResult = removePeer();
      if (isSucceed(runResult)) {
        reportSucceed(tRegionId, "RemovePeer");
      } else {
        reportFailed(tRegionId, destDataNode, TRegionMigrateFailedType.RemovePeerFailed, runResult);
      }
    }

    private TSStatus removePeer() {
      ConsensusGroupId regionId = ConsensusGroupId.Factory.createFromTConsensusGroupId(tRegionId);
      TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      TEndPoint destEndPoint = getConsensusEndPoint(destDataNode, regionId);
      taskLogger.info(
          "{}, Start to removePeer {} for region {}",
          REGION_MIGRATE_PROCESS,
          destEndPoint,
          regionId);
      ConsensusGenericResponse resp = null;
      boolean removePeerSucceed = true;
      for (int i = 0; i < MAX_RETRY_NUM; i++) {
        try {
          if (!removePeerSucceed) {
            Thread.sleep(SLEEP_MILLIS);
          }
          resp =
              removeRegionPeer(
                  regionId, new Peer(regionId, destDataNode.getDataNodeId(), destEndPoint));
        } catch (Throwable e) {
          removePeerSucceed = false;
          taskLogger.error(
              "{}, executed removePeer {} for region {} error, retry times: {}",
              REGION_MIGRATE_PROCESS,
              destEndPoint,
              regionId,
              i,
              e);
        }
        if (removePeerSucceed && resp != null && resp.isSuccess()) {
          break;
        }
      }

      if (!removePeerSucceed || resp == null || !resp.isSuccess()) {
        String errorMsg =
            String.format(
                "%s, RemovePeer for region error after max retry times, peerId: %s, regionId: %s, resp: %s",
                REGION_MIGRATE_PROCESS, destEndPoint, regionId, resp);
        taskLogger.error(errorMsg);
        status.setCode(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
        status.setMessage(errorMsg);
        return status;
      }

      taskLogger.info(
          "{}, Succeed to removePeer {} for region {}",
          REGION_MIGRATE_PROCESS,
          destEndPoint,
          regionId);
      status.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      status.setMessage("removePeer " + destEndPoint + " for region " + regionId + " succeed");
      return status;
    }

    private ConsensusGenericResponse removeRegionPeer(ConsensusGroupId regionId, Peer oldPeer) {
      ConsensusGenericResponse resp;
      if (regionId instanceof DataRegionId) {
        resp = DataRegionConsensusImpl.getInstance().removePeer(regionId, oldPeer);
      } else {
        resp = SchemaRegionConsensusImpl.getInstance().removePeer(regionId, oldPeer);
      }
      return resp;
    }
  }

  private static class DeleteOldRegionPeerTask implements Runnable {

    private static final Logger taskLogger = LoggerFactory.getLogger(DeleteOldRegionPeerTask.class);

    private final TConsensusGroupId tRegionId;

    private final TDataNodeLocation originalDataNode;

    public DeleteOldRegionPeerTask(
        TConsensusGroupId tRegionId, TDataNodeLocation originalDataNode) {
      this.tRegionId = tRegionId;
      this.originalDataNode = originalDataNode;
    }

    @Override
    public void run() {
      // deletePeer: remove the peer from the consensus group
      TSStatus runResult = deletePeer();
      if (isFailed(runResult)) {
        reportFailed(
            tRegionId,
            originalDataNode,
            TRegionMigrateFailedType.RemoveConsensusGroupFailed,
            runResult);
      }

      // deleteRegion: delete region data
      runResult = deleteRegion();
      if (isFailed(runResult)) {
        reportFailed(
            tRegionId, originalDataNode, TRegionMigrateFailedType.DeleteRegionFailed, runResult);
      }

      reportSucceed(tRegionId, "DeletePeer");
    }

    private TSStatus deletePeer() {
      taskLogger.info(
          "{}, Start to deletePeer {} for region {}",
          REGION_MIGRATE_PROCESS,
          originalDataNode,
          tRegionId);
      ConsensusGroupId regionId = ConsensusGroupId.Factory.createFromTConsensusGroupId(tRegionId);
      TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      ConsensusGenericResponse resp;
      try {
        if (regionId instanceof DataRegionId) {
          resp = DataRegionConsensusImpl.getInstance().deletePeer(regionId);
        } else {
          resp = SchemaRegionConsensusImpl.getInstance().deletePeer(regionId);
        }
      } catch (Throwable e) {
        taskLogger.error("{}, deletePeer error, regionId: {}", REGION_MIGRATE_PROCESS, regionId, e);
        status.setCode(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
        status.setMessage(
            "deletePeer for region: " + regionId + " error. exception: " + e.getMessage());
        return status;
      }
      if (!resp.isSuccess()) {
        String errorMsg =
            String.format(
                "deletePeer error, regionId: %s, errorMessage: %s",
                regionId, resp.getException().getMessage());
        taskLogger.error(errorMsg);
        status.setCode(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
        status.setMessage(errorMsg);
        return status;
      }
      taskLogger.info(
          "{}, Succeed to deletePeer {} from consensus group", REGION_MIGRATE_PROCESS, regionId);
      status.setMessage("deletePeer from consensus group " + regionId + "succeed");
      return status;
    }

    private TSStatus deleteRegion() {
      taskLogger.info(
          "{}, Start to deleteRegion {} for datanode {}",
          REGION_MIGRATE_PROCESS,
          tRegionId,
          originalDataNode);
      TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      ConsensusGroupId regionId = ConsensusGroupId.Factory.createFromTConsensusGroupId(tRegionId);
      try {
        if (regionId instanceof DataRegionId) {
          StorageEngine.getInstance().deleteDataRegion((DataRegionId) regionId);
        } else {
          SchemaEngine.getInstance().deleteSchemaRegion((SchemaRegionId) regionId);
        }
      } catch (Throwable e) {
        taskLogger.error("{}, deleteRegion {} error", REGION_MIGRATE_PROCESS, regionId, e);
        status.setCode(TSStatusCode.DELETE_REGION_ERROR.getStatusCode());
        status.setMessage("deleteRegion " + regionId + " error, " + e.getMessage());
        return status;
      }
      status.setMessage("deleteRegion " + regionId + " succeed");
      taskLogger.info("{}, Succeed to deleteRegion {}", REGION_MIGRATE_PROCESS, regionId);
      return status;
    }
  }

  private static class Holder {

    private static final RegionMigrateService INSTANCE = new RegionMigrateService();

    private Holder() {}
  }

  private static void reportSucceed(TConsensusGroupId tRegionId, String migrateState) {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    status.setMessage(
        String.format("Region: %s, state: %s, executed succeed", tRegionId, migrateState));
    TRegionMigrateResultReportReq req = new TRegionMigrateResultReportReq(tRegionId, status);
    try {
      reportRegionMigrateResultToConfigNode(req);
    } catch (Throwable e) {
      LOGGER.error(
          "{}, Report region {} migrate result error in reportSucceed, result: {}",
          REGION_MIGRATE_PROCESS,
          tRegionId,
          req,
          e);
    }
  }

  private static void reportFailed(
      TConsensusGroupId tRegionId,
      TDataNodeLocation failedNode,
      TRegionMigrateFailedType failedType,
      TSStatus status) {
    Map<TDataNodeLocation, TRegionMigrateFailedType> failedNodeAndReason = new HashMap<>();
    failedNodeAndReason.put(failedNode, failedType);
    TRegionMigrateResultReportReq req = new TRegionMigrateResultReportReq(tRegionId, status);
    req.setFailedNodeAndReason(failedNodeAndReason);
    try {
      reportRegionMigrateResultToConfigNode(req);
    } catch (Throwable e) {
      LOGGER.error(
          "{}, Report region {} migrate error in reportFailed, result:{}",
          REGION_MIGRATE_PROCESS,
          tRegionId,
          req,
          e);
    }
  }

  private static void reportRegionMigrateResultToConfigNode(TRegionMigrateResultReportReq req)
      throws TException, ClientManagerException {
    TSStatus status;
    try (ConfigNodeClient client =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      status = client.reportRegionMigrateResult(req);
      LOGGER.info(
          "{}, Report region {} migrate result {} to Config node succeed, result: {}",
          REGION_MIGRATE_PROCESS,
          req.getRegionId(),
          req,
          status);
    }
  }

  private static boolean isSucceed(TSStatus status) {
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  private static boolean isFailed(TSStatus status) {
    return !isSucceed(status);
  }

  private static TEndPoint getConsensusEndPoint(
      TDataNodeLocation nodeLocation, ConsensusGroupId regionId) {
    if (regionId instanceof DataRegionId) {
      return nodeLocation.getDataRegionConsensusEndPoint();
    }
    return nodeLocation.getSchemaRegionConsensusEndPoint();
  }
}
