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
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.engine.StorageEngineV2;
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

  private static final int RETRY = 5;

  private static final int SLEEP_MILLIS = 5000;

  private RegionMigratePool regionMigratePool;

  private RegionMigrateService() {}

  public static RegionMigrateService getInstance() {
    return Holder.INSTANCE;
  }

  /**
   * submit AddRegionPeerTask
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
          "Submit addRegionPeer task error for Region: {} on DataNode: {}.",
          req.getRegionId(),
          req.getDestNode().getInternalEndPoint().getIp(),
          e);
      submitSucceed = false;
    }
    return submitSucceed;
  }

  /**
   * submit RemoveRegionPeerTask
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
          "Submit removeRegionPeer task error for Region: {} on DataNode: {}.",
          req.getRegionId(),
          req.getDestNode().getInternalEndPoint().getIp(),
          e);
      submitSucceed = false;
    }
    return submitSucceed;
  }

  /**
   * remove a region peer
   *
   * @param req TMigrateRegionReq
   * @return submit task succeed?
   */
  public synchronized boolean submitDeleteOldRegionPeerTask(TMaintainPeerReq req) {

    boolean submitSucceed = true;
    try {
      regionMigratePool.submit(new DeleteOldRegionPeerTask(req.getRegionId(), req.getDestNode()));
    } catch (Exception e) {
      LOGGER.error(
          "Submit deleteOldRegionPeerTask error for Region: {} on DataNode: {}.",
          req.getRegionId(),
          req.getDestNode().getInternalEndPoint().getIp(),
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

  private static class Holder {
    private static final RegionMigrateService INSTANCE = new RegionMigrateService();

    private Holder() {}
  }

  private static class RegionMigratePool extends AbstractPoolManager {
    private final Logger poolLogger = LoggerFactory.getLogger(RegionMigratePool.class);

    private RegionMigratePool() {
      // migrate region one by one
      this.pool = IoTDBThreadPoolFactory.newSingleThreadExecutor("Region-Migrate-Pool");
    }

    @Override
    public Logger getLogger() {
      return poolLogger;
    }

    @Override
    public void start() {
      if (this.pool != null) {
        poolLogger.info("Data Node region migrate pool start");
      }
    }

    @Override
    public String getName() {
      return "migrate region";
    }
  }

  private static void reportSucceed(TConsensusGroupId tRegionId) {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    status.setMessage("Region: " + tRegionId + " migrated succeed");
    TRegionMigrateResultReportReq req = new TRegionMigrateResultReportReq(tRegionId, status);
    try {
      reportRegionMigrateResultToConfigNode(req);
    } catch (Throwable e) {
      LOGGER.error(
          "Report region {} migrate successful result error, result:{}", tRegionId, req, e);
    }
  }

  private static void reportFailed(
      TConsensusGroupId tRegionId,
      TDataNodeLocation failedNode,
      TRegionMigrateFailedType failedType,
      TSStatus status) {
    TRegionMigrateResultReportReq req =
        createFailedRequest(tRegionId, failedNode, failedType, status);
    try {
      reportRegionMigrateResultToConfigNode(req);
    } catch (Throwable e) {
      LOGGER.error("Report region {} migrate failed result error, result:{}", tRegionId, req, e);
    }
  }

  private static TRegionMigrateResultReportReq createFailedRequest(
      TConsensusGroupId tRegionId,
      TDataNodeLocation failedNode,
      TRegionMigrateFailedType failedType,
      TSStatus status) {
    Map<TDataNodeLocation, TRegionMigrateFailedType> failedNodeAndReason = new HashMap<>();
    failedNodeAndReason.put(failedNode, failedType);
    TRegionMigrateResultReportReq req = new TRegionMigrateResultReportReq(tRegionId, status);
    req.setFailedNodeAndReason(failedNodeAndReason);
    return req;
  }

  private static void reportRegionMigrateResultToConfigNode(TRegionMigrateResultReportReq req)
      throws TException {
    TSStatus status;
    try (ConfigNodeClient client = new ConfigNodeClient()) {
      status = client.reportRegionMigrateResult(req);
      LOGGER.info(
          "Report region {} migrate result {} to Config node succeed, result: {}",
          req.getRegionId(),
          req,
          status);
    }
  }

  private static class AddRegionPeerTask implements Runnable {
    private static final Logger taskLogger = LoggerFactory.getLogger(AddRegionPeerTask.class);

    // The RegionGroup that shall perform the add peer process
    private final TConsensusGroupId tRegionId;

    // The DataNode that selected to perform the add peer process
    private final TDataNodeLocation selectedDataNode;

    public AddRegionPeerTask(TConsensusGroupId tRegionId, TDataNodeLocation selectedDataNode) {
      this.tRegionId = tRegionId;
      this.selectedDataNode = selectedDataNode;
    }

    @Override
    public void run() {
      TSStatus runResult = addPeer();
      if (isFailed(runResult)) {
        reportFailed(
            tRegionId, selectedDataNode, TRegionMigrateFailedType.AddPeerFailed, runResult);
        return;
      }

      reportSucceed(tRegionId);
    }

    private TSStatus addPeer() {
      ConsensusGroupId regionId = ConsensusGroupId.Factory.createFromTConsensusGroupId(tRegionId);
      TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      ConsensusGenericResponse resp = null;
      TEndPoint newPeerNode = getConsensusEndPoint(selectedDataNode, regionId);
      taskLogger.info("Start to add peer {} for region {}", newPeerNode, tRegionId);
      boolean addPeerSucceed = true;
      for (int i = 0; i < RETRY; i++) {
        try {
          if (!addPeerSucceed) {
            Thread.sleep(SLEEP_MILLIS);
          }
          resp = addRegionPeer(regionId, new Peer(regionId, newPeerNode));
          addPeerSucceed = true;
        } catch (Throwable e) {
          addPeerSucceed = false;
          taskLogger.error(
              "Add new peer {} for region {} error, retry times: {}", newPeerNode, regionId, i, e);
          status.setCode(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
          status.setMessage(
              String.format(
                  "Add peer for region error, peerId: %s, regionId: %s, errorMessage: %s",
                  newPeerNode, regionId, e.getMessage()));
        }
        if (addPeerSucceed && resp != null && resp.isSuccess()) {
          break;
        }
      }
      if (!addPeerSucceed || resp == null || !resp.isSuccess()) {
        taskLogger.error(
            "Add new peer {} for region {} failed, resp: {}", newPeerNode, regionId, resp);
        status.setCode(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
        status.setMessage(
            String.format(
                "Add peer for region error, peerId: %s, regionId: %s, resp: %s",
                newPeerNode, regionId, resp));
        return status;
      }

      taskLogger.info("Succeed to add peer {} for region {}", newPeerNode, regionId);
      status.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      status.setMessage("add peer " + newPeerNode + " for region " + regionId + " succeed");
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

    private boolean isFailed(TSStatus status) {
      return !isSucceed(status);
    }
  }

  private static class RemoveRegionPeerTask implements Runnable {
    private static final Logger taskLogger = LoggerFactory.getLogger(RemoveRegionPeerTask.class);

    // The RegionGroup that shall perform the remove peer process
    private final TConsensusGroupId tRegionId;

    // The DataNode that selected to perform the remove peer process
    private final TDataNodeLocation selectedDataNode;

    public RemoveRegionPeerTask(TConsensusGroupId tRegionId, TDataNodeLocation selectedDataNode) {
      this.tRegionId = tRegionId;
      this.selectedDataNode = selectedDataNode;
    }

    @Override
    public void run() {
      TSStatus runResult = removePeer();
      if (isFailed(runResult)) {
        reportFailed(
            tRegionId, selectedDataNode, TRegionMigrateFailedType.RemovePeerFailed, runResult);
      }

      reportSucceed(tRegionId);
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

    private TSStatus removePeer() {
      ConsensusGroupId regionId = ConsensusGroupId.Factory.createFromTConsensusGroupId(tRegionId);
      TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      TEndPoint oldPeerNode = getConsensusEndPoint(selectedDataNode, regionId);
      taskLogger.info("Start to remove peer {} for region {}", oldPeerNode, regionId);
      ConsensusGenericResponse resp = null;
      boolean removePeerSucceed = true;
      for (int i = 0; i < RETRY; i++) {
        try {
          if (!removePeerSucceed) {
            Thread.sleep(SLEEP_MILLIS);
          }
          resp = removeRegionPeer(regionId, new Peer(regionId, oldPeerNode));
          removePeerSucceed = true;
        } catch (Throwable e) {
          removePeerSucceed = false;
          taskLogger.error(
              "remove peer {} for region {} error, retry times: {}", oldPeerNode, regionId, i, e);
          status.setCode(TSStatusCode.REGION_MIGRATE_FAILED.getStatusCode());
          status.setMessage(
              "remove peer: "
                  + oldPeerNode
                  + " for region: "
                  + regionId
                  + " error. exception: "
                  + e.getMessage());
        }
        if (removePeerSucceed && resp != null && resp.isSuccess()) {
          break;
        }
      }

      if (!removePeerSucceed || resp == null || !resp.isSuccess()) {
        taskLogger.error(
            "Remove old peer {} for region {} failed, resp: {}", oldPeerNode, regionId, resp);
        status.setCode(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
        status.setMessage("remove old peer " + oldPeerNode + " for region " + regionId + " failed");
        return status;
      }

      taskLogger.info("Succeed to remove peer {} for region {}", oldPeerNode, regionId);
      status.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      status.setMessage("Remove peer " + oldPeerNode + " for region " + regionId + " succeed");
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

    private boolean isFailed(TSStatus status) {
      return !isSucceed(status);
    }
  }

  private static class DeleteOldRegionPeerTask implements Runnable {
    private static final Logger taskLogger = LoggerFactory.getLogger(DeleteOldRegionPeerTask.class);

    // migrate which region
    private final TConsensusGroupId tRegionId;

    // migrate from which node
    private final TDataNodeLocation fromNode;

    public DeleteOldRegionPeerTask(TConsensusGroupId tRegionId, TDataNodeLocation fromNode) {
      this.tRegionId = tRegionId;
      this.fromNode = fromNode;
    }

    @Override
    public void run() {
      TSStatus runResult = deleteOldRegionPeer();
      if (isFailed(runResult)) {
        reportFailed(
            tRegionId, fromNode, TRegionMigrateFailedType.RemoveConsensusGroupFailed, runResult);
      }

      runResult = deleteRegion();
      if (isFailed(runResult)) {
        reportFailed(tRegionId, fromNode, TRegionMigrateFailedType.DeleteRegionFailed, runResult);
      }

      reportSucceed(tRegionId);
    }

    private TSStatus deleteOldRegionPeer() {
      ConsensusGroupId regionId = ConsensusGroupId.Factory.createFromTConsensusGroupId(tRegionId);
      taskLogger.info("Start to deleteOldRegionPeer: {}", regionId);
      TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      ConsensusGenericResponse resp;
      try {
        if (regionId instanceof DataRegionId) {
          resp = DataRegionConsensusImpl.getInstance().deletePeer(regionId);
        } else {
          resp = SchemaRegionConsensusImpl.getInstance().deletePeer(regionId);
        }
      } catch (Throwable e) {
        taskLogger.error("DeleteOldRegionPeer error, regionId: {}", regionId, e);
        status.setCode(TSStatusCode.REGION_MIGRATE_FAILED.getStatusCode());
        status.setMessage(
            "deleteOldRegionPeer for region: " + regionId + " error. exception: " + e.getMessage());
        return status;
      }
      if (!resp.isSuccess()) {
        taskLogger.error("deleteOldRegionPeer error, regionId: {}", regionId, resp.getException());
        status.setCode(TSStatusCode.REGION_MIGRATE_FAILED.getStatusCode());
        status.setMessage(
            String.format(
                "deleteOldRegionPeer error, regionId: %s, errorMessage: %s",
                regionId, resp.getException().getMessage()));
        return status;
      }
      taskLogger.info("succeed to remove region {} consensus group", regionId);
      status.setMessage("remove region consensus group " + regionId + "succeed");
      return status;
    }

    private TSStatus deleteRegion() {
      TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      ConsensusGroupId regionId = ConsensusGroupId.Factory.createFromTConsensusGroupId(tRegionId);
      taskLogger.debug("start to delete region {}", regionId);
      try {
        if (regionId instanceof DataRegionId) {
          StorageEngineV2.getInstance().deleteDataRegion((DataRegionId) regionId);
        } else {
          SchemaEngine.getInstance().deleteSchemaRegion((SchemaRegionId) regionId);
        }
      } catch (Throwable e) {
        taskLogger.error("delete the region {} failed", regionId, e);
        status.setCode(TSStatusCode.DELETE_REGION_ERROR.getStatusCode());
        status.setMessage("delete region " + regionId + "failed, " + e.getMessage());
        return status;
      }
      status.setMessage("delete region " + regionId + " succeed");
      taskLogger.info("Finished to delete region {}", regionId);
      return status;
    }

    private boolean isSucceed(TSStatus status) {
      return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    }

    private boolean isFailed(TSStatus status) {
      return !isSucceed(status);
    }
  }
}
