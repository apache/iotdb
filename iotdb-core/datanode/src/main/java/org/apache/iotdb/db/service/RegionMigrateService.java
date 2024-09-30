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
import org.apache.iotdb.common.rpc.thrift.TRegionMaintainTaskStatus;
import org.apache.iotdb.common.rpc.thrift.TRegionMigrateFailedType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.consensus.exception.PeerAlreadyInConsensusGroupException;
import org.apache.iotdb.consensus.exception.PeerNotInConsensusGroupException;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.protocol.thrift.impl.DataNodeRegionManager;
import org.apache.iotdb.mpp.rpc.thrift.TMaintainPeerReq;
import org.apache.iotdb.mpp.rpc.thrift.TRegionMigrateResult;
import org.apache.iotdb.mpp.rpc.thrift.TResetPeerListReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class RegionMigrateService implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(RegionMigrateService.class);

  public static final String REGION_MIGRATE_PROCESS = "[REGION_MIGRATE_PROCESS]";

  private static final int MAX_RETRY_NUM = 5;

  private static final int SLEEP_MILLIS = 5000;

  private ExecutorService regionMigratePool;

  // Map<taskId, taskStatus>
  // TODO: Due to the use of procedureId as taskId, it is currently unable to handle the situation
  // where different asynchronous tasks are submitted to the same datanode within a single procedure
  private static final ConcurrentHashMap<Long, TRegionMigrateResult> taskResultMap =
      new ConcurrentHashMap<>();
  private static final TRegionMigrateResult unfinishedResult = new TRegionMigrateResult();

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
      if (!addToTaskResultMap(req.getTaskId())) {
        LOGGER.warn(
            "{} The AddRegionPeerTask {} has already been submitted and will not be submitted again.",
            REGION_MIGRATE_PROCESS,
            req.getTaskId());
        return true;
      }
      regionMigratePool.submit(
          new AddRegionPeerTask(req.getTaskId(), req.getRegionId(), req.getDestNode()));
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
      if (!addToTaskResultMap(req.getTaskId())) {
        LOGGER.warn(
            "{} The RemoveRegionPeer {} has already been submitted and will not be submitted again.",
            REGION_MIGRATE_PROCESS,
            req.getTaskId());
        return true;
      }
      regionMigratePool.submit(
          new RemoveRegionPeerTask(req.getTaskId(), req.getRegionId(), req.getDestNode()));
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
      if (!addToTaskResultMap(req.getTaskId())) {
        LOGGER.warn(
            "{} The DeleteOldRegionPeerTask {} has already been submitted and will not be submitted again.",
            REGION_MIGRATE_PROCESS,
            req.getTaskId());
        return true;
      }
      regionMigratePool.submit(
          new DeleteOldRegionPeerTask(req.getTaskId(), req.getRegionId(), req.getDestNode()));
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

  public synchronized TSStatus resetPeerList(TResetPeerListReq req) {
    List<Peer> correctPeers =
        req.getCorrectLocations().stream()
            .map(location -> Peer.valueOf(req.getRegionId(), location))
            .collect(Collectors.toList());
    ConsensusGroupId regionId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getRegionId());
    try {
      if (regionId instanceof DataRegionId) {
        DataRegionConsensusImpl.getInstance().resetPeerList(regionId, correctPeers);
      } else {
        SchemaRegionConsensusImpl.getInstance().resetPeerList(regionId, correctPeers);
      }
    } catch (ConsensusGroupNotExistException e) {
      LOGGER.warn(
          "Reset peer list fail, this DataNode not contains peer of consensus group {}. Maybe caused by create local peer failure.",
          regionId,
          e);
    } catch (ConsensusException e) {
      LOGGER.error("reset peer list fail", e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  private boolean addToTaskResultMap(long taskId) {
    return taskResultMap.putIfAbsent(taskId, unfinishedResult) == null;
  }

  @Override
  public void start() throws StartupException {
    regionMigratePool =
        IoTDBThreadPoolFactory.newCachedThreadPool(ThreadName.REGION_MIGRATE.getName());
    LOGGER.info("Region migrate service start");
  }

  @Override
  public void stop() {
    if (regionMigratePool != null) {
      regionMigratePool.shutdown();
    }
    LOGGER.info("Region migrate service stop");
  }

  @Override
  public ServiceType getID() {
    return ServiceType.DATA_NODE_REGION_MIGRATE_SERVICE;
  }

  private static class AddRegionPeerTask implements Runnable {

    private static final Logger taskLogger = LoggerFactory.getLogger(AddRegionPeerTask.class);

    private final long taskId;

    // The RegionGroup that shall perform the add peer process
    private final TConsensusGroupId tRegionId;

    // The new DataNode to be added in the RegionGroup
    private final TDataNodeLocation destDataNode;

    public AddRegionPeerTask(
        long taskId, TConsensusGroupId tRegionId, TDataNodeLocation destDataNode) {
      this.taskId = taskId;
      this.tRegionId = tRegionId;
      this.destDataNode = destDataNode;
    }

    @Override
    public void run() {
      TSStatus runResult = addPeer();
      if (isFailed(runResult)) {
        taskFail(
            taskId, tRegionId, destDataNode, TRegionMigrateFailedType.AddPeerFailed, runResult);
        return;
      }

      taskSucceed(taskId, tRegionId, "AddPeer");
    }

    private TSStatus addPeer() {
      taskLogger.info(
          "{}, Start to addPeer {} for region {}", REGION_MIGRATE_PROCESS, destDataNode, tRegionId);
      ConsensusGroupId regionId = ConsensusGroupId.Factory.createFromTConsensusGroupId(tRegionId);
      TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      TEndPoint destEndpoint = getConsensusEndPoint(destDataNode, regionId);
      boolean addPeerSucceed = true;
      Throwable throwable = null;
      for (int i = 0; i < MAX_RETRY_NUM; i++) {
        try {
          if (!addPeerSucceed) {
            Thread.sleep(SLEEP_MILLIS);
          }
          addRegionPeer(regionId, new Peer(regionId, destDataNode.getDataNodeId(), destEndpoint));
          addPeerSucceed = true;
        } catch (PeerAlreadyInConsensusGroupException e) {
          addPeerSucceed = true;
        } catch (InterruptedException e) {
          throwable = e;
          Thread.currentThread().interrupt();
        } catch (ConsensusException e) {
          addPeerSucceed = false;
          throwable = e;
          taskLogger.error(
              "{}, executed addPeer {} for region {} error, retry times: {}",
              REGION_MIGRATE_PROCESS,
              destEndpoint,
              regionId,
              i,
              e);
        } catch (Exception e) {
          addPeerSucceed = false;
          throwable = e;
          taskLogger.warn("Unexpected exception", e);
        }
        if (addPeerSucceed || throwable instanceof InterruptedException) {
          break;
        }
      }

      if (!addPeerSucceed) {
        String errorMsg =
            String.format(
                "%s, AddPeer for region error after max retry times, peerId: %s, regionId: %s",
                REGION_MIGRATE_PROCESS, destEndpoint, regionId);
        taskLogger.error(errorMsg, throwable);
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

    private void addRegionPeer(ConsensusGroupId regionId, Peer newPeer) throws ConsensusException {
      if (regionId instanceof DataRegionId) {
        DataRegionConsensusImpl.getInstance().addRemotePeer(regionId, newPeer);
      } else {
        SchemaRegionConsensusImpl.getInstance().addRemotePeer(regionId, newPeer);
      }
    }
  }

  private static class RemoveRegionPeerTask implements Runnable {

    private static final Logger taskLogger = LoggerFactory.getLogger(RemoveRegionPeerTask.class);

    private final long taskId;

    private final TConsensusGroupId tRegionId;

    private final TDataNodeLocation destDataNode;

    public RemoveRegionPeerTask(
        long taskId, TConsensusGroupId tRegionId, TDataNodeLocation destDataNode) {
      this.taskId = taskId;
      this.tRegionId = tRegionId;
      this.destDataNode = destDataNode;
    }

    @Override
    public void run() {
      TSStatus runResult = removePeer();
      if (isSucceed(runResult)) {
        taskSucceed(taskId, tRegionId, "RemovePeer");
      } else {
        taskFail(
            taskId, tRegionId, destDataNode, TRegionMigrateFailedType.RemovePeerFailed, runResult);
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
      Throwable throwable = null;
      boolean removePeerSucceed = true;
      for (int i = 0; i < MAX_RETRY_NUM; i++) {
        try {
          if (!removePeerSucceed) {
            Thread.sleep(SLEEP_MILLIS);
          }
          removeRegionPeer(
              regionId, new Peer(regionId, destDataNode.getDataNodeId(), destEndPoint));
          removePeerSucceed = true;
        } catch (PeerNotInConsensusGroupException e) {
          removePeerSucceed = true;
        } catch (InterruptedException e) {
          throwable = e;
          Thread.currentThread().interrupt();
        } catch (ConsensusException e) {
          removePeerSucceed = false;
          throwable = e;
          taskLogger.error(
              "{}, executed removePeer {} for region {} error, retry times: {}",
              REGION_MIGRATE_PROCESS,
              destEndPoint,
              regionId,
              i,
              e);
        } catch (Exception e) {
          removePeerSucceed = false;
          throwable = e;
          taskLogger.warn("Unexpected exception", e);
        }
        if (removePeerSucceed || throwable instanceof InterruptedException) {
          break;
        }
      }

      if (!removePeerSucceed) {
        String errorMsg =
            String.format(
                "%s, RemovePeer for region error after max retry times, peerId: %s, regionId: %s",
                REGION_MIGRATE_PROCESS, destEndPoint, regionId);
        taskLogger.error(errorMsg, throwable);
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

    private void removeRegionPeer(ConsensusGroupId regionId, Peer oldPeer)
        throws ConsensusException {
      if (regionId instanceof DataRegionId) {
        DataRegionConsensusImpl.getInstance().removeRemotePeer(regionId, oldPeer);
      } else {
        SchemaRegionConsensusImpl.getInstance().removeRemotePeer(regionId, oldPeer);
      }
    }
  }

  private static class DeleteOldRegionPeerTask implements Runnable {

    private static final Logger taskLogger = LoggerFactory.getLogger(DeleteOldRegionPeerTask.class);
    private final long taskId;

    private final TConsensusGroupId tRegionId;

    private final TDataNodeLocation originalDataNode;

    public DeleteOldRegionPeerTask(
        long taskId, TConsensusGroupId tRegionId, TDataNodeLocation originalDataNode) {
      this.taskId = taskId;
      this.tRegionId = tRegionId;
      this.originalDataNode = originalDataNode;
    }

    @Override
    public void run() {
      // deletePeer: remove the peer from the consensus group
      TSStatus runResult = deletePeer();
      if (isFailed(runResult)) {
        taskFail(
            taskId,
            tRegionId,
            originalDataNode,
            TRegionMigrateFailedType.RemoveConsensusGroupFailed,
            runResult);
      }

      // deleteRegion: delete region data
      runResult = deleteRegion();

      if (isFailed(runResult)) {
        taskFail(
            taskId,
            tRegionId,
            originalDataNode,
            TRegionMigrateFailedType.DeleteRegionFailed,
            runResult);
      }

      taskSucceed(taskId, tRegionId, "DeletePeer");
    }

    private TSStatus deletePeer() {
      taskLogger.info(
          "{}, Start to deletePeer {} for region {}",
          REGION_MIGRATE_PROCESS,
          originalDataNode,
          tRegionId);
      ConsensusGroupId regionId = ConsensusGroupId.Factory.createFromTConsensusGroupId(tRegionId);
      TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      try {
        if (regionId instanceof DataRegionId) {
          DataRegionConsensusImpl.getInstance().deleteLocalPeer(regionId);
        } else {
          SchemaRegionConsensusImpl.getInstance().deleteLocalPeer(regionId);
        }
      } catch (ConsensusException e) {
        String errorMsg =
            String.format(
                "deletePeer error, regionId: %s, errorMessage: %s", regionId, e.getMessage());
        taskLogger.error(errorMsg);
        status.setCode(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
        status.setMessage(errorMsg);
        return status;
      } catch (Exception e) {
        taskLogger.error("{}, deletePeer error, regionId: {}", REGION_MIGRATE_PROCESS, regionId, e);
        status.setCode(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
        status.setMessage(
            "deletePeer for region: " + regionId + " error. exception: " + e.getMessage());
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
          DataNodeRegionManager.getInstance().deleteDataRegion((DataRegionId) regionId);
        } else {
          DataNodeRegionManager.getInstance().deleteSchemaRegion((SchemaRegionId) regionId);
        }
      } catch (Exception e) {
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

  private static void taskSucceed(long taskId, TConsensusGroupId tRegionId, String migrateState) {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    status.setMessage(
        String.format("Region: %s, state: %s, executed succeed", tRegionId, migrateState));
    TRegionMigrateResult req = new TRegionMigrateResult(TRegionMaintainTaskStatus.SUCCESS);
    req.setRegionId(tRegionId).setMigrateResult(status);
    taskResultMap.put(taskId, req);
  }

  private static void taskFail(
      long taskId,
      TConsensusGroupId tRegionId,
      TDataNodeLocation failedNode,
      TRegionMigrateFailedType failedType,
      TSStatus status) {
    Map<TDataNodeLocation, TRegionMigrateFailedType> failedNodeAndReason = new HashMap<>();
    failedNodeAndReason.put(failedNode, failedType);
    TRegionMigrateResult req = new TRegionMigrateResult(TRegionMaintainTaskStatus.FAIL);
    req.setRegionId(tRegionId).setMigrateResult(status);
    req.setFailedNodeAndReason(failedNodeAndReason);
    taskResultMap.put(taskId, req);
  }

  public TRegionMigrateResult getRegionMaintainResult(Long taskId) {
    TRegionMigrateResult result = new TRegionMigrateResult();
    if (!taskResultMap.containsKey(taskId)) {
      result.setTaskStatus(TRegionMaintainTaskStatus.TASK_NOT_EXIST);
    } else if (taskResultMap.get(taskId) == unfinishedResult) {
      result.setTaskStatus(TRegionMaintainTaskStatus.PROCESSING);
    } else {
      result = taskResultMap.get(taskId);
    }
    return result;
  }

  public static boolean isSucceed(TSStatus status) {
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  public static boolean isFailed(TSStatus status) {
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
