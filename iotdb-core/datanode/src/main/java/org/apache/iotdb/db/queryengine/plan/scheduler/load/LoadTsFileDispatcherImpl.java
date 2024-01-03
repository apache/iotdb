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

package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.exception.mpp.FragmentInstanceDispatchException;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileCommandNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.queryengine.plan.scheduler.FragInstanceDispatchResult;
import org.apache.iotdb.db.queryengine.plan.scheduler.IFragInstanceDispatcher;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.mpp.rpc.thrift.TLoadCommandReq;
import org.apache.iotdb.mpp.rpc.thrift.TLoadResp;
import org.apache.iotdb.mpp.rpc.thrift.TTsFilePieceReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import io.airlift.concurrent.SetThreadName;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class LoadTsFileDispatcherImpl implements IFragInstanceDispatcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileDispatcherImpl.class);

  private String uuid;
  private String tsFileName;
  private final String localhostIpAddr;
  private final int localhostInternalPort;
  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      internalServiceClientManager;
  private final ExecutorService executor;
  private final boolean isGeneratedByPipe;

  private static final String NODE_CONNECTION_ERROR = "can't connect to node {}";

  public LoadTsFileDispatcherImpl(
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> internalServiceClientManager,
      boolean isGeneratedByPipe) {
    this.internalServiceClientManager = internalServiceClientManager;
    this.localhostIpAddr = IoTDBDescriptor.getInstance().getConfig().getInternalAddress();
    this.localhostInternalPort = IoTDBDescriptor.getInstance().getConfig().getInternalPort();
    this.executor =
        IoTDBThreadPoolFactory.newCachedThreadPool(LoadTsFileDispatcherImpl.class.getName());
    this.isGeneratedByPipe = isGeneratedByPipe;
  }

  public void setUuidAndTsFileName(String uuid, String tsFileName) {
    this.uuid = uuid;
    this.tsFileName = tsFileName;
  }

  @Override
  public Future<FragInstanceDispatchResult> dispatch(List<FragmentInstance> instances) {
    return executor.submit(
        () -> {
          try (SetThreadName threadName =
              new SetThreadName(
                  LoadTsFileScheduler.class.getSimpleName() + "-TsFilePiece-" + tsFileName)) {
            for (FragmentInstance instance : instances) {
              dispatchOneInstance(instance);
            }
          } catch (FragmentInstanceDispatchException e) {
            return new FragInstanceDispatchResult(e.getFailureStatus());
          } catch (Exception t) {
            LOGGER.warn("cannot dispatch FI for load operation", t);
            return new FragInstanceDispatchResult(
                RpcUtils.getStatus(
                    TSStatusCode.INTERNAL_SERVER_ERROR, "Unexpected errors: " + t.getMessage()));
          }
          return new FragInstanceDispatchResult(true);
        });
  }

  private void dispatchOneInstance(FragmentInstance instance)
      throws FragmentInstanceDispatchException {
    TTsFilePieceReq loadTsFileReq = null;

    for (TDataNodeLocation dataNodeLocation :
        instance.getRegionReplicaSet().getDataNodeLocations()) {
      TEndPoint endPoint = dataNodeLocation.getInternalEndPoint();
      if (isDispatchedToLocal(endPoint)) {
        dispatchLocally(instance);
      } else {
        if (loadTsFileReq == null) {
          loadTsFileReq =
              new TTsFilePieceReq(
                  instance.getFragment().getPlanNodeTree().serializeToByteBuffer(),
                  uuid,
                  instance.getRegionReplicaSet().getRegionId());
        }
        dispatchRemote(loadTsFileReq, endPoint);
      }
    }
  }

  private boolean isDispatchedToLocal(TEndPoint endPoint) {
    return this.localhostIpAddr.equals(endPoint.getIp()) && localhostInternalPort == endPoint.port;
  }

  private void dispatchRemote(TTsFilePieceReq loadTsFileReq, TEndPoint endPoint)
      throws FragmentInstanceDispatchException {
    try (SyncDataNodeInternalServiceClient client =
        internalServiceClientManager.borrowClient(endPoint)) {
      TLoadResp loadResp = client.sendTsFilePieceNode(loadTsFileReq);
      if (!loadResp.isAccepted()) {
        LOGGER.warn(loadResp.message);
        throw new FragmentInstanceDispatchException(loadResp.status);
      }
    } catch (ClientManagerException | TException e) {
      String warning = NODE_CONNECTION_ERROR;
      LOGGER.warn(warning, endPoint, e);
      TSStatus status = new TSStatus();
      status.setCode(TSStatusCode.DISPATCH_ERROR.getStatusCode());
      status.setMessage(warning + endPoint);
      throw new FragmentInstanceDispatchException(status);
    }
  }

  public void dispatchLocally(FragmentInstance instance) throws FragmentInstanceDispatchException {
    LOGGER.info("Receive load node from uuid {}.", uuid);

    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(
            instance.getRegionReplicaSet().getRegionId());
    PlanNode planNode = instance.getFragment().getPlanNodeTree();

    if (planNode instanceof LoadTsFilePieceNode) { // split
      LoadTsFilePieceNode pieceNode =
          (LoadTsFilePieceNode) PlanNodeType.deserialize(planNode.serializeToByteBuffer());
      if (pieceNode == null) {
        throw new FragmentInstanceDispatchException(
            new TSStatus(TSStatusCode.DESERIALIZE_PIECE_OF_TSFILE_ERROR.getStatusCode()));
      }
      TSStatus resultStatus =
          StorageEngine.getInstance().writeLoadTsFileNode((DataRegionId) groupId, pieceNode, uuid);

      if (!RpcUtils.SUCCESS_STATUS.equals(resultStatus)) {
        throw new FragmentInstanceDispatchException(resultStatus);
      }
    } else if (planNode instanceof LoadSingleTsFileNode) { // do not need to split
      try {
        StorageEngine.getInstance()
            .getDataRegion((DataRegionId) groupId)
            .loadNewTsFile(
                ((LoadSingleTsFileNode) planNode).getTsFileResource(),
                ((LoadSingleTsFileNode) planNode).isDeleteAfterLoad(),
                isGeneratedByPipe);
      } catch (LoadFileException e) {
        LOGGER.warn("Load TsFile Node {} error.", planNode, e);
        TSStatus resultStatus = new TSStatus();
        resultStatus.setCode(TSStatusCode.LOAD_FILE_ERROR.getStatusCode());
        resultStatus.setMessage(e.getMessage());
        throw new FragmentInstanceDispatchException(resultStatus);
      }
    }
  }

  public Future<FragInstanceDispatchResult> dispatchCommand(List<FragmentInstance> instances) {
    final Map<TEndPoint, TLoadCommandReq> endPoint2LoadCommandReq = new HashMap<>();
    instances.forEach(
        instance ->
            endPoint2LoadCommandReq
                .computeIfAbsent(
                    instance.getHostDataNode().getInternalEndPoint(),
                    o ->
                        ((LoadTsFileCommandNode) instance.getFragment().getPlanNodeTree())
                            .toTLoadCommandReq())
                .addToConsensusGroupIds(instance.getRegionReplicaSet().getRegionId()));

    return executor.submit(
        () -> {
          try (SetThreadName threadName =
              new SetThreadName(
                  LoadTsFileScheduler.class.getSimpleName() + "-Command-" + tsFileName)) {
            for (Map.Entry<TEndPoint, TLoadCommandReq> entry : endPoint2LoadCommandReq.entrySet()) {
              FragInstanceDispatchResult result =
                  dispatchCommandRemote(entry.getKey(), entry.getValue());
              if (!result.isSuccessful()) {
                return result;
              }
            }
          } catch (Exception t) {
            LOGGER.warn("cannot dispatch FI for load operation", t);
            return new FragInstanceDispatchResult(
                RpcUtils.getStatus(
                    TSStatusCode.INTERNAL_SERVER_ERROR, "Unexpected errors: " + t.getMessage()));
          }
          return new FragInstanceDispatchResult(true);
        });
  }

  private FragInstanceDispatchResult dispatchCommandRemote(
      TEndPoint endPoint, TLoadCommandReq commandReq) {
    try (SyncDataNodeInternalServiceClient client =
        internalServiceClientManager.borrowClient(endPoint)) {
      TLoadResp loadResp = client.sendLoadCommand(commandReq);
      if (loadResp.accepted) {
        return new FragInstanceDispatchResult(true);
      }

      LOGGER.warn(
          "Dispatch command {} to {} failed, dispatch result status {}, dispatch result message {}",
          commandReq,
          endPoint,
          loadResp.status,
          loadResp.message);
      return new FragInstanceDispatchResult(
          loadResp.status.setMessage(loadResp.status.message + loadResp.message));
    } catch (ClientManagerException | TException e) {
      LOGGER.warn(NODE_CONNECTION_ERROR, endPoint, e);
      TSStatus status = new TSStatus();
      status.setCode(TSStatusCode.DISPATCH_ERROR.getStatusCode());
      status.setMessage(
          "can't connect to node {}, please reset longer dn_connection_timeout_ms "
              + "in iotdb-common.properties and restart iotdb."
              + endPoint);
      return new FragInstanceDispatchResult(status);
    } catch (Exception t) {
      LOGGER.warn("cannot dispatch FI for load operation", t);
      return new FragInstanceDispatchResult(
          RpcUtils.getStatus(
              TSStatusCode.INTERNAL_SERVER_ERROR, "Unexpected errors: " + t.getMessage()));
    }
  }

  @Override
  public void abort() {
    // Do nothing
  }
}
