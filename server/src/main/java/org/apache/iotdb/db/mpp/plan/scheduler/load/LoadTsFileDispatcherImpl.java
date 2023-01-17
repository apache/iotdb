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

package org.apache.iotdb.db.mpp.plan.scheduler.load;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.exception.mpp.FragmentInstanceDispatchException;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.mpp.plan.scheduler.FragInstanceDispatchResult;
import org.apache.iotdb.db.mpp.plan.scheduler.IFragInstanceDispatcher;
import org.apache.iotdb.mpp.rpc.thrift.TLoadCommandReq;
import org.apache.iotdb.mpp.rpc.thrift.TLoadResp;
import org.apache.iotdb.mpp.rpc.thrift.TTsFilePieceReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import io.airlift.concurrent.SetThreadName;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.google.common.util.concurrent.Futures.immediateFuture;

public class LoadTsFileDispatcherImpl implements IFragInstanceDispatcher {
  private static final Logger logger = LoggerFactory.getLogger(LoadTsFileDispatcherImpl.class);

  private String uuid;
  private final String localhostIpAddr;
  private final int localhostInternalPort;
  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      internalServiceClientManager;
  private final ExecutorService executor;

  public LoadTsFileDispatcherImpl(
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> internalServiceClientManager) {
    this.internalServiceClientManager = internalServiceClientManager;
    this.localhostIpAddr = IoTDBDescriptor.getInstance().getConfig().getInternalAddress();
    this.localhostInternalPort = IoTDBDescriptor.getInstance().getConfig().getInternalPort();
    this.executor =
        IoTDBThreadPoolFactory.newCachedThreadPool(LoadTsFileDispatcherImpl.class.getName());
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  @Override
  public Future<FragInstanceDispatchResult> dispatch(List<FragmentInstance> instances) {
    return executor.submit(
        () -> {
          for (FragmentInstance instance : instances) {
            try (SetThreadName threadName =
                new SetThreadName(
                    LoadTsFileScheduler.class.getName() + instance.getId().getFullId())) {
              dispatchOneInstance(instance);
            } catch (FragmentInstanceDispatchException e) {
              return new FragInstanceDispatchResult(e.getFailureStatus());
            } catch (Throwable t) {
              logger.warn("cannot dispatch FI for load operation", t);
              return new FragInstanceDispatchResult(
                  RpcUtils.getStatus(
                      TSStatusCode.INTERNAL_SERVER_ERROR, "Unexpected errors: " + t.getMessage()));
            }
          }
          return new FragInstanceDispatchResult(true);
        });
  }

  private void dispatchOneInstance(FragmentInstance instance)
      throws FragmentInstanceDispatchException {
    for (TDataNodeLocation dataNodeLocation :
        instance.getRegionReplicaSet().getDataNodeLocations()) {
      TEndPoint endPoint = dataNodeLocation.getInternalEndPoint();
      if (isDispatchedToLocal(endPoint)) {
        dispatchLocally(instance);
      } else {
        dispatchRemote(instance, endPoint);
      }
    }
  }

  private boolean isDispatchedToLocal(TEndPoint endPoint) {
    return this.localhostIpAddr.equals(endPoint.getIp()) && localhostInternalPort == endPoint.port;
  }

  private void dispatchRemote(FragmentInstance instance, TEndPoint endPoint)
      throws FragmentInstanceDispatchException {
    try (SyncDataNodeInternalServiceClient client =
        internalServiceClientManager.borrowClient(endPoint)) {
      TTsFilePieceReq loadTsFileReq =
          new TTsFilePieceReq(
              instance.getFragment().getPlanNodeTree().serializeToByteBuffer(),
              uuid,
              instance.getRegionReplicaSet().getRegionId());
      TLoadResp loadResp = client.sendTsFilePieceNode(loadTsFileReq);
      if (!loadResp.isAccepted()) {
        logger.warn(loadResp.message);
        throw new FragmentInstanceDispatchException(loadResp.status);
      }
    } catch (ClientManagerException | TException e) {
      logger.warn("can't connect to node {}", endPoint, e);
      TSStatus status = new TSStatus();
      status.setCode(TSStatusCode.DISPATCH_ERROR.getStatusCode());
      status.setMessage("can't connect to node {}" + endPoint);
      throw new FragmentInstanceDispatchException(status);
    }
  }

  public void dispatchLocally(FragmentInstance instance) throws FragmentInstanceDispatchException {
    logger.info("Receive load node from uuid {}.", uuid);

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
    } else if (planNode instanceof LoadSingleTsFileNode) { // do not need split
      try {
        StorageEngine.getInstance()
            .getDataRegion((DataRegionId) groupId)
            .loadNewTsFile(
                ((LoadSingleTsFileNode) planNode).getTsFileResource(),
                ((LoadSingleTsFileNode) planNode).isDeleteAfterLoad());
      } catch (LoadFileException e) {
        logger.warn(String.format("Load TsFile Node %s error.", planNode), e);
        TSStatus resultStatus = new TSStatus();
        resultStatus.setCode(TSStatusCode.LOAD_FILE_ERROR.getStatusCode());
        resultStatus.setMessage(e.getMessage());
        throw new FragmentInstanceDispatchException(resultStatus);
      }
    }
  }

  public Future<FragInstanceDispatchResult> dispatchCommand(
      TLoadCommandReq loadCommandReq, Set<TRegionReplicaSet> replicaSets) {
    Set<TEndPoint> allEndPoint = new HashSet<>();
    for (TRegionReplicaSet replicaSet : replicaSets) {
      for (TDataNodeLocation dataNodeLocation : replicaSet.getDataNodeLocations()) {
        allEndPoint.add(dataNodeLocation.getInternalEndPoint());
      }
    }

    for (TEndPoint endPoint : allEndPoint) {
      try (SetThreadName threadName =
          new SetThreadName(
              LoadTsFileScheduler.class.getName() + "-" + loadCommandReq.commandType)) {
        if (isDispatchedToLocal(endPoint)) {
          dispatchLocally(loadCommandReq);
        } else {
          dispatchRemote(loadCommandReq, endPoint);
        }
      } catch (FragmentInstanceDispatchException e) {
        return immediateFuture(new FragInstanceDispatchResult(e.getFailureStatus()));
      } catch (Throwable t) {
        logger.warn("cannot dispatch LoadCommand for load operation", t);
        return immediateFuture(
            new FragInstanceDispatchResult(
                RpcUtils.getStatus(
                    TSStatusCode.INTERNAL_SERVER_ERROR, "Unexpected errors: " + t.getMessage())));
      }
    }
    return immediateFuture(new FragInstanceDispatchResult(true));
  }

  private void dispatchRemote(TLoadCommandReq loadCommandReq, TEndPoint endPoint)
      throws FragmentInstanceDispatchException {
    try (SyncDataNodeInternalServiceClient client =
        internalServiceClientManager.borrowClient(endPoint)) {
      TLoadResp loadResp = client.sendLoadCommand(loadCommandReq);
      if (!loadResp.isAccepted()) {
        logger.warn(loadResp.message);
        throw new FragmentInstanceDispatchException(loadResp.status);
      }
    } catch (ClientManagerException | TException e) {
      logger.warn("can't connect to node {}", endPoint, e);
      TSStatus status = new TSStatus();
      status.setCode(TSStatusCode.DISPATCH_ERROR.getStatusCode());
      status.setMessage(
          "can't connect to node {}, please reset longer dn_connection_timeout_ms in iotdb-common.properties and restart iotdb."
              + endPoint);
      throw new FragmentInstanceDispatchException(status);
    }
  }

  private void dispatchLocally(TLoadCommandReq loadCommandReq)
      throws FragmentInstanceDispatchException {
    TSStatus resultStatus =
        StorageEngine.getInstance()
            .executeLoadCommand(
                LoadTsFileScheduler.LoadCommand.values()[loadCommandReq.commandType],
                loadCommandReq.uuid);
    if (!RpcUtils.SUCCESS_STATUS.equals(resultStatus)) {
      throw new FragmentInstanceDispatchException(resultStatus);
    }
  }

  @Override
  public void abort() {}
}
