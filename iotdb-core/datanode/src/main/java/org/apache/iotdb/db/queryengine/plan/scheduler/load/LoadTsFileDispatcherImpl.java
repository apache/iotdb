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
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.ProgressIndexType;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.exception.mpp.FragmentInstanceDispatchException;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.SubPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.queryengine.plan.scheduler.FragInstanceDispatchResult;
import org.apache.iotdb.db.queryengine.plan.scheduler.IFragInstanceDispatcher;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.mpp.rpc.thrift.TLoadCommandReq;
import org.apache.iotdb.mpp.rpc.thrift.TLoadResp;
import org.apache.iotdb.mpp.rpc.thrift.TTsFilePieceReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.util.concurrent.Futures.immediateFuture;

public class LoadTsFileDispatcherImpl implements IFragInstanceDispatcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileDispatcherImpl.class);

  private static final int MAX_CONNECTION_TIMEOUT_MS = 24 * 60 * 60 * 1000; // 1 day
  private static final int FIRST_ADJUSTMENT_TIMEOUT_MS = 6 * 60 * 60 * 1000; // 6 hours
  private static final AtomicInteger CONNECTION_TIMEOUT_MS =
      new AtomicInteger(IoTDBDescriptor.getInstance().getConfig().getConnectionTimeoutInMS());

  private String uuid;
  private final String localhostIpAddr;
  private final int localhostInternalPort;
  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      internalServiceClientManager;
  private final ExecutorService executor;
  private final boolean isGeneratedByPipe;

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

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  @Override
  public Future<FragInstanceDispatchResult> dispatch(
      SubPlan root, List<FragmentInstance> instances) {
    return executor.submit(
        () -> {
          for (FragmentInstance instance : instances) {
            try (SetThreadName threadName =
                new SetThreadName(
                    "load-dispatcher" + "-" + instance.getId().getFullId() + "-" + uuid)) {
              dispatchOneInstance(instance);
            } catch (FragmentInstanceDispatchException e) {
              return new FragInstanceDispatchResult(e.getFailureStatus());
            } catch (Exception t) {
              LOGGER.warn("cannot dispatch FI for load operation", t);
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
      final TsFileResource tsFileResource = ((LoadSingleTsFileNode) planNode).getTsFileResource();
      final String filePath = tsFileResource.getTsFile().getAbsolutePath();
      try {
        PipeDataNodeAgent.runtime().assignProgressIndexForTsFileLoad(tsFileResource);
        tsFileResource.setGeneratedByPipe(isGeneratedByPipe);
        tsFileResource.serialize();
        TsFileResource cloneTsFileResource = null;
        try {
          cloneTsFileResource = tsFileResource.shallowCloneForNative();
        } catch (CloneNotSupportedException e) {
          cloneTsFileResource = tsFileResource.shallowClone();
        }

        StorageEngine.getInstance()
            .getDataRegion((DataRegionId) groupId)
            .loadNewTsFile(
                cloneTsFileResource,
                ((LoadSingleTsFileNode) planNode).isDeleteAfterLoad(),
                isGeneratedByPipe,
                false);
      } catch (LoadFileException e) {
        LOGGER.warn("Load TsFile Node {} error.", planNode, e);
        TSStatus resultStatus = new TSStatus();
        resultStatus.setCode(TSStatusCode.LOAD_FILE_ERROR.getStatusCode());
        resultStatus.setMessage(e.getMessage());
        throw new FragmentInstanceDispatchException(resultStatus);
      } catch (IOException e) {
        LOGGER.warn("Serialize TsFileResource {} error.", filePath, e);
        TSStatus resultStatus = new TSStatus();
        resultStatus.setCode(TSStatusCode.LOAD_FILE_ERROR.getStatusCode());
        resultStatus.setMessage(e.getMessage());
        throw new FragmentInstanceDispatchException(resultStatus);
      }
    }
  }

  private void dispatchRemote(TTsFilePieceReq loadTsFileReq, TEndPoint endPoint)
      throws FragmentInstanceDispatchException {
    try (SyncDataNodeInternalServiceClient client =
        internalServiceClientManager.borrowClient(endPoint)) {
      client.setTimeout(CONNECTION_TIMEOUT_MS.get());

      final TLoadResp loadResp = client.sendTsFilePieceNode(loadTsFileReq);
      if (!loadResp.isAccepted()) {
        LOGGER.warn(loadResp.message);
        throw new FragmentInstanceDispatchException(loadResp.status);
      }
    } catch (Exception e) {
      adjustTimeoutIfNecessary(e);

      final String exceptionMessage =
          String.format(
              "failed to dispatch load command %s to node %s because of exception: %s",
              loadTsFileReq, endPoint, e);
      LOGGER.warn(exceptionMessage, e);
      throw new FragmentInstanceDispatchException(
          new TSStatus()
              .setCode(TSStatusCode.DISPATCH_ERROR.getStatusCode())
              .setMessage(exceptionMessage));
    }
  }

  public Future<FragInstanceDispatchResult> dispatchCommand(
      TLoadCommandReq originalLoadCommandReq, Set<TRegionReplicaSet> replicaSets) {
    Set<TEndPoint> allEndPoint = new HashSet<>();
    for (TRegionReplicaSet replicaSet : replicaSets) {
      for (TDataNodeLocation dataNodeLocation : replicaSet.getDataNodeLocations()) {
        allEndPoint.add(dataNodeLocation.getInternalEndPoint());
      }
    }

    for (TEndPoint endPoint : allEndPoint) {
      // duplicate for progress index binary serialization
      final TLoadCommandReq duplicatedLoadCommandReq = originalLoadCommandReq.deepCopy();
      try (SetThreadName threadName =
          new SetThreadName(
              "load-dispatcher"
                  + "-"
                  + LoadTsFileScheduler.LoadCommand.values()[duplicatedLoadCommandReq.commandType]
                  + "-"
                  + duplicatedLoadCommandReq.uuid)) {
        if (isDispatchedToLocal(endPoint)) {
          dispatchLocally(duplicatedLoadCommandReq);
        } else {
          dispatchRemote(duplicatedLoadCommandReq, endPoint);
        }
      } catch (FragmentInstanceDispatchException e) {
        LOGGER.warn(
            "Cannot dispatch LoadCommand for load operation {}", duplicatedLoadCommandReq, e);
        return immediateFuture(new FragInstanceDispatchResult(e.getFailureStatus()));
      } catch (Exception t) {
        LOGGER.warn(
            "Cannot dispatch LoadCommand for load operation {}", duplicatedLoadCommandReq, t);
        return immediateFuture(
            new FragInstanceDispatchResult(
                RpcUtils.getStatus(
                    TSStatusCode.INTERNAL_SERVER_ERROR, "Unexpected errors: " + t.getMessage())));
      }
    }
    return immediateFuture(new FragInstanceDispatchResult(true));
  }

  private void dispatchLocally(TLoadCommandReq loadCommandReq)
      throws FragmentInstanceDispatchException {
    final Map<TTimePartitionSlot, ProgressIndex> timePartitionProgressIndexMap = new HashMap<>();
    if (loadCommandReq.isSetTimePartition2ProgressIndex()) {
      for (Map.Entry<TTimePartitionSlot, ByteBuffer> entry :
          loadCommandReq.getTimePartition2ProgressIndex().entrySet()) {
        timePartitionProgressIndexMap.put(
            entry.getKey(), ProgressIndexType.deserializeFrom(entry.getValue()));
      }
    } else {
      final TSStatus status = new TSStatus();
      status.setCode(TSStatusCode.LOAD_FILE_ERROR.getStatusCode());
      status.setMessage("Load command requires time partition to progress index map");
      throw new FragmentInstanceDispatchException(status);
    }

    final TSStatus resultStatus =
        StorageEngine.getInstance()
            .executeLoadCommand(
                LoadTsFileScheduler.LoadCommand.values()[loadCommandReq.commandType],
                loadCommandReq.uuid,
                loadCommandReq.isSetIsGeneratedByPipe() && loadCommandReq.isGeneratedByPipe,
                timePartitionProgressIndexMap);
    if (!RpcUtils.SUCCESS_STATUS.equals(resultStatus)) {
      throw new FragmentInstanceDispatchException(resultStatus);
    }
  }

  private void dispatchRemote(TLoadCommandReq loadCommandReq, TEndPoint endPoint)
      throws FragmentInstanceDispatchException {
    try (SyncDataNodeInternalServiceClient client =
        internalServiceClientManager.borrowClient(endPoint)) {
      client.setTimeout(CONNECTION_TIMEOUT_MS.get());

      final TLoadResp loadResp = client.sendLoadCommand(loadCommandReq);
      if (!loadResp.isAccepted()) {
        LOGGER.warn(loadResp.message);
        throw new FragmentInstanceDispatchException(loadResp.status);
      }
    } catch (Exception e) {
      adjustTimeoutIfNecessary(e);

      final String exceptionMessage =
          String.format(
              "failed to dispatch load command %s to node %s because of exception: %s",
              loadCommandReq, endPoint, e);
      LOGGER.warn(exceptionMessage, e);
      throw new FragmentInstanceDispatchException(
          new TSStatus()
              .setCode(TSStatusCode.DISPATCH_ERROR.getStatusCode())
              .setMessage(exceptionMessage));
    }
  }

  private boolean isDispatchedToLocal(TEndPoint endPoint) {
    return this.localhostIpAddr.equals(endPoint.getIp()) && localhostInternalPort == endPoint.port;
  }

  private static void adjustTimeoutIfNecessary(Throwable e) {
    do {
      if (e instanceof SocketTimeoutException || e instanceof TimeoutException) {
        int newConnectionTimeout;
        try {
          newConnectionTimeout =
              Math.min(
                  Math.max(
                      FIRST_ADJUSTMENT_TIMEOUT_MS,
                      Math.toIntExact(CONNECTION_TIMEOUT_MS.get() * 2L)),
                  MAX_CONNECTION_TIMEOUT_MS);
        } catch (ArithmeticException arithmeticException) {
          newConnectionTimeout = MAX_CONNECTION_TIMEOUT_MS;
        }

        if (newConnectionTimeout != CONNECTION_TIMEOUT_MS.get()) {
          CONNECTION_TIMEOUT_MS.set(newConnectionTimeout);
          LOGGER.info(
              "Load remote procedure call connection timeout is adjusted to {} ms ({} mins)",
              newConnectionTimeout,
              newConnectionTimeout / 60000.0);
        }
        return;
      }
    } while ((e = e.getCause()) != null);
  }

  @Override
  public void abort() {
    // Do nothing
  }
}
