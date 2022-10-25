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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.load.ChunkData;
import org.apache.iotdb.db.engine.load.TsFileData;
import org.apache.iotdb.db.engine.load.TsFileSplitter;
import org.apache.iotdb.db.exception.mpp.FragmentInstanceDispatchException;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.execution.QueryStateMachine;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInfo;
import org.apache.iotdb.db.mpp.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.mpp.plan.scheduler.FragInstanceDispatchResult;
import org.apache.iotdb.db.mpp.plan.scheduler.IScheduler;
import org.apache.iotdb.mpp.rpc.thrift.TLoadCommandReq;
import org.apache.iotdb.rpc.TSStatusCode;

import io.airlift.units.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * {@link LoadTsFileScheduler} is used for scheduling {@link LoadSingleTsFileNode} and {@link
 * LoadTsFilePieceNode}. because these two nodes need two phases to finish transfer.
 *
 * <p>for more details please check: <a
 * href="https://apache-iotdb.feishu.cn/docx/doxcnyBYWzek8ksSEU6obZMpYLe">...</a>;
 */
public class LoadTsFileScheduler implements IScheduler {
  private static final Logger logger = LoggerFactory.getLogger(LoadTsFileScheduler.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  public static final long LOAD_TASK_MAX_TIME_IN_SECOND = 5184000L; // one day
  private static final long MAX_MEMORY_SIZE =
      Math.min(config.getThriftMaxFrameSize() / 2, config.getAllocateMemoryForStorageEngine() / 8);

  private final MPPQueryContext queryContext;
  private final QueryStateMachine stateMachine;
  private final LoadTsFileDispatcherImpl dispatcher;
  private final List<LoadSingleTsFileNode> tsFileNodeList;
  private final PlanFragmentId fragmentId;

  private Set<TRegionReplicaSet> allReplicaSets;

  public LoadTsFileScheduler(
      DistributedQueryPlan distributedQueryPlan,
      MPPQueryContext queryContext,
      QueryStateMachine stateMachine,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> internalServiceClientManager) {
    this.queryContext = queryContext;
    this.stateMachine = stateMachine;
    this.tsFileNodeList = new ArrayList<>();
    this.fragmentId = distributedQueryPlan.getRootSubPlan().getPlanFragment().getId();
    this.dispatcher = new LoadTsFileDispatcherImpl(internalServiceClientManager);
    this.allReplicaSets = new HashSet<>();

    for (FragmentInstance fragmentInstance : distributedQueryPlan.getInstances()) {
      tsFileNodeList.add((LoadSingleTsFileNode) fragmentInstance.getFragment().getPlanNodeTree());
    }
  }

  @Override
  public void start() {
    stateMachine.transitionToRunning();
    for (LoadSingleTsFileNode node : tsFileNodeList) {
      if (!node.needDecodeTsFile()) {
        boolean isLoadLocallySuccess = loadLocally(node);

        node.clean();
        if (!isLoadLocallySuccess) {
          return;
        }
        continue;
      }

      String uuid = UUID.randomUUID().toString();
      dispatcher.setUuid(uuid);
      allReplicaSets.clear();

      boolean isFirstPhaseSuccess = firstPhase(node);
      boolean isSecondPhaseSuccess = secondPhase(isFirstPhaseSuccess, uuid);

      node.clean();
      if (!isFirstPhaseSuccess || !isSecondPhaseSuccess) {
        return;
      }
    }
    stateMachine.transitionToFinished();
  }

  private boolean firstPhase(LoadSingleTsFileNode node) {
    try {
      new TsFileSplitter(
              node.getTsFileResource().getTsFile(),
              new TsFileDataManager(this, node)::addOrSendTsFileData)
          .splitTsFileByDataPartition();
    } catch (IllegalStateException e) {
      return false;
    } catch (Exception e) {
      stateMachine.transitionToFailed(e);
      logger.error(
          String.format("Parse TsFile %s error.", node.getTsFileResource().getTsFile()), e);
      return false;
    }
    return true;
  }

  private boolean dispatchOnePieceNode(
      LoadTsFilePieceNode pieceNode, TRegionReplicaSet replicaSet) {
    allReplicaSets.add(replicaSet);
    FragmentInstance instance =
        new FragmentInstance(
            new PlanFragment(fragmentId, pieceNode),
            fragmentId.genFragmentInstanceId(),
            null,
            queryContext.getQueryType(),
            queryContext.getTimeOut());
    instance.setDataRegionAndHost(replicaSet);
    Future<FragInstanceDispatchResult> dispatchResultFuture =
        dispatcher.dispatch(Collections.singletonList(instance));

    try {
      FragInstanceDispatchResult result =
          dispatchResultFuture.get(
              LoadTsFileScheduler.LOAD_TASK_MAX_TIME_IN_SECOND, TimeUnit.SECONDS);
      if (!result.isSuccessful()) {
        // TODO: retry.
        logger.error(
            String.format(
                "Dispatch one piece to ReplicaSet %s error, result status code %s.",
                replicaSet, TSStatusCode.representOf(result.getFailureStatus().getCode()).name()));
        logger.error(
            String.format("Result status message %s.", result.getFailureStatus().getMessage()));
        if (result.getFailureStatus().getSubStatus() != null) {
          for (TSStatus status : result.getFailureStatus().getSubStatus()) {
            logger.error(
                String.format(
                    "Sub status code %s.", TSStatusCode.representOf(status.getCode()).name()));
            logger.error(String.format("Sub status message %s.", status.getMessage()));
          }
        }
        logger.error(String.format("Dispatch piece node error:%n%s", pieceNode));
        stateMachine.transitionToFailed(result.getFailureStatus()); // TODO: record more status
        return false;
      }
    } catch (InterruptedException | ExecutionException | CancellationException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      logger.warn("Interrupt or Execution error.", e);
      stateMachine.transitionToFailed(e);
      return false;
    } catch (TimeoutException e) {
      dispatchResultFuture.cancel(true);
      logger.error(
          String.format("Wait for loading %s time out.", LoadTsFilePieceNode.class.getName()), e);
      stateMachine.transitionToFailed(e);
      return false;
    }
    return true;
  }

  private boolean secondPhase(boolean isFirstPhaseSuccess, String uuid) {
    logger.info(String.format("Start dispatching Load command for uuid %s", uuid));
    TLoadCommandReq loadCommandReq =
        new TLoadCommandReq(
            (isFirstPhaseSuccess ? LoadCommand.EXECUTE : LoadCommand.ROLLBACK).ordinal(), uuid);
    Future<FragInstanceDispatchResult> dispatchResultFuture =
        dispatcher.dispatchCommand(loadCommandReq, allReplicaSets);

    try {
      FragInstanceDispatchResult result = dispatchResultFuture.get();
      if (!result.isSuccessful()) {
        // TODO: retry.
        logger.error(
            String.format("Dispatch LoadCommand error to replicaSets %s error.", allReplicaSets));
        logger.error(String.format("Result status code %s.", result.getFailureStatus().getCode()));
        logger.error(
            String.format("Result status message %s.", result.getFailureStatus().getMessage()));
        stateMachine.transitionToFailed(result.getFailureStatus());
        return false;
      }
    } catch (InterruptedException | ExecutionException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      logger.warn("Interrupt or Execution error.", e);
      stateMachine.transitionToFailed(e);
      return false;
    }
    return true;
  }

  private boolean loadLocally(LoadSingleTsFileNode node) {
    logger.info(
        String.format(
            "Start load TsFile %s locally.", node.getTsFileResource().getTsFile().getPath()));
    try {
      FragmentInstance instance =
          new FragmentInstance(
              new PlanFragment(fragmentId, node),
              fragmentId.genFragmentInstanceId(),
              null,
              queryContext.getQueryType(),
              queryContext.getTimeOut());
      instance.setDataRegionAndHost(node.getLocalRegionReplicaSet());
      dispatcher.dispatchLocally(instance);
    } catch (FragmentInstanceDispatchException e) {
      logger.error("Dispatch LoadCommand error to local error.");
      logger.error(String.format("Result status code %s.", e.getFailureStatus().getCode()));
      logger.error(String.format("Result status message %s.", e.getFailureStatus().getMessage()));
      stateMachine.transitionToFailed(e.getFailureStatus());
      return false;
    }
    return true;
  }

  @Override
  public void stop() {}

  @Override
  public Duration getTotalCpuTime() {
    return null;
  }

  @Override
  public FragmentInfo getFragmentInfo() {
    return null;
  }

  @Override
  public void abortFragmentInstance(FragmentInstanceId instanceId, Throwable failureCause) {}

  @Override
  public void cancelFragment(PlanFragmentId planFragmentId) {}

  public enum LoadCommand {
    EXECUTE,
    ROLLBACK
  }

  private class TsFileDataManager {
    private final LoadTsFileScheduler scheduler;
    private final LoadSingleTsFileNode singleTsFileNode;

    private long dataSize;
    private Map<TRegionReplicaSet, LoadTsFilePieceNode> replicaSet2Piece;

    public TsFileDataManager(LoadTsFileScheduler scheduler, LoadSingleTsFileNode singleTsFileNode) {
      this.scheduler = scheduler;
      this.singleTsFileNode = singleTsFileNode;
      this.dataSize = 0;
      this.replicaSet2Piece = new HashMap<>();
    }

    private boolean addOrSendTsFileData(TsFileData tsFileData) {
      return tsFileData.isModification()
          ? addOrSendDeletionData(tsFileData)
          : addOrSendChunkData((ChunkData) tsFileData);
    }

    private boolean addOrSendChunkData(ChunkData chunkData) {
      dataSize += chunkData.getDataSize();
      if (dataSize > MAX_MEMORY_SIZE) {
        List<TRegionReplicaSet> sortedReplicaSets =
            replicaSet2Piece.keySet().stream()
                .sorted(
                    Comparator.comparingLong(o -> replicaSet2Piece.get(o).getDataSize()).reversed())
                .collect(Collectors.toList());

        for (TRegionReplicaSet sortedReplicaSet : sortedReplicaSets) {
          LoadTsFilePieceNode pieceNode = replicaSet2Piece.get(sortedReplicaSet);
          if (pieceNode.getDataSize() == 0) { // total data size has been reduced to 0
            break;
          }
          if (!scheduler.dispatchOnePieceNode(pieceNode, sortedReplicaSet)) {
            return false;
          }

          dataSize -= pieceNode.getDataSize();
          replicaSet2Piece.put(
              sortedReplicaSet,
              new LoadTsFilePieceNode(
                  singleTsFileNode.getPlanNodeId(),
                  singleTsFileNode.getTsFileResource().getTsFile()));
          if (dataSize <= MAX_MEMORY_SIZE) {
            break;
          }
        }
      }
      TRegionReplicaSet replicaSet =
          singleTsFileNode
              .getDataPartition()
              .getDataRegionReplicaSetForWriting(
                  chunkData.getDevice(), chunkData.getTimePartitionSlot());
      replicaSet2Piece
          .computeIfAbsent(
              replicaSet,
              o ->
                  new LoadTsFilePieceNode(
                      singleTsFileNode.getPlanNodeId(),
                      singleTsFileNode.getTsFileResource().getTsFile()))
          .addTsFileData(chunkData);
      return true;
    }

    private boolean addOrSendDeletionData(TsFileData deletionData) {
      for (Map.Entry<TRegionReplicaSet, LoadTsFilePieceNode> entry : replicaSet2Piece.entrySet()) {
        dataSize += deletionData.getDataSize();
        entry.getValue().addTsFileData(deletionData);
      }
      return true;
    }
  }
}
