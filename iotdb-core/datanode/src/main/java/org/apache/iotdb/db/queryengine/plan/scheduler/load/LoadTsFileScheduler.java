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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.partition.StorageExecutor;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.LoadReadOnlyException;
import org.apache.iotdb.db.exception.mpp.FragmentInstanceDispatchException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInfo;
import org.apache.iotdb.db.queryengine.execution.load.DataPartitionBatchFetcher;
import org.apache.iotdb.db.queryengine.execution.load.TsFileDataManager;
import org.apache.iotdb.db.queryengine.execution.load.TsFileSplitter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.queryengine.plan.scheduler.FragInstanceDispatchResult;
import org.apache.iotdb.db.queryengine.plan.scheduler.IScheduler;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.mpp.rpc.thrift.TLoadCommandReq;
import org.apache.iotdb.rpc.TSStatusCode;

import io.airlift.units.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * {@link LoadTsFileScheduler} is used for scheduling {@link LoadSingleTsFileNode} and {@link
 * LoadTsFilePieceNode}. because these two nodes need two phases to finish transfer.
 *
 * <p>for more details please check: <a
 * href="https://apache-iotdb.feishu.cn/docx/doxcnyBYWzek8ksSEU6obZMpYLe">...</a>;
 */
public class LoadTsFileScheduler implements IScheduler {

  private static final Logger logger = LoggerFactory.getLogger(LoadTsFileScheduler.class);
  public static final long LOAD_TASK_MAX_TIME_IN_SECOND = 900L; // 15min
  public static final long MAX_MEMORY_SIZE;
  public static final int TRANSMIT_LIMIT;

  static {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    MAX_MEMORY_SIZE =
        Math.min(
            config.getThriftMaxFrameSize() >> 2,
            (long) (config.getAllocateMemoryForStorageEngine() * config.getLoadTsFileProportion()));
    TRANSMIT_LIMIT =
        CommonDescriptor.getInstance().getConfig().getTTimePartitionSlotTransmitLimit();
  }

  public static long getMaxMemorySize() {
    return MAX_MEMORY_SIZE;
  }

  private final MPPQueryContext queryContext;
  private final QueryStateMachine stateMachine;
  private final LoadTsFileDispatcherImpl dispatcher;
  private final DataPartitionBatchFetcher partitionFetcher;
  private final List<LoadSingleTsFileNode> tsFileNodeList;
  private final PlanFragmentId fragmentId;
  private final Set<TRegionReplicaSet> allReplicaSets;
  private final boolean isGeneratedByPipe;

  public LoadTsFileScheduler(
      DistributedQueryPlan distributedQueryPlan,
      MPPQueryContext queryContext,
      QueryStateMachine stateMachine,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> internalServiceClientManager,
      DataPartitionBatchFetcher partitionFetcher,
      boolean isGeneratedByPipe) {
    this.queryContext = queryContext;
    this.stateMachine = stateMachine;
    this.tsFileNodeList = new ArrayList<>();
    this.fragmentId = distributedQueryPlan.getRootSubPlan().getPlanFragment().getId();
    this.dispatcher = new LoadTsFileDispatcherImpl(internalServiceClientManager, isGeneratedByPipe);
    this.partitionFetcher = partitionFetcher;
    this.allReplicaSets = new HashSet<>();
    this.isGeneratedByPipe = isGeneratedByPipe;

    for (FragmentInstance fragmentInstance : distributedQueryPlan.getInstances()) {
      tsFileNodeList.add((LoadSingleTsFileNode) fragmentInstance.getFragment().getPlanNodeTree());
    }
  }

  @Override
  public void start() {
    stateMachine.transitionToRunning();
    int tsFileNodeListSize = tsFileNodeList.size();
    boolean isLoadSuccess = true;

    for (int i = 0; i < tsFileNodeListSize; ++i) {
      LoadSingleTsFileNode node = tsFileNodeList.get(i);
      boolean isLoadSingleTsFileSuccess = true;
      try {
        if (node.isTsFileEmpty()) {
          logger.info(
              "Load skip TsFile {}, because it has no data.",
              node.getTsFileResource().getTsFilePath());

        } else if (!node.needDecodeTsFile(
            slotList ->
                partitionFetcher.queryDataPartition(
                    slotList,
                    queryContext.getSession().getUserName()))) { // do not decode, load locally
          isLoadSingleTsFileSuccess = loadLocally(node);
          node.clean();

        } else { // need decode, load locally or remotely, use two phases method
          String uuid = UUID.randomUUID().toString();
          dispatcher.setUuid(uuid);
          allReplicaSets.clear();

          boolean isFirstPhaseSuccess = firstPhase(node);
          boolean isSecondPhaseSuccess =
              secondPhase(isFirstPhaseSuccess, uuid, node.getTsFileResource().getTsFile());

          node.clean();
          if (!isFirstPhaseSuccess || !isSecondPhaseSuccess) {
            isLoadSingleTsFileSuccess = false;
          }
        }
        if (isLoadSingleTsFileSuccess) {
          logger.info(
              "Load TsFile {} Successfully, load process [{}/{}]",
              node.getTsFileResource().getTsFilePath(),
              i + 1,
              tsFileNodeListSize);
        } else {
          isLoadSuccess = false;
          logger.warn(
              "Can not Load TsFile {}, load process [{}/{}]",
              node.getTsFileResource().getTsFilePath(),
              i + 1,
              tsFileNodeListSize);
        }
      } catch (Exception e) {
        isLoadSuccess = false;
        stateMachine.transitionToFailed(e);
        logger.warn(
            String.format(
                "LoadTsFileScheduler loads TsFile %s error",
                node.getTsFileResource().getTsFilePath()),
            e);
      }
    }
    if (isLoadSuccess) {
      stateMachine.transitionToFinished();
    }
  }

  private boolean firstPhase(LoadSingleTsFileNode node) {
    final TsFileDataManager tsFileDataManager =
        new TsFileDataManager(
            this::dispatchOnePieceNode,
            node.getPlanNodeId(),
            node.getTsFileResource().getTsFile(),
            partitionFetcher,
            MAX_MEMORY_SIZE,
            queryContext.getSession().getUserName());
    try {
      new TsFileSplitter(
              node.getTsFileResource().getTsFile(), tsFileDataManager::addOrSendTsFileData, 0)
          .splitTsFileByDataPartition();
      if (!tsFileDataManager.sendAllTsFileData()) {
        stateMachine.transitionToFailed(new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode()));
        return false;
      }
    } catch (IllegalStateException e) {
      stateMachine.transitionToFailed(e);
      logger.warn(
          String.format(
              "Dispatch TsFileData error when parsing TsFile %s.",
              node.getTsFileResource().getTsFile()),
          e);
      return false;
    } catch (Exception e) {
      stateMachine.transitionToFailed(e);
      logger.warn(
          String.format("Parse or send TsFile %s error.", node.getTsFileResource().getTsFile()), e);
      return false;
    } finally {
      tsFileDataManager.clear();
    }
    return true;
  }

  public boolean dispatchOnePieceNode(LoadTsFilePieceNode pieceNode, TRegionReplicaSet replicaSet) {
    allReplicaSets.add(replicaSet);
    FragmentInstance instance =
        new FragmentInstance(
            new PlanFragment(fragmentId, pieceNode),
            fragmentId.genFragmentInstanceId(),
            null,
            queryContext.getQueryType(),
            queryContext.getTimeOut(),
            queryContext.getSession());
    instance.setExecutorAndHost(new StorageExecutor(replicaSet));
    Future<FragInstanceDispatchResult> dispatchResultFuture =
        dispatcher.dispatch(Collections.singletonList(instance));

    try {
      FragInstanceDispatchResult result =
          dispatchResultFuture.get(
              LoadTsFileScheduler.LOAD_TASK_MAX_TIME_IN_SECOND, TimeUnit.SECONDS);
      if (!result.isSuccessful()) {
        // TODO: retry.
        logger.warn(
            "Dispatch one piece to ReplicaSet {} error. Result status code {}. "
                + "Result status message {}. Dispatch piece node error:%n{}",
            replicaSet,
            TSStatusCode.representOf(result.getFailureStatus().getCode()).name(),
            result.getFailureStatus().getMessage(),
            pieceNode);
        if (result.getFailureStatus().getSubStatus() != null) {
          for (TSStatus status : result.getFailureStatus().getSubStatus()) {
            logger.warn(
                "Sub status code {}. Sub status message {}.",
                TSStatusCode.representOf(status.getCode()).name(),
                status.getMessage());
          }
        }
        TSStatus status = result.getFailureStatus();
        status.setMessage(
            String.format("Load %s piece error in 1st phase. Because ", pieceNode.getTsFile())
                + status.getMessage());
        stateMachine.transitionToFailed(status); // TODO: record more status
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
      logger.warn(
          String.format("Wait for loading %s time out.", LoadTsFilePieceNode.class.getName()), e);
      stateMachine.transitionToFailed(e);
      return false;
    }
    return true;
  }

  private boolean secondPhase(boolean isFirstPhaseSuccess, String uuid, File tsFile) {
    logger.info("Start dispatching Load command for uuid {}", uuid);
    TLoadCommandReq loadCommandReq =
        new TLoadCommandReq(
            (isFirstPhaseSuccess ? LoadCommand.EXECUTE : LoadCommand.ROLLBACK).ordinal(), uuid);
    loadCommandReq.setIsGeneratedByPipe(isGeneratedByPipe);
    Future<FragInstanceDispatchResult> dispatchResultFuture =
        dispatcher.dispatchCommand(loadCommandReq, allReplicaSets);

    try {
      FragInstanceDispatchResult result = dispatchResultFuture.get();
      if (!result.isSuccessful()) {
        // TODO: retry.
        logger.warn(
            "Dispatch load command {} of TsFile {} error to replicaSets {} error. "
                + "Result status code {}. Result status message {}.",
            loadCommandReq,
            tsFile,
            allReplicaSets,
            TSStatusCode.representOf(result.getFailureStatus().getCode()).name(),
            result.getFailureStatus().getMessage());
        TSStatus status = result.getFailureStatus();
        status.setMessage(
            String.format("Load %s error in 2nd phase. Because ", tsFile) + status.getMessage());
        stateMachine.transitionToFailed(status);
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

  private boolean loadLocally(LoadSingleTsFileNode node) throws IoTDBException {
    logger.info("Start load TsFile {} locally.", node.getTsFileResource().getTsFile().getPath());

    if (CommonDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new LoadReadOnlyException();
    }

    try {
      FragmentInstance instance =
          new FragmentInstance(
              new PlanFragment(fragmentId, node),
              fragmentId.genFragmentInstanceId(),
              null,
              queryContext.getQueryType(),
              queryContext.getTimeOut(),
              queryContext.getSession());
      instance.setExecutorAndHost(new StorageExecutor(node.getLocalRegionReplicaSet()));
      dispatcher.dispatchLocally(instance);
    } catch (FragmentInstanceDispatchException e) {
      logger.warn(
          String.format(
              "Dispatch tsFile %s error to local error. Result status code %s. "
                  + "Result status message %s.",
              node.getTsFileResource().getTsFile(),
              TSStatusCode.representOf(e.getFailureStatus().getCode()).name(),
              e.getFailureStatus().getMessage()));
      stateMachine.transitionToFailed(e.getFailureStatus());
      return false;
    }

    // add metrics
    DataRegion dataRegion =
        StorageEngine.getInstance()
            .getDataRegion(
                (DataRegionId)
                    ConsensusGroupId.Factory.createFromTConsensusGroupId(
                        node.getLocalRegionReplicaSet().getRegionId()));
    MetricService.getInstance()
        .count(
            node.getWritePointCount(),
            Metric.QUANTITY.toString(),
            MetricLevel.CORE,
            Tag.NAME.toString(),
            Metric.POINTS_IN.toString(),
            Tag.DATABASE.toString(),
            dataRegion.getDatabaseName(),
            Tag.REGION.toString(),
            dataRegion.getDataRegionId());
    return true;
  }

  @Override
  public void stop(Throwable t) {
    // Do nothing
  }

  @Override
  public Duration getTotalCpuTime() {
    return null;
  }

  @Override
  public FragmentInfo getFragmentInfo() {
    return null;
  }

  public enum LoadCommand {
    EXECUTE,
    ROLLBACK
  }

  public DataPartitionBatchFetcher getPartitionFetcher() {
    return partitionFetcher;
  }
}
