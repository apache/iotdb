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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * {@link LoadTsFileScheduler} is used for scheduling {@link LoadSingleTsFileNode} and {@link
 * LoadTsFilePieceNode}. because these two nodes need two phases to finish transfer.
 *
 * <p>for more details please check: <a
 * href="https://apache-iotdb.feishu.cn/docx/doxcnyBYWzek8ksSEU6obZMpYLe">...</a>;
 */
public class LoadTsFileScheduler implements IScheduler {
  private static final Logger logger = LoggerFactory.getLogger(LoadTsFileScheduler.class);

  private final MPPQueryContext queryContext;
  private QueryStateMachine stateMachine;
  private LoadTsFileDispatcherImpl dispatcher;
  private List<LoadSingleTsFileNode> tsFileNodeList;
  private PlanFragmentId fragmentId;

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
    if (!dispatchOneTsFile(node)) {
      logger.error(
          String.format("Dispatch Single TsFile Node error, LoadSingleTsFileNode %s.", node));
      return false;
    }
    return true;
  }

  private boolean dispatchOneTsFile(LoadSingleTsFileNode node) {
    for (Map.Entry<TRegionReplicaSet, List<LoadTsFilePieceNode>> entry :
        node.getReplicaSet2Pieces().entrySet()) {
      allReplicaSets.add(entry.getKey());
      for (LoadTsFilePieceNode pieceNode : entry.getValue()) {
        FragmentInstance instance =
            new FragmentInstance(
                new PlanFragment(fragmentId, pieceNode),
                fragmentId.genFragmentInstanceId(),
                null,
                queryContext.getQueryType(),
                queryContext.getTimeOut());
        instance.setDataRegionAndHost(entry.getKey());
        Future<FragInstanceDispatchResult> dispatchResultFuture =
            dispatcher.dispatch(Collections.singletonList(instance));

        try {
          FragInstanceDispatchResult result = dispatchResultFuture.get();
          if (!result.isSuccessful()) {
            // TODO: retry.
            logger.error(
                String.format(
                    "Dispatch one piece  to ReplicaSet %s error, result status code %s.",
                    entry.getKey(),
                    TSStatusCode.representOf(result.getFailureStatus().getCode()).name()));
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
            logger.error(String.format("Dispatch piece node:%n%s", pieceNode));
            stateMachine.transitionToFailed(result.getFailureStatus()); // TODO: record more status
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
      }
    }
    return true;
  }

  private boolean secondPhase(boolean isFirstPhaseSuccess, String uuid) {
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
}
