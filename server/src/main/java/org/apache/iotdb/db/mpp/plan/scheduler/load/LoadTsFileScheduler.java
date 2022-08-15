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
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInfo;
import org.apache.iotdb.db.mpp.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.mpp.plan.scheduler.FragInstanceDispatchResult;
import org.apache.iotdb.db.mpp.plan.scheduler.IScheduler;

import io.airlift.units.Duration;
import org.apache.iotdb.mpp.rpc.thrift.TLoadCommandReq;
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

public class LoadTsFileScheduler implements IScheduler {
  private static final Logger logger = LoggerFactory.getLogger(LoadTsFileScheduler.class);

  private final String uuid;
  private final MPPQueryContext queryContext;
  private LoadTsFileDispatcherImpl dispatcher;
  private List<LoadSingleTsFileNode> tsFileNodeList;
  private PlanFragmentId fragmentId;

  private Set<TRegionReplicaSet> allReplicaSets;

  public LoadTsFileScheduler(
      DistributedQueryPlan distributedQueryPlan,
      MPPQueryContext queryContext,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> internalServiceClientManager) {
    this.uuid = UUID.randomUUID().toString();
    this.queryContext = queryContext;
    this.tsFileNodeList = new ArrayList<>();
    this.fragmentId = distributedQueryPlan.getRootSubPlan().getPlanFragment().getId();
    this.dispatcher = new LoadTsFileDispatcherImpl(uuid, internalServiceClientManager);
    this.allReplicaSets = new HashSet<>();

    for (FragmentInstance fragmentInstance : distributedQueryPlan.getInstances()) {
      tsFileNodeList.add((LoadSingleTsFileNode) fragmentInstance.getFragment().getRoot());
    }
  }

  @Override
  public void start() {
    boolean isFirstPhaseSuccess = firstPhase();
    boolean isSuccess = secondPhase(isFirstPhaseSuccess);
  }

  private boolean firstPhase() {
    for (LoadSingleTsFileNode node : tsFileNodeList) {
      if (!dispatchOneTsFile(node)) {
        logger.error(
            String.format("Dispatch Single TsFile Node error, LoadSingleTsFileNode %s.", node));
        return false;
      }
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
                    entry.getKey(), result.getFailureStatus().getCode()));
            logger.error(
                String.format("Result message %s.", result.getFailureStatus().getMessage()));
            logger.error(String.format("Dispatch piece node:%n%s", pieceNode));
            return false;
          }
        } catch (InterruptedException | ExecutionException e) {
          if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
          }
          logger.warn("Interrupt or Execution error.", e);
          return false;
        }
      }
    }
    return true;
  }

  private boolean secondPhase(boolean isFirstPhaseSuccess) {
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
        return false;
      }
    } catch (InterruptedException | ExecutionException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      logger.warn("Interrupt or Execution error.", e);
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

  public enum LoadResult {
    SUCCESS,
    FAILED
  }
}
