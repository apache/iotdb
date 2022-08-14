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

import io.airlift.units.Duration;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInfo;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceState;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.mpp.plan.scheduler.FragInstanceDispatchResult;
import org.apache.iotdb.db.mpp.plan.scheduler.IScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class LoadTsFileScheduler implements IScheduler {
  private static final Logger logger = LoggerFactory.getLogger(LoadTsFileScheduler.class);

  private String uuid;
  private LoadTsFileDispatcherImpl dispatcher;

  private List<LoadSingleTsFileNode> tsFileNodeList;
  private MPPQueryContext queryContext;

  public LoadTsFileScheduler(
      DistributedQueryPlan distributedQueryPlan, MPPQueryContext queryContext) {
    this.uuid = UUID.randomUUID().toString();
    this.tsFileNodeList = new ArrayList<>();
    this.queryContext = queryContext;

    for (FragmentInstance fragmentInstance : distributedQueryPlan.getInstances()) {
      tsFileNodeList.add((LoadSingleTsFileNode) fragmentInstance.getFragment().getRoot());
    }
  }

  @Override
  public void start() {}

  private boolean firstPhase() {
    for (LoadSingleTsFileNode node : tsFileNodeList) {}
    return true;
  }

  private boolean dispatchOneTsFile(LoadSingleTsFileNode node) {
    for (Map.Entry<TRegionReplicaSet, List<LoadTsFilePieceNode>> entry :
        node.getReplicaSet2Pieces().entrySet()) {
      for (LoadTsFilePieceNode pieceNode : entry.getValue()) {
        FragmentInstance instance =
            new FragmentInstance(
                new PlanFragment(null, pieceNode),
                null,
                null,
                queryContext.getQueryType(),
                queryContext.getTimeOut());
        instance.setDataRegionAndHost(entry.getKey());
        Future<FragInstanceDispatchResult> dispatchResultFuture = dispatcher.dispatch(Collections.singletonList(instance));

        try {
          FragInstanceDispatchResult result = dispatchResultFuture.get();
          if (!result.isSuccessful()) {
            logger.error("Dispatch pieceNode error");
            return false;
          }
        } catch (InterruptedException | ExecutionException e) {
          // If the dispatch request cannot be sent or TException is caught, we will retry this query.
          if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
          }
//          stateMachine.transitionToFailed(e);
          return false;
        }
      }
    }
    return true;
  }

  private boolean secondPhase() {
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
}
