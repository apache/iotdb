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
package org.apache.iotdb.db.mpp.execution.scheduler;

import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.execution.FragmentInfo;
import org.apache.iotdb.db.mpp.execution.QueryState;
import org.apache.iotdb.db.mpp.execution.QueryStateMachine;
import org.apache.iotdb.db.mpp.sql.planner.plan.FragmentInstance;

import io.airlift.units.Duration;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

/**
 * QueryScheduler is used to dispatch the fragment instances of a query to target nodes. And it will
 * continue to collect and monitor the query execution before the query is finished.
 *
 * <p>Later, we can add more control logic for a QueryExecution such as retry, kill and so on by
 * this scheduler.
 */
public class ClusterScheduler implements IScheduler {
  // The stateMachine of the QueryExecution owned by this QueryScheduler
  private QueryStateMachine stateMachine;

  // The fragment instances which should be sent to corresponding Nodes.
  private List<FragmentInstance> instances;
  private IFragInstanceDispatcher dispatcher;

  public ClusterScheduler(QueryStateMachine stateMachine, List<FragmentInstance> instances) {
    this.stateMachine = stateMachine;
    this.instances = instances;
    this.dispatcher = new SimpleFragInstanceDispatcher();
  }

  @Override
  public void start() {
    // TODO: consider where the state transition should be put
    stateMachine.transitionToDispatching();
    Future<FragInstanceDispatchResult> dispatchResultFuture = dispatcher.dispatch(instances);
    //TODO: start the FragmentInstance state fetcher
    try {
      FragInstanceDispatchResult result = dispatchResultFuture.get();
      if (result.isSuccessful()) {
        stateMachine.transitionToRunning();
      } else {
        stateMachine.transitionToFailed();
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void abort() {}

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

  // Send the instances to other nodes
  private void sendFragmentInstances() {}

  // After sending, start to collect the states of these fragment instances
  private void startMonitorInstances() {}
}
