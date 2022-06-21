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

package org.apache.iotdb.db.mpp.plan.scheduler;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.mpp.execution.QueryStateMachine;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceState;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStateReq;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceStateResp;

import org.apache.thrift.TException;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public abstract class AbstractFragInsStateTracker implements IFragInstanceStateTracker {

  protected QueryStateMachine stateMachine;
  protected ExecutorService executor;
  protected ScheduledExecutorService scheduledExecutor;
  protected List<FragmentInstance> instances;

  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      internalServiceClientManager;

  public AbstractFragInsStateTracker(
      QueryStateMachine stateMachine,
      ExecutorService executor,
      ScheduledExecutorService scheduledExecutor,
      List<FragmentInstance> instances,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> internalServiceClientManager) {
    this.stateMachine = stateMachine;
    this.executor = executor;
    this.scheduledExecutor = scheduledExecutor;
    this.instances = instances;
    this.internalServiceClientManager = internalServiceClientManager;
  }

  public abstract void start();

  public abstract void abort();

  protected FragmentInstanceState fetchState(FragmentInstance instance)
      throws TException, IOException {
    TEndPoint endPoint = instance.getHostDataNode().internalEndPoint;
    try (SyncDataNodeInternalServiceClient client =
        internalServiceClientManager.borrowClient(endPoint)) {
      TFragmentInstanceStateResp resp =
          client.fetchFragmentInstanceState(new TFetchFragmentInstanceStateReq(getTId(instance)));
      return FragmentInstanceState.valueOf(resp.state);
    }
  }

  private TFragmentInstanceId getTId(FragmentInstance instance) {
    return new TFragmentInstanceId(
        instance.getId().getQueryId().getId(),
        instance.getId().getFragmentId().getId(),
        instance.getId().getInstanceId());
  }
}
