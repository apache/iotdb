/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.metadata.LocalSchemaProcessor;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.execution.QueryStateMachine;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInfo;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;

import io.airlift.units.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class StandaloneScheduler implements IScheduler {

  private static final StorageEngineV2 STORAGE_ENGINE = StorageEngineV2.getInstance();

  private static final LocalSchemaProcessor SCHEMA_ENGINE = LocalSchemaProcessor.getInstance();

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterScheduler.class);

  private MPPQueryContext queryContext;
  // The stateMachine of the QueryExecution owned by this QueryScheduler
  private QueryStateMachine stateMachine;
  private QueryType queryType;
  // The fragment instances which should be sent to corresponding Nodes.
  private List<FragmentInstance> instances;

  private ExecutorService executor;
  private ScheduledExecutorService scheduledExecutor;

  private IFragInstanceDispatcher dispatcher;
  private IFragInstanceStateTracker stateTracker;
  private IQueryTerminator queryTerminator;

  public StandaloneScheduler(
      MPPQueryContext queryContext,
      QueryStateMachine stateMachine,
      List<FragmentInstance> instances,
      QueryType queryType,
      ExecutorService executor,
      ScheduledExecutorService scheduledExecutor,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> internalServiceClientManager) {
    this.queryContext = queryContext;
    this.instances = instances;
    this.queryType = queryType;
    this.executor = executor;
    this.scheduledExecutor = scheduledExecutor;
    this.stateTracker =
        new FixedRateFragInsStateTracker(
            stateMachine, executor, scheduledExecutor, instances, internalServiceClientManager);
    this.queryTerminator =
        new SimpleQueryTerminator(
            executor, queryContext.getQueryId(), instances, internalServiceClientManager);
  }

  @Override
  public void start() {
    // For the FragmentInstance of WRITE, it will be executed directly when dispatching.
    // TODO: Other QueryTypes
    if (queryType == QueryType.WRITE) {

      return;
    }
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
