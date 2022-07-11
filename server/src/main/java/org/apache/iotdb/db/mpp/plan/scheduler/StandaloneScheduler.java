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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.execution.QueryStateMachine;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInfo;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceState;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.analyze.SchemaValidator;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.rpc.TSStatusCode;

import io.airlift.units.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class StandaloneScheduler implements IScheduler {

  private static final StorageEngineV2 STORAGE_ENGINE = StorageEngineV2.getInstance();

  private static final SchemaEngine SCHEMA_ENGINE = SchemaEngine.getInstance();

  private static final Logger LOGGER = LoggerFactory.getLogger(StandaloneScheduler.class);

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
    this.stateMachine = stateMachine;
    this.stateTracker =
        new FixedRateFragInsStateTracker(
            stateMachine, executor, scheduledExecutor, instances, internalServiceClientManager);
    this.queryTerminator =
        new SimpleQueryTerminator(
            executor,
            scheduledExecutor,
            queryContext.getQueryId(),
            instances,
            internalServiceClientManager);
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public void start() {
    stateMachine.transitionToDispatching();
    // For the FragmentInstance of WRITE, it will be executed directly when dispatching.
    // TODO: Other QueryTypes
    switch (queryType) {
      case READ:
        try {
          for (FragmentInstance fragmentInstance : instances) {
            ConsensusGroupId groupId =
                ConsensusGroupId.Factory.createFromTConsensusGroupId(
                    fragmentInstance.getRegionReplicaSet().getRegionId());
            if (groupId instanceof DataRegionId) {
              DataRegion region =
                  StorageEngineV2.getInstance().getDataRegion((DataRegionId) groupId);
              FragmentInstanceManager.getInstance()
                  .execDataQueryFragmentInstance(fragmentInstance, region);
            } else {
              ISchemaRegion region =
                  SchemaEngine.getInstance().getSchemaRegion((SchemaRegionId) groupId);
              FragmentInstanceManager.getInstance()
                  .execSchemaQueryFragmentInstance(fragmentInstance, region);
            }
          }
        } catch (Exception e) {
          stateMachine.transitionToFailed(e);
        }
        // The FragmentInstances has been dispatched successfully to corresponding host, we mark the
        stateMachine.transitionToRunning();
        LOGGER.info("{} transit to RUNNING", getLogHeader());
        instances.forEach(
            instance ->
                stateMachine.initialFragInstanceState(
                    instance.getId(), FragmentInstanceState.RUNNING));
        this.stateTracker.start();
        LOGGER.info("{} state tracker starts", getLogHeader());
        break;
      case WRITE:
        try {
          for (FragmentInstance fragmentInstance : instances) {
            PlanNode planNode = fragmentInstance.getFragment().getRoot();
            ConsensusGroupId groupId =
                ConsensusGroupId.Factory.createFromTConsensusGroupId(
                    fragmentInstance.getRegionReplicaSet().getRegionId());
            boolean hasFailedMeasurement = false;
            if (planNode instanceof InsertNode) {
              InsertNode insertNode = (InsertNode) planNode;
              SchemaValidator.validate(insertNode);
              hasFailedMeasurement = insertNode.hasFailedMeasurements();
              if (hasFailedMeasurement) {
                LOGGER.warn(
                    "Fail to insert measurements {} caused by {}",
                    insertNode.getFailedMeasurements(),
                    insertNode.getFailedMessages());
              }
            }

            TSStatus executionResult;

            if (groupId instanceof DataRegionId) {
              executionResult = STORAGE_ENGINE.write((DataRegionId) groupId, planNode);
            } else {
              executionResult = SCHEMA_ENGINE.write((SchemaRegionId) groupId, planNode);
            }

            // partial insert
            if (hasFailedMeasurement) {
              InsertNode node = (InsertNode) planNode;
              List<Exception> exceptions = node.getFailedExceptions();
              throw new WriteProcessException(
                  "failed to insert measurements "
                      + node.getFailedMeasurements()
                      + (!exceptions.isEmpty()
                          ? (" caused by " + exceptions.get(0).getMessage())
                          : ""));
            }

            if (executionResult.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              LOGGER.error("Execute write operation error: {}", executionResult.getMessage());
              stateMachine.transitionToFailed(executionResult);
              return;
            }
          }
          stateMachine.transitionToFinished();
        } catch (Exception e) {
          LOGGER.error("Execute write operation error ", e);
          stateMachine.transitionToFailed(e);
        }
    }
  }

  @Override
  public void stop() {
    // TODO: It seems that it is unnecessary to check whether they are null or not. Is it a best
    // practice ?
    stateTracker.abort();
    // TODO: (xingtanzjr) handle the exception when the termination cannot succeed
    for (FragmentInstance fragmentInstance : instances) {
      FragmentInstanceManager.getInstance().cancelTask(fragmentInstance.getId());
    }
  }

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

  private String getLogHeader() {
    return String.format("Query[%s]", queryContext.getQueryId());
  }
}
