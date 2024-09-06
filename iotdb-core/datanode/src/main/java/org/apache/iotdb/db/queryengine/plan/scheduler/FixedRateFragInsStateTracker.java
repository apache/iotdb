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

package org.apache.iotdb.db.queryengine.plan.scheduler;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceInfo;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceState;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.utils.SetThreadName;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class FixedRateFragInsStateTracker extends AbstractFragInsStateTracker {

  private static final Logger logger = LoggerFactory.getLogger(FixedRateFragInsStateTracker.class);

  private static final long SAME_STATE_PRINT_RATE_IN_MS = 10L * 60 * 1000;

  // TODO: (xingtanzjr) consider how much Interval is OK for state tracker
  private static final long STATE_FETCH_INTERVAL_IN_MS = 500;
  private ScheduledFuture<?> trackTask;
  private final Map<FragmentInstanceId, InstanceStateMetrics> instanceStateMap;
  private volatile boolean aborted;

  public FixedRateFragInsStateTracker(
      QueryStateMachine stateMachine,
      ScheduledExecutorService scheduledExecutor,
      List<FragmentInstance> instances,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> internalServiceClientManager) {
    super(stateMachine, scheduledExecutor, instances, internalServiceClientManager);
    this.aborted = false;
    this.instanceStateMap = new HashMap<>();
  }

  @Override
  public synchronized void start() {
    if (aborted) {
      return;
    }
    trackTask =
        ScheduledExecutorUtil.safelyScheduleAtFixedRate(
            scheduledExecutor,
            this::fetchStateAndUpdate,
            0,
            STATE_FETCH_INTERVAL_IN_MS,
            TimeUnit.MILLISECONDS);
  }

  @Override
  public synchronized List<FragmentInstanceId> filterUnFinishedFIs(
      List<FragmentInstanceId> instanceIds) {
    List<FragmentInstanceId> res = new ArrayList<>();
    if (instanceIds == null) {
      return res;
    }
    for (FragmentInstanceId fragmentInstanceId : instanceIds) {
      if (unfinished(fragmentInstanceId)) {
        // FI whose state has not been updated is considered to be unfinished.(In Query with limit
        // clause, it's possible that the query is finished before the state of FI being recorded.)
        res.add(fragmentInstanceId);
      }
    }
    return res;
  }

  private boolean unfinished(FragmentInstanceId fragmentInstanceId) {
    InstanceStateMetrics stateMetrics = instanceStateMap.get(fragmentInstanceId);
    // FI whose state has not been updated is considered to be unfinished.(In Query with limit
    // clause, it's possible that the query is finished before the state of FI being recorded.)
    return stateMetrics == null
        || stateMetrics.lastState == null
        || !stateMetrics.lastState.isDone();
  }

  @Override
  public synchronized void abort() {
    aborted = true;
    if (trackTask != null) {
      boolean cancelResult = trackTask.cancel(true);
      // TODO: (xingtanzjr) a strange case here is that sometimes
      // the cancelResult is false but the trackTask is definitely cancelled
      if (!cancelResult) {
        logger.debug("cancel state tracking task failed. {}", trackTask.isCancelled());
      }
    } else {
      logger.debug("trackTask not started");
    }
  }

  private void fetchStateAndUpdate() {
    for (FragmentInstance instance : instances) {
      if (unfinished(instance.getId())) {
        try (SetThreadName threadName = new SetThreadName(instance.getId().getFullId())) {
          FragmentInstanceInfo instanceInfo = fetchInstanceInfo(instance);
          synchronized (this) {
            InstanceStateMetrics metrics =
                instanceStateMap.computeIfAbsent(
                    instance.getId(), k -> new InstanceStateMetrics(instance.isRoot()));
            if (needPrintState(
                metrics.lastState, instanceInfo.getState(), metrics.durationToLastPrintInMS)) {
              if (logger.isDebugEnabled()) {
                logger.debug("[PrintFIState] state is {}", instanceInfo.getState());
              }
              metrics.reset(instanceInfo.getState());
            } else {
              metrics.addDuration(STATE_FETCH_INTERVAL_IN_MS);
            }

            updateQueryState(instance.getId(), instanceInfo);
          }
        } catch (ClientManagerException | TException e) {
          // TODO: do nothing ?
          logger.warn("error happened while fetching query state", e);
        }
      }
    }
  }

  private void updateQueryState(FragmentInstanceId instanceId, FragmentInstanceInfo instanceInfo) {
    // no such instance may be caused by DN restarting
    if (instanceInfo.getState() == FragmentInstanceState.NO_SUCH_INSTANCE) {
      stateMachine.transitionToFailed(
          new RuntimeException(
              String.format(
                  "FragmentInstance[%s] is failed. %s, may be caused by DN restarting.",
                  instanceId, instanceInfo.getMessage())));
    }
    if (instanceInfo.getState().isFailed()) {
      if (instanceInfo.getFailureInfoList() == null
          || instanceInfo.getFailureInfoList().isEmpty()) {
        stateMachine.transitionToFailed(
            new RuntimeException(
                String.format(
                    "FragmentInstance[%s] is failed. %s", instanceId, instanceInfo.getMessage())));
      } else if (instanceInfo.getErrorCode().isPresent()) {
        stateMachine.transitionToFailed(
            new IoTDBException(
                instanceInfo.getErrorCode().get().getMessage(),
                instanceInfo.getErrorCode().get().getCode()));
      } else {
        stateMachine.transitionToFailed(instanceInfo.getFailureInfoList().get(0).toException());
      }
    }
    boolean queryFinished = false;
    List<InstanceStateMetrics> rootInstanceStateMetricsList =
        instanceStateMap.values().stream()
            .filter(instanceStateMetrics -> instanceStateMetrics.isRootInstance)
            .collect(Collectors.toList());
    if (!rootInstanceStateMetricsList.isEmpty()) {
      queryFinished =
          rootInstanceStateMetricsList.stream()
              .allMatch(
                  instanceStateMetrics ->
                      instanceStateMetrics.lastState == FragmentInstanceState.FINISHED);
    }

    if (queryFinished) {
      stateMachine.transitionToFinished();
    }
  }

  private boolean needPrintState(
      FragmentInstanceState previous, FragmentInstanceState current, long durationToLastPrintInMS) {
    if (current != previous) {
      return true;
    }
    return durationToLastPrintInMS >= SAME_STATE_PRINT_RATE_IN_MS;
  }

  private static class InstanceStateMetrics {
    private final boolean isRootInstance;
    private FragmentInstanceState lastState;
    private long durationToLastPrintInMS;

    private InstanceStateMetrics(boolean isRootInstance) {
      this.isRootInstance = isRootInstance;
      this.lastState = null;
      this.durationToLastPrintInMS = 0L;
    }

    private void reset(FragmentInstanceState newState) {
      this.lastState = newState;
      this.durationToLastPrintInMS = 0L;
    }

    private void addDuration(long duration) {
      durationToLastPrintInMS += duration;
    }
  }
}
