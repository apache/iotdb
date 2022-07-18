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
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.db.mpp.execution.QueryStateMachine;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceState;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;

import io.airlift.concurrent.SetThreadName;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class FixedRateFragInsStateTracker extends AbstractFragInsStateTracker {

  private static final Logger logger = LoggerFactory.getLogger(FixedRateFragInsStateTracker.class);

  private static final long SAME_STATE_PRINT_RATE_IN_MS = 10 * 60 * 1000;

  // TODO: (xingtanzjr) consider how much Interval is OK for state tracker
  private static final long STATE_FETCH_INTERVAL_IN_MS = 500;
  private ScheduledFuture<?> trackTask;
  private volatile FragmentInstanceState lastState;
  private volatile long durationToLastPrintInMS;

  public FixedRateFragInsStateTracker(
      QueryStateMachine stateMachine,
      ExecutorService executor,
      ScheduledExecutorService scheduledExecutor,
      List<FragmentInstance> instances,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> internalServiceClientManager) {
    super(stateMachine, executor, scheduledExecutor, instances, internalServiceClientManager);
  }

  @Override
  public void start() {
    trackTask =
        ScheduledExecutorUtil.safelyScheduleAtFixedRate(
            scheduledExecutor,
            this::fetchStateAndUpdate,
            0,
            STATE_FETCH_INTERVAL_IN_MS,
            TimeUnit.MILLISECONDS);
  }

  @Override
  public void abort() {
    logger.info("start to abort state tracker");
    if (trackTask != null) {
      logger.info("start to cancel fixed rate tracking task");
      boolean cancelResult = trackTask.cancel(true);
      if (!cancelResult) {
        logger.error("cancel state tracking task failed. {}", trackTask.isCancelled());
      } else {
        logger.info("cancellation succeeds");
      }
    }
  }

  private void fetchStateAndUpdate() {
    for (FragmentInstance instance : instances) {
      try (SetThreadName threadName = new SetThreadName(instance.getId().getFullId())) {
        FragmentInstanceState state = fetchState(instance);
        if (needPrintState(lastState, state, durationToLastPrintInMS)) {
          logger.info("State is {}", state);
          lastState = state;
          durationToLastPrintInMS = 0;
        } else {
          durationToLastPrintInMS += STATE_FETCH_INTERVAL_IN_MS;
        }

        if (state != null) {
          stateMachine.updateFragInstanceState(instance.getId(), state);
        }
      } catch (TException | IOException e) {
        // TODO: do nothing ?
        logger.error("error happened while fetching query state", e);
      }
    }
  }

  private boolean needPrintState(
      FragmentInstanceState previous, FragmentInstanceState current, long durationToLastPrintInMS) {
    if (current != previous) {
      return true;
    }
    return durationToLastPrintInMS >= SAME_STATE_PRINT_RATE_IN_MS;
  }
}
