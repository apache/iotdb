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

import org.apache.iotdb.db.mpp.execution.FragmentInstanceState;
import org.apache.iotdb.db.mpp.execution.QueryStateMachine;
import org.apache.iotdb.db.mpp.sql.planner.plan.FragmentInstance;

import org.apache.thrift.TException;

import java.util.List;
import java.util.concurrent.*;

public class FixedRateFragInsStateTracker extends AbstractFragInsStateTracker {
  // TODO: (xingtanzjr) consider how much Interval is OK for state tracker
  private static final long STATE_FETCH_INTERVAL_IN_MS = 500;
  private ScheduledFuture<?> trackTask;

  public FixedRateFragInsStateTracker(
      QueryStateMachine stateMachine,
      ExecutorService executor,
      ScheduledExecutorService scheduledExecutor,
      List<FragmentInstance> instances) {
    super(stateMachine, executor, scheduledExecutor, instances);
  }

  @Override
  public void start() {
    trackTask =
        scheduledExecutor.scheduleAtFixedRate(
            this::fetchStateAndUpdate, 0, STATE_FETCH_INTERVAL_IN_MS, TimeUnit.MILLISECONDS);
  }

  @Override
  public void abort() {
    if (trackTask != null) {
      trackTask.cancel(true);
    }
  }

  private void fetchStateAndUpdate() {
    for (FragmentInstance instance : instances) {
      try {
        FragmentInstanceState state = fetchState(instance);
        if (state != null) {
          stateMachine.updateFragInstanceState(instance.getId(), state);
        }
      } catch (TException e) {
        // TODO: do nothing ?
      }
    }
  }
}
