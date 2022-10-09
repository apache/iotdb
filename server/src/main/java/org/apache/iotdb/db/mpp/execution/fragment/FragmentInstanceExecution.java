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
package org.apache.iotdb.db.mpp.execution.fragment;

import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.execution.driver.IDriver;
import org.apache.iotdb.db.mpp.execution.exchange.ISinkHandle;
import org.apache.iotdb.db.mpp.execution.schedule.DriverTaskTimeoutSentinelThread;
import org.apache.iotdb.db.mpp.execution.schedule.IDriverScheduler;
import org.apache.iotdb.db.utils.SetThreadName;

import com.google.common.collect.ImmutableList;
import io.airlift.stats.CounterStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceState.FAILED;

public class FragmentInstanceExecution {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FragmentInstanceExecution.class);
  private final FragmentInstanceId instanceId;
  private final FragmentInstanceContext context;

  // it will be set to null while this FI is FINISHED
  private IDriver driver;

  // it will be set to null while this FI is FINISHED
  private ISinkHandle sinkHandle;

  private final FragmentInstanceStateMachine stateMachine;

  private long lastHeartbeat;

  public static FragmentInstanceExecution createFragmentInstanceExecution(
      IDriverScheduler scheduler,
      FragmentInstanceId instanceId,
      FragmentInstanceContext context,
      IDriver driver,
      FragmentInstanceStateMachine stateMachine,
      CounterStat failedInstances,
      long timeOut) {
    FragmentInstanceExecution execution =
        new FragmentInstanceExecution(instanceId, context, driver, stateMachine);
    execution.initialize(failedInstances, scheduler);
    LOGGER.info("timeout is {}ms.", timeOut);
    scheduler.submitDrivers(instanceId.getQueryId(), ImmutableList.of(driver), timeOut);
    return execution;
  }

  private FragmentInstanceExecution(
      FragmentInstanceId instanceId,
      FragmentInstanceContext context,
      IDriver driver,
      FragmentInstanceStateMachine stateMachine) {
    this.instanceId = instanceId;
    this.context = context;
    this.driver = driver;
    this.sinkHandle = driver.getSinkHandle();
    this.stateMachine = stateMachine;
  }

  public void recordHeartbeat() {
    lastHeartbeat = System.currentTimeMillis();
  }

  public void setLastHeartbeat(long lastHeartbeat) {
    this.lastHeartbeat = lastHeartbeat;
  }

  public FragmentInstanceState getInstanceState() {
    return stateMachine.getState();
  }

  public FragmentInstanceInfo getInstanceInfo() {
    return new FragmentInstanceInfo(
        stateMachine.getState(), context.getEndTime(), context.getFailedCause());
  }

  // this is a separate method to ensure that the `this` reference is not leaked during construction
  private void initialize(CounterStat failedInstances, IDriverScheduler scheduler) {
    requireNonNull(failedInstances, "failedInstances is null");
    stateMachine.addStateChangeListener(
        newState -> {
          try (SetThreadName threadName = new SetThreadName(instanceId.getFullId())) {
            if (!newState.isDone()) {
              return;
            }

            // Update failed tasks counter
            if (newState == FAILED) {
              failedInstances.update(1);
            }

            if (newState.isFailed()) {
              sinkHandle.abort();
            } else {
              sinkHandle.close();
            }
            // help for gc
            sinkHandle = null;
            // close the driver after sinkHandle is aborted or closed because in driver.close() it
            // will try to call ISinkHandle.setNoMoreTsBlocks()
            driver.close();
            // help for gc
            driver = null;
            if (newState.isFailed()) {
              scheduler.abortFragmentInstance(instanceId);
            }
          }
        });
  }
}
