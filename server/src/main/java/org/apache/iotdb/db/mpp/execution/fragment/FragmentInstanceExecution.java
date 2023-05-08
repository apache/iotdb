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

import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.exception.CpuNotEnoughException;
import org.apache.iotdb.db.mpp.exception.MemoryNotEnoughException;
import org.apache.iotdb.db.mpp.execution.driver.IDriver;
import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.mpp.execution.exchange.sink.ISink;
import org.apache.iotdb.db.mpp.execution.schedule.IDriverScheduler;
import org.apache.iotdb.db.utils.SetThreadName;

import io.airlift.stats.CounterStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceState.FAILED;

public class FragmentInstanceExecution {

  private static final Logger LOGGER = LoggerFactory.getLogger(FragmentInstanceExecution.class);
  private final FragmentInstanceId instanceId;
  private final FragmentInstanceContext context;

  // it will be set to null while this FI is FINISHED
  private List<IDriver> drivers;

  // it will be set to null while this FI is FINISHED
  private ISink sink;

  private final FragmentInstanceStateMachine stateMachine;

  private long lastHeartbeat;

  public static FragmentInstanceExecution createFragmentInstanceExecution(
      IDriverScheduler scheduler,
      FragmentInstanceId instanceId,
      FragmentInstanceContext context,
      List<IDriver> drivers,
      ISink sinkHandle,
      FragmentInstanceStateMachine stateMachine,
      CounterStat failedInstances,
      long timeOut)
      throws CpuNotEnoughException, MemoryNotEnoughException {
    FragmentInstanceExecution execution =
        new FragmentInstanceExecution(instanceId, context, drivers, sinkHandle, stateMachine);
    execution.initialize(failedInstances, scheduler);
    LOGGER.debug("timeout is {}ms.", timeOut);
    scheduler.submitDrivers(instanceId.getQueryId(), drivers, timeOut, context.getSessionInfo());
    return execution;
  }

  private FragmentInstanceExecution(
      FragmentInstanceId instanceId,
      FragmentInstanceContext context,
      List<IDriver> drivers,
      ISink sink,
      FragmentInstanceStateMachine stateMachine) {
    this.instanceId = instanceId;
    this.context = context;
    this.drivers = drivers;
    this.sink = sink;
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
        stateMachine.getState(),
        context.getEndTime(),
        context.getFailedCause(),
        context.getFailureInfoList());
  }

  public long getStartTime() {
    return context.getStartTime();
  }

  public FragmentInstanceStateMachine getStateMachine() {
    return stateMachine;
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

            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("Enter the stateChangeListener");
            }

            // Update failed tasks counter
            if (newState == FAILED) {
              failedInstances.update(1);
            }

            if (newState.isFailed()) {
              sink.abort();
            } else {
              sink.close();
            }
            // help for gc
            sink = null;

            // delete tmp file if exists
            if (context.mayHaveTmpFile()) {
              String tmpFilePath =
                  IoTDBDescriptor.getInstance().getConfig().getSortTmpDir()
                      + File.separator
                      + context.getId().getFullId()
                      + File.separator;
              File tmpFile = new File(tmpFilePath);
              if (tmpFile.exists()) {
                FileUtils.deleteDirectory(tmpFile);
              }
            }

            // close the driver after sink is aborted or closed because in driver.close() it
            // will try to call ISink.setNoMoreTsBlocks()
            for (IDriver driver : drivers) {
              driver.close();
            }
            context.releaseResource();
            // help for gc
            drivers = null;
            MPPDataExchangeService.getInstance()
                .getMPPDataExchangeManager()
                .deRegisterFragmentInstanceFromMemoryPool(
                    instanceId.getQueryId().getId(), instanceId.getFragmentInstanceId());
            if (newState.isFailed()) {
              scheduler.abortFragmentInstance(instanceId);
            }
          } catch (Throwable t) {
            try (SetThreadName threadName = new SetThreadName(instanceId.getFullId())) {
              LOGGER.error(
                  "Errors happened while trying to finish FI, resource may already leak!", t);
            }
          }
        });
  }
}
