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

package org.apache.iotdb.db.queryengine.execution.fragment;

import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.exception.CpuNotEnoughException;
import org.apache.iotdb.db.queryengine.exception.MemoryNotEnoughException;
import org.apache.iotdb.db.queryengine.execution.driver.IDriver;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.schedule.IDriverScheduler;
import org.apache.iotdb.db.utils.SetThreadName;

import io.airlift.stats.CounterStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceState.FAILED;

public class FragmentInstanceExecution {

  private static final Logger LOGGER = LoggerFactory.getLogger(FragmentInstanceExecution.class);
  private final FragmentInstanceId instanceId;
  private final FragmentInstanceContext context;

  // It will be set to null while this FI is FINISHED
  private List<IDriver> drivers;

  // It will be set to null while this FI is FINISHED
  private ISink sink;

  private final FragmentInstanceStateMachine stateMachine;

  private final long timeoutInMs;

  private final MPPDataExchangeManager exchangeManager;

  @SuppressWarnings("squid:S107")
  public static FragmentInstanceExecution createFragmentInstanceExecution(
      IDriverScheduler scheduler,
      FragmentInstanceId instanceId,
      FragmentInstanceContext context,
      List<IDriver> drivers,
      ISink sinkHandle,
      FragmentInstanceStateMachine stateMachine,
      CounterStat failedInstances,
      long timeOut,
      MPPDataExchangeManager exchangeManager)
      throws CpuNotEnoughException, MemoryNotEnoughException {
    FragmentInstanceExecution execution =
        new FragmentInstanceExecution(
            instanceId, context, drivers, sinkHandle, stateMachine, timeOut, exchangeManager);
    execution.initialize(failedInstances, scheduler);
    scheduler.submitDrivers(instanceId.getQueryId(), drivers, timeOut, context.getSessionInfo());
    return execution;
  }

  private FragmentInstanceExecution(
      FragmentInstanceId instanceId,
      FragmentInstanceContext context,
      List<IDriver> drivers,
      ISink sink,
      FragmentInstanceStateMachine stateMachine,
      long timeoutInMs,
      MPPDataExchangeManager exchangeManager) {
    this.instanceId = instanceId;
    this.context = context;
    this.drivers = drivers;
    this.sink = sink;
    this.stateMachine = stateMachine;
    this.timeoutInMs = timeoutInMs;
    this.exchangeManager = exchangeManager;
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

  public long getTimeoutInMs() {
    return timeoutInMs;
  }

  public FragmentInstanceStateMachine getStateMachine() {
    return stateMachine;
  }

  // this is a separate method to ensure that the `this` reference is not leaked during construction
  @SuppressWarnings("squid:S1181")
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

            clearShuffleSinkHandle(newState);

            // delete tmp file if exists
            deleteTmpFile();

            // close the driver after sink is aborted or closed because in driver.close() it
            // will try to call ISink.setNoMoreTsBlocks()
            for (IDriver driver : drivers) {
              driver.close();
            }
            // help for gc
            drivers = null;

            // release file handlers
            context.releaseResourceWhenAllDriversAreClosed();

            // release memory
            exchangeManager.deRegisterFragmentInstanceFromMemoryPool(
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

  private void clearShuffleSinkHandle(FragmentInstanceState newState) {
    if (newState.isFailed()) {
      sink.abort();
    } else {
      sink.close();
    }
    // help for gc
    sink = null;
  }

  private void deleteTmpFile() {
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
  }
}
