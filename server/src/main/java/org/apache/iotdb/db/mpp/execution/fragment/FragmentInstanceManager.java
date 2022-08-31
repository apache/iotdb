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

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.execution.driver.DataDriver;
import org.apache.iotdb.db.mpp.execution.driver.SchemaDriver;
import org.apache.iotdb.db.mpp.execution.schedule.DriverScheduler;
import org.apache.iotdb.db.mpp.execution.schedule.IDriverScheduler;
import org.apache.iotdb.db.mpp.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;

import io.airlift.concurrent.SetThreadName;
import io.airlift.stats.CounterStat;
import io.airlift.units.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceExecution.createFragmentInstanceExecution;

public class FragmentInstanceManager {

  private static final Logger logger = LoggerFactory.getLogger(FragmentInstanceManager.class);

  private final Map<FragmentInstanceId, FragmentInstanceContext> instanceContext;
  private final Map<FragmentInstanceId, FragmentInstanceExecution> instanceExecution;
  private final LocalExecutionPlanner planner = LocalExecutionPlanner.getInstance();
  private final IDriverScheduler scheduler = DriverScheduler.getInstance();

  private final ScheduledExecutorService instanceManagementExecutor;
  private final ExecutorService instanceNotificationExecutor;

  private final Duration infoCacheTime;

  // record failed instances count
  private final CounterStat failedInstances = new CounterStat();

  private static final long QUERY_TIMEOUT_MS =
      IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold();

  public static FragmentInstanceManager getInstance() {
    return FragmentInstanceManager.InstanceHolder.INSTANCE;
  }

  private FragmentInstanceManager() {
    this.instanceContext = new ConcurrentHashMap<>();
    this.instanceExecution = new ConcurrentHashMap<>();
    this.instanceManagementExecutor =
        IoTDBThreadPoolFactory.newScheduledThreadPool(1, "instance-management");
    this.instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(4, "instance-notification");

    this.infoCacheTime = new Duration(15, TimeUnit.MINUTES);

    ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
        instanceManagementExecutor, this::removeOldInstances, 200, 200, TimeUnit.MILLISECONDS);
    ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
        instanceManagementExecutor,
        this::cancelTimeoutFlushingInstances,
        200,
        200,
        TimeUnit.MILLISECONDS);
  }

  public FragmentInstanceInfo execDataQueryFragmentInstance(
      FragmentInstance instance, DataRegion dataRegion) {

    FragmentInstanceId instanceId = instance.getId();
    try (SetThreadName fragmentInstanceName = new SetThreadName(instanceId.getFullId())) {
      FragmentInstanceExecution execution =
          instanceExecution.computeIfAbsent(
              instanceId,
              id -> {
                FragmentInstanceStateMachine stateMachine =
                    new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);

                FragmentInstanceContext context =
                    instanceContext.computeIfAbsent(
                        instanceId,
                        fragmentInstanceId ->
                            createFragmentInstanceContext(fragmentInstanceId, stateMachine));

                try {
                  DataDriver driver =
                      planner.plan(
                          instance.getFragment().getPlanNodeTree(),
                          instance.getFragment().getTypeProvider(),
                          context,
                          instance.getTimeFilter(),
                          dataRegion);
                  return createFragmentInstanceExecution(
                      scheduler,
                      instanceId,
                      context,
                      driver,
                      stateMachine,
                      failedInstances,
                      instance.getTimeOut());
                } catch (Throwable t) {
                  logger.error("error when create FragmentInstanceExecution.", t);
                  stateMachine.failed(t);
                  return null;
                }
              });

      return execution != null ? execution.getInstanceInfo() : createFailedInstanceInfo(instanceId);
    }
  }

  public FragmentInstanceInfo execSchemaQueryFragmentInstance(
      FragmentInstance instance, ISchemaRegion schemaRegion) {
    FragmentInstanceId instanceId = instance.getId();
    FragmentInstanceExecution execution =
        instanceExecution.computeIfAbsent(
            instanceId,
            id -> {
              FragmentInstanceStateMachine stateMachine =
                  new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);

              FragmentInstanceContext context =
                  instanceContext.computeIfAbsent(
                      instanceId,
                      fragmentInstanceId ->
                          createFragmentInstanceContext(fragmentInstanceId, stateMachine));

              try {
                SchemaDriver driver =
                    planner.plan(instance.getFragment().getPlanNodeTree(), context, schemaRegion);
                return createFragmentInstanceExecution(
                    scheduler,
                    instanceId,
                    context,
                    driver,
                    stateMachine,
                    failedInstances,
                    instance.getTimeOut());
              } catch (Throwable t) {
                logger.error("Execute error caused by ", t);
                stateMachine.failed(t);
                return null;
              }
            });
    return execution != null ? execution.getInstanceInfo() : createFailedInstanceInfo(instanceId);
  }

  /** Aborts a FragmentInstance. keep FragmentInstanceContext for later state tracking */
  public FragmentInstanceInfo abortFragmentInstance(FragmentInstanceId fragmentInstanceId) {
    instanceExecution.remove(fragmentInstanceId);
    FragmentInstanceContext context = instanceContext.get(fragmentInstanceId);
    if (context != null) {
      context.abort();
      return context.getInstanceInfo();
    }
    return null;
  }

  /** Cancels a FragmentInstance. */
  public FragmentInstanceInfo cancelTask(FragmentInstanceId instanceId) {
    logger.debug("cancelTask");
    requireNonNull(instanceId, "taskId is null");

    FragmentInstanceContext context = instanceContext.remove(instanceId);
    if (context != null) {
      instanceExecution.remove(instanceId);
      context.cancel();
      return context.getInstanceInfo();
    }
    return null;
  }

  /**
   * Gets the info for the specified fragment instance.
   *
   * <p>NOTE: this design assumes that only fragment instances that will eventually exist are
   * queried.
   */
  public FragmentInstanceInfo getInstanceInfo(FragmentInstanceId instanceId) {
    requireNonNull(instanceId, "instanceId is null");
    FragmentInstanceContext context = instanceContext.get(instanceId);
    if (context == null) {
      return null;
    }
    return context.getInstanceInfo();
  }

  public CounterStat getFailedInstances() {
    return failedInstances;
  }

  private FragmentInstanceInfo createFailedInstanceInfo(FragmentInstanceId instanceId) {
    FragmentInstanceContext context = instanceContext.get(instanceId);
    return new FragmentInstanceInfo(
        FragmentInstanceState.FAILED, context.getEndTime(), context.getFailedCause());
  }

  private void removeOldInstances() {
    long oldestAllowedInstance = System.currentTimeMillis() - infoCacheTime.toMillis();
    instanceContext
        .entrySet()
        .removeIf(
            entry -> {
              long endTime = entry.getValue().getEndTime();
              return endTime != -1 && endTime <= oldestAllowedInstance;
            });
  }

  private void cancelTimeoutFlushingInstances() {
    long now = System.currentTimeMillis();
    instanceContext.entrySet().stream()
        .filter(
            entry -> {
              FragmentInstanceContext context = entry.getValue();
              return context.getStateMachine().getState() == FragmentInstanceState.FLUSHING
                  && (now - context.getStartTime()) > QUERY_TIMEOUT_MS;
            })
        .forEach(entry -> entry.getValue().failed(new TimeoutException()));
  }

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static final FragmentInstanceManager INSTANCE = new FragmentInstanceManager();
  }
}
