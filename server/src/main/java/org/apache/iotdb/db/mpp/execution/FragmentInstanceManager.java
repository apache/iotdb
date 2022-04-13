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
package org.apache.iotdb.db.mpp.execution;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.schedule.FragmentInstanceScheduler;
import org.apache.iotdb.db.mpp.schedule.IFragmentInstanceScheduler;
import org.apache.iotdb.db.mpp.sql.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.mpp.sql.planner.plan.FragmentInstance;

import io.airlift.units.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public class FragmentInstanceManager {

  private static final Logger logger = LoggerFactory.getLogger(FragmentInstanceManager.class);

  private final Map<FragmentInstanceId, FragmentInstanceContext> instanceContext;
  private final Map<FragmentInstanceId, FragmentInstanceExecution> instanceExecution;
  private final LocalExecutionPlanner planner = LocalExecutionPlanner.getInstance();
  private final IFragmentInstanceScheduler scheduler = FragmentInstanceScheduler.getInstance();

  private final ScheduledExecutorService instanceManagementExecutor;

  private final Duration infoCacheTime;

  public static FragmentInstanceManager getInstance() {
    return FragmentInstanceManager.InstanceHolder.INSTANCE;
  }

  private FragmentInstanceManager() {
    this.instanceContext = new ConcurrentHashMap<>();
    this.instanceExecution = new ConcurrentHashMap<>();
    this.instanceManagementExecutor =
        IoTDBThreadPoolFactory.newScheduledThreadPool(5, "instance-management");

    this.infoCacheTime = new Duration(15, TimeUnit.MINUTES);

    instanceManagementExecutor.scheduleWithFixedDelay(
        () -> {
          try {
            removeOldTasks();
          } catch (Throwable e) {
            logger.warn("Error removing old tasks", e);
          }
        },
        200,
        200,
        TimeUnit.MILLISECONDS);
  }

  public FragmentInstanceInfo execDataQueryFragmentInstance(
      FragmentInstance instance, DataRegion dataRegion) {
    FragmentInstanceId instanceId = instance.getId();

    FragmentInstanceExecution execution =
        instanceExecution.computeIfAbsent(
            instanceId,
            id -> {
              AtomicReference<FragmentInstanceState> state = new AtomicReference<>();
              state.set(FragmentInstanceState.PLANNED);

              FragmentInstanceContext context =
                  instanceContext.computeIfAbsent(
                      instanceId,
                      fragmentInstanceId -> new FragmentInstanceContext(fragmentInstanceId, state));

              try {
                DataDriver driver =
                    planner.plan(
                        instance.getFragment().getRoot(),
                        context,
                        instance.getTimeFilter(),
                        dataRegion);
                return new FragmentInstanceExecution(scheduler, instanceId, context, driver, state);
              } catch (Throwable t) {
                context.failed(t);
                return null;
              }
            });

    return execution != null ? execution.getInstanceInfo() : createFailedInstanceInfo(instanceId);
  }

  public FragmentInstanceInfo execSchemaQueryFragmentInstance(
      FragmentInstance instance, ISchemaRegion schemaRegion) {
    FragmentInstanceId instanceId = instance.getId();

    FragmentInstanceExecution execution =
        instanceExecution.computeIfAbsent(
            instanceId,
            id -> {
              AtomicReference<FragmentInstanceState> state = new AtomicReference<>();
              state.set(FragmentInstanceState.PLANNED);

              FragmentInstanceContext context =
                  instanceContext.computeIfAbsent(
                      instanceId,
                      fragmentInstanceId -> new FragmentInstanceContext(fragmentInstanceId, state));

              try {
                SchemaDriver driver =
                    planner.plan(instance.getFragment().getRoot(), context, schemaRegion);
                return new FragmentInstanceExecution(scheduler, instanceId, context, driver, state);
              } catch (Throwable t) {
                context.failed(t);
                return null;
              }
            });
    return execution != null ? execution.getInstanceInfo() : createFailedInstanceInfo(instanceId);
  }

  public FragmentInstanceInfo abortFragmentInstance(FragmentInstanceId fragmentInstanceId) {
    FragmentInstanceExecution execution = instanceExecution.remove(fragmentInstanceId);
    if (execution != null) {
      instanceContext.remove(fragmentInstanceId);
      execution.abort();
      return execution.getInstanceInfo();
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
    FragmentInstanceExecution execution = instanceExecution.get(instanceId);
    if (execution == null) {
      return null;
    }
    return execution.getInstanceInfo();
  }

  private FragmentInstanceInfo createFailedInstanceInfo(FragmentInstanceId instanceId) {
    return new FragmentInstanceInfo(
        FragmentInstanceState.FAILED, instanceContext.get(instanceId).getEndTime());
  }

  private void removeOldTasks() {
    long oldestAllowedInstance = System.currentTimeMillis() - infoCacheTime.toMillis();
    instanceContext
        .entrySet()
        .removeIf(
            entry -> {
              FragmentInstanceId instanceId = entry.getKey();
              FragmentInstanceExecution execution = instanceExecution.get(instanceId);
              if (execution == null) {
                return true;
              }
              long endTime = execution.getInstanceInfo().getEndTime();
              if (endTime != -1 && endTime <= oldestAllowedInstance) {
                instanceContext.remove(instanceId);
                return true;
              } else {
                return false;
              }
            });
  }

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static final FragmentInstanceManager INSTANCE = new FragmentInstanceManager();
  }
}
