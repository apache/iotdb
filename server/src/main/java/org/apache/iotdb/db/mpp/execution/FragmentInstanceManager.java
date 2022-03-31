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

import org.apache.iotdb.db.engine.storagegroup.VirtualStorageGroupProcessor;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.schedule.FragmentInstanceScheduler;
import org.apache.iotdb.db.mpp.schedule.IFragmentInstanceScheduler;
import org.apache.iotdb.db.mpp.sql.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.mpp.sql.planner.plan.FragmentInstance;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

public class FragmentInstanceManager {

  private final Map<FragmentInstanceId, FragmentInstanceContext> instanceContext;
  private final Map<FragmentInstanceId, FragmentInstanceExecution> instanceExecution;
  private final LocalExecutionPlanner planner = LocalExecutionPlanner.getInstance();
  private final IFragmentInstanceScheduler scheduler = FragmentInstanceScheduler.getInstance();

  public static FragmentInstanceManager getInstance() {
    return FragmentInstanceManager.InstanceHolder.INSTANCE;
  }

  private FragmentInstanceManager() {
    this.instanceContext = new ConcurrentHashMap<>();
    this.instanceExecution = new ConcurrentHashMap<>();
  }

  public FragmentInstanceInfo execDataQueryFragmentInstance(
      FragmentInstance instance, VirtualStorageGroupProcessor dataRegion) {
    FragmentInstanceId instanceId = instance.getId();

    FragmentInstanceExecution execution =
        instanceExecution.computeIfAbsent(
            instanceId,
            id -> {
              FragmentInstanceContext context =
                  instanceContext.computeIfAbsent(instanceId, FragmentInstanceContext::new);

              Driver driver =
                  planner.plan(
                      instance.getFragment().getRoot(),
                      context,
                      instance.getTimeFilter(),
                      dataRegion);
              return new FragmentInstanceExecution(scheduler, instanceId, context, driver);
            });

    return execution.getInstanceInfo();
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

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static final FragmentInstanceManager INSTANCE = new FragmentInstanceManager();
  }
}
