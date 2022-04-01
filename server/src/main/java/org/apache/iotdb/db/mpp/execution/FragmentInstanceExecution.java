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

import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.schedule.IFragmentInstanceScheduler;

import com.google.common.collect.ImmutableList;

import static java.util.Objects.requireNonNull;

public class FragmentInstanceExecution {

  private final IFragmentInstanceScheduler scheduler;

  private final FragmentInstanceId instanceId;
  private final FragmentInstanceContext context;

  private final ExecFragmentInstance driver;

  private FragmentInstanceState state;

  private long lastHeartbeat;

  public FragmentInstanceExecution(
      IFragmentInstanceScheduler scheduler,
      FragmentInstanceId instanceId,
      FragmentInstanceContext context,
      ExecFragmentInstance driver) {
    this.scheduler = scheduler;
    this.instanceId = instanceId;
    this.context = context;
    this.driver = driver;
    scheduler.submitFragmentInstances(instanceId.getQueryId(), ImmutableList.of(driver));
  }

  public void recordHeartbeat() {
    lastHeartbeat = System.currentTimeMillis();
  }

  public void setLastHeartbeat(long lastHeartbeat) {
    this.lastHeartbeat = lastHeartbeat;
  }

  public FragmentInstanceState getInstanceState() {
    return state;
  }

  public void setState(FragmentInstanceState state) {
    this.state = state;
  }

  public FragmentInstanceInfo getInstanceInfo() {
    return new FragmentInstanceInfo(state);
  }

  public void failed(Throwable cause) {
    requireNonNull(cause, "cause is null");
    // TODO
  }

  public void cancel() {
    // TODO
  }

  public void abort() {
    // TODO
  }
}
