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
package org.apache.iotdb.db.mpp.execution.schedule;

import org.apache.iotdb.db.mpp.execution.schedule.task.DriverTask;
import org.apache.iotdb.db.mpp.execution.schedule.task.DriverTaskStatus;

/** the scheduler interface of {@link DriverTask} */
public interface ITaskScheduler {

  /**
   * Switch a task from {@link DriverTaskStatus#BLOCKED} to {@link DriverTaskStatus#READY}.
   *
   * @param task the task to be switched.
   */
  void blockedToReady(DriverTask task);

  /**
   * Switch a task from {@link DriverTaskStatus#READY} to {@link DriverTaskStatus#RUNNING}.
   *
   * @param task the task to be switched.
   * @return true if it's switched to the target status successfully, otherwise false.
   */
  boolean readyToRunning(DriverTask task);

  /**
   * Switch a task from {@link DriverTaskStatus#RUNNING} to {@link DriverTaskStatus#READY}.
   *
   * @param task the task to be switched.
   * @param context the execution context of last running.
   */
  void runningToReady(DriverTask task, ExecutionContext context);

  /**
   * Switch a task from {@link DriverTaskStatus#RUNNING} to {@link DriverTaskStatus#BLOCKED}.
   *
   * @param task the task to be switched.
   * @param context the execution context of last running.
   */
  void runningToBlocked(DriverTask task, ExecutionContext context);

  /**
   * Switch a task from {@link DriverTaskStatus#RUNNING} to {@link DriverTaskStatus#FINISHED}.
   *
   * @param task the task to be switched.
   * @param context the execution context of last running.
   */
  void runningToFinished(DriverTask task, ExecutionContext context);

  /**
   * Switch a task to {@link DriverTaskStatus#ABORTED}.
   *
   * @param task the task to be switched.
   */
  void toAborted(DriverTask task);

  /**
   * Switch a task to {@link DriverTaskStatus#ABORTED}.
   *
   * @param task the task to be switched.
   */
  void toAborted(DriverTask task, Throwable t);
}
