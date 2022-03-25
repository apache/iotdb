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
package org.apache.iotdb.db.mpp.schedule;

import org.apache.iotdb.db.mpp.schedule.task.FragmentInstanceTask;
import org.apache.iotdb.db.mpp.schedule.task.FragmentInstanceTaskStatus;

/** the scheduler interface of {@link FragmentInstanceTask} */
interface ITaskScheduler {

  /**
   * Switch a task from {@link FragmentInstanceTaskStatus#BLOCKED} to {@link
   * FragmentInstanceTaskStatus#READY}.
   *
   * @param task the task to be switched.
   */
  void blockedToReady(FragmentInstanceTask task);

  /**
   * Switch a task from {@link FragmentInstanceTaskStatus#READY} to {@link
   * FragmentInstanceTaskStatus#RUNNING}.
   *
   * @param task the task to be switched.
   * @return true if it's switched to the target status successfully, otherwise false.
   */
  boolean readyToRunning(FragmentInstanceTask task);

  /**
   * Switch a task from {@link FragmentInstanceTaskStatus#RUNNING} to {@link
   * FragmentInstanceTaskStatus#READY}.
   *
   * @param task the task to be switched.
   * @param context the execution context of last running.
   */
  void runningToReady(FragmentInstanceTask task, ExecutionContext context);

  /**
   * Switch a task from {@link FragmentInstanceTaskStatus#RUNNING} to {@link
   * FragmentInstanceTaskStatus#BLOCKED}.
   *
   * @param task the task to be switched.
   * @param context the execution context of last running.
   */
  void runningToBlocked(FragmentInstanceTask task, ExecutionContext context);

  /**
   * Switch a task from {@link FragmentInstanceTaskStatus#RUNNING} to {@link
   * FragmentInstanceTaskStatus#FINISHED}.
   *
   * @param task the task to be switched.
   * @param context the execution context of last running.
   */
  void runningToFinished(FragmentInstanceTask task, ExecutionContext context);

  /**
   * Switch a task to {@link FragmentInstanceTaskStatus#ABORTED}.
   *
   * @param task the task to be switched.
   */
  void toAborted(FragmentInstanceTask task);
}
