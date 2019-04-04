/**
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
package org.apache.iotdb.cluster.callback;

import java.util.Map;

public abstract class MultiQPTask extends QPTask {

  /**
   * Each request is corresponding to a group id. String: group id
   */
  Map<String, QPTask> taskMap;

  /**
   * Task thread map
   */
  Map<String, Thread> taskThreadMap;

  public MultiQPTask(boolean isSyncTask, int taskNum, TaskState taskState, TaskType taskType) {
    super(isSyncTask, taskNum, TaskState.INITIAL, taskType);
  }

  @Override
  public void shutdown() {
    for (Thread taskThread : taskThreadMap.values()) {
      if (taskThread.isAlive() && !taskThread.isInterrupted()) {
        taskThread.interrupt();
      }
    }
    this.taskCountDownLatch.countDown();
  }
}
