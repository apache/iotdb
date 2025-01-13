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

package org.apache.iotdb.db.pipe.agent.task.subtask.checker;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.pipe.agent.task.subtask.connector.UserDefinedConnectorSubtask;

import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PipeConnectorTimeOutChecker implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConnectorTimeOutChecker.class);

  private static final long PIPE_SUBTASK_EXECUTION_TIMEOUT_MS =
      CommonDescriptor.getInstance().getConfig().getPipeSubtaskExecutionTimeoutMs();

  private final Set<UserDefinedConnectorSubtask> userDefinedConnectorSubtasks =
      Collections.newSetFromMap(new ConcurrentHashMap<>());

  public void registerUserDefinedConnectorSubtask(
      UserDefinedConnectorSubtask userDefinedConnectorSubtask) {
    userDefinedConnectorSubtasks.add(userDefinedConnectorSubtask);
  }

  public void unregisterUserDefinedConnectorSubtask(
      UserDefinedConnectorSubtask userDefinedConnectorSubtask) {
    userDefinedConnectorSubtasks.remove(userDefinedConnectorSubtask);
  }

  @Override
  public void run() {
    while (true) {
      for (UserDefinedConnectorSubtask subtask : userDefinedConnectorSubtasks) {
        if (Objects.isNull(subtask)) {
          LOGGER.info("Subtask for worker {} is null Skipping.", subtask.getTaskID());
          continue;
        }
        synchronized (subtask) {
          if (!subtask.isScheduled()) {
            continue;
          }
          final long currTime = System.currentTimeMillis();
          if (currTime - subtask.getStartRunningTime() <= PIPE_SUBTASK_EXECUTION_TIMEOUT_MS) {
            continue;
          }
          ListenableFuture<Boolean> futures = subtask.getNextFuture();
          if (Objects.isNull(futures) || futures.isDone()) {
            continue;
          }
          // Interrupt a running thread
          futures.cancel(true);
          LOGGER.info("Interrupt {} connector subtask due to timeout.", subtask.getTaskID());
        }
      }
      try {
        Thread.sleep(PIPE_SUBTASK_EXECUTION_TIMEOUT_MS / 2);
      } catch (InterruptedException ignored) {
        LOGGER.info("time out check waiting to be interrupted");
      }
    }
  }
}
