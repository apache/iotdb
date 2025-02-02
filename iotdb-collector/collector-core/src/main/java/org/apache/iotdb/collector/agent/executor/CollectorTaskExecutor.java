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

package org.apache.iotdb.collector.agent.executor;

import org.apache.iotdb.collector.agent.task.CollectorTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

public abstract class CollectorTaskExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(CollectorTaskExecutor.class);

  public void register(final CollectorTask collectorTask) {
    if (validateIfAbsent(collectorTask.getTaskId())) {
      getExecutor(collectorTask.getTaskId())
          .ifPresent(
              executor -> {
                executor.submit(collectorTask);
                recordExecution(collectorTask, executor);
              });
    } else {
      LOGGER.warn("task {} has existed", collectorTask.getTaskId());
    }
  }

  public abstract boolean validateIfAbsent(final String taskId);

  public abstract Optional<ExecutorService> getExecutor(final String taskId);

  public abstract void recordExecution(
      final CollectorTask collectorTask, final ExecutorService executorService);

  public void deregister(final String taskId) {
    if (!validateIfAbsent(taskId)) {
      eraseExecution(taskId);
    } else {
      LOGGER.warn("task {} has not existed", taskId);
    }
  }

  public abstract void eraseExecution(final String taskId);
}
