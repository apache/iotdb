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

package org.apache.iotdb.db.pipe.execution.executor;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.WrappedThreadPoolExecutor;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.task.callable.PipeConnectorSubtask;
import org.apache.iotdb.db.pipe.task.callable.PipeSubtask;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class PipeConnectorSubtaskExecutor implements PipeSubtaskExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConnectorSubtaskExecutor.class);
  private final WrappedThreadPoolExecutor connectorExecutorThreadPool;
  private final ListeningExecutorService executorService;
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static int MAX_THREAD_NUM = config.getPipeTaskExecutorMaxThreadNum();

  private ConcurrentHashMap<String, PipeConnectorSubtask> subtaskMap;

  public PipeConnectorSubtaskExecutor() {
    connectorExecutorThreadPool =
        (WrappedThreadPoolExecutor)
            IoTDBThreadPoolFactory.newFixedThreadPool(
                MAX_THREAD_NUM, ThreadName.PIPE_CONNECTOR_EXECUTOR_POOL.getName());
    executorService = MoreExecutors.listeningDecorator(connectorExecutorThreadPool);
    subtaskMap = new ConcurrentHashMap<>();
  }

  @Override
  public void submit(PipeSubtask subtask) {
    if (!subtaskMap.containsKey(subtask.getTaskID())) {
      LOGGER.warn("The subtask {} is not in the subtask map", subtask.getTaskID());
      return;
    }

    ListenableFuture<Void> nextFuture = executorService.submit(subtask);
    Futures.addCallback(nextFuture, subtask, executorService);
  }

  @Override
  public void stop() {
    if (executorService != null) {
      executorService.shutdown();
    }
  }

  @Override
  public void putSubtask(PipeSubtask subtask) {
    subtaskMap.put(subtask.getTaskID(), (PipeConnectorSubtask) subtask);
  }

  @Override
  public void removeSubtask(String taskID) {
    subtaskMap.remove(taskID);
  }

  @Override
  public boolean isSubtaskExist(String taskID) {
    return subtaskMap.containsKey(taskID);
  }

  @Override
  public void setExecutorThreadNum(int threadNum) {
    MAX_THREAD_NUM = threadNum;
    connectorExecutorThreadPool.setCorePoolSize(threadNum);
  }

  @Override
  public int getExecutorThreadNum() {
    return MAX_THREAD_NUM;
  }

  @TestOnly
  public ListeningExecutorService getExecutorService() {
    return executorService;
  }
}
