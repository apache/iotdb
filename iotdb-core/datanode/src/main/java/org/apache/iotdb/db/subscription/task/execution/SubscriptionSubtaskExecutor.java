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

package org.apache.iotdb.db.subscription.task.execution;

import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.pipe.agent.task.execution.PipeSubtaskExecutor;
import org.apache.iotdb.commons.pipe.agent.task.execution.PipeSubtaskScheduler;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.pipe.agent.task.execution.PipeConnectorSubtaskExecutor;
import org.apache.iotdb.db.subscription.task.subtask.SubscriptionReceiverSubtask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

public class SubscriptionSubtaskExecutor extends PipeConnectorSubtaskExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionSubtaskExecutor.class);

  private final AtomicLong submittedReceiverSubtasks = new AtomicLong(0);

  public SubscriptionSubtaskExecutor() {
    super(
        SubscriptionConfig.getInstance().getSubscriptionSubtaskExecutorMaxThreadNum(),
        ThreadName.SUBSCRIPTION_EXECUTOR_POOL);
  }

  @Override
  protected PipeSubtaskScheduler schedulerSupplier(final PipeSubtaskExecutor executor) {
    return new SubscriptionSubtaskScheduler((SubscriptionSubtaskExecutor) executor);
  }

  public void executeReceiverSubtask(
      final SubscriptionReceiverSubtask subtask, final long timeoutMs) throws Exception {
    if (!super.hasAvailableThread()) {
      subtask.call(); // non-strict timeout
      return;
    }

    submittedReceiverSubtasks.incrementAndGet();
    try {
      final Future<Void> future = subtaskWorkerThreadPoolExecutor.submit(subtask);
      try {
        future.get(timeoutMs, TimeUnit.MILLISECONDS); // strict timeout
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt(); // restore interrupted state
        future.cancel(true);
        throw e;
      } catch (final ExecutionException | TimeoutException e) {
        future.cancel(true);
        throw e;
      }
    } finally {
      submittedReceiverSubtasks.decrementAndGet();
    }
  }

  public boolean hasSubmittedReceiverSubtasks() {
    return submittedReceiverSubtasks.get() > 0;
  }
}
