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

import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;

public class ConsensusSubscriptionPrefetchExecutorManager {

  private volatile ConsensusSubscriptionPrefetchExecutor executor;
  private volatile boolean started = false;

  private ConsensusSubscriptionPrefetchExecutorManager() {
    // singleton
  }

  public synchronized void start() {
    if (!SubscriptionConfig.getInstance().getSubscriptionEnabled()) {
      started = false;
      return;
    }
    started = true;
    if (executor == null || executor.isShutdown()) {
      executor = new ConsensusSubscriptionPrefetchExecutor();
    }
  }

  public synchronized ConsensusSubscriptionPrefetchExecutor getExecutor() {
    if (!started || !SubscriptionConfig.getInstance().getSubscriptionEnabled()) {
      return null;
    }
    if (executor == null || executor.isShutdown()) {
      executor = new ConsensusSubscriptionPrefetchExecutor();
    }
    return executor;
  }

  public synchronized void stop() {
    started = false;
    if (executor != null) {
      executor.shutdown();
      executor = null;
    }
  }

  public boolean isStarted() {
    return started;
  }

  private static class Holder {
    private static final ConsensusSubscriptionPrefetchExecutorManager INSTANCE =
        new ConsensusSubscriptionPrefetchExecutorManager();
  }

  public static ConsensusSubscriptionPrefetchExecutorManager getInstance() {
    return Holder.INSTANCE;
  }
}
