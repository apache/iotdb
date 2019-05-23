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
package org.apache.iotdb.cluster.concurrent.pool;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.cluster.concurrent.ThreadName;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;

/**
 * Manage all query timer in query node, if timer is timeout, close all query resource for remote
 * coordinator node.
 */
public class QueryTimerThreadManager extends ThreadPoolManager {

  private static final String MANAGER_NAME = "remote-query-timer-thread-manager";

  private static final int CORE_POOL_SIZE = 1;

  @Override
  public void init() {
    pool = IoTDBThreadPoolFactory.newScheduledThreadPool(getThreadPoolSize(), getThreadName());
  }

  public static QueryTimerThreadManager getInstance() {
    return QueryTimerThreadManager.QueryTimerManagerHolder.INSTANCE;
  }

  @Override
  public String getManagerName() {
    return MANAGER_NAME;
  }

  @Override
  public String getThreadName() {
    return ThreadName.REMOTE_QUERY_TIMER.getName();
  }

  @Override
  public int getThreadPoolSize() {
    return CORE_POOL_SIZE;
  }

  public ScheduledFuture<?> execute(Runnable task, long delayMs) {
    checkInit();
    return ((ScheduledExecutorService) pool).schedule(task, delayMs, TimeUnit.MICROSECONDS);
  }

  private static class QueryTimerManagerHolder {

    private static final QueryTimerThreadManager INSTANCE = new QueryTimerThreadManager();

    private QueryTimerManagerHolder() {

    }
  }
}
