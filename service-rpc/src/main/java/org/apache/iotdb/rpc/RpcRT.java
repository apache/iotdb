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
package org.apache.iotdb.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class RpcRT {

  private static final long RPC_RT_PRINT_INTERVAL_IN_MS = 5_000;

  private static final Logger RPC_RT_LOGGER = LoggerFactory.getLogger(RpcRT.class);

  private final AtomicLong totalTime;
  private final AtomicLong totalCount;

  private RpcRT() {
    this.totalTime = new AtomicLong(0);
    this.totalCount = new AtomicLong(0);

    ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);
    scheduledExecutor.scheduleAtFixedRate(
        this::printRpcRTStatistics,
        2 * RPC_RT_PRINT_INTERVAL_IN_MS,
        RPC_RT_PRINT_INTERVAL_IN_MS,
        TimeUnit.MILLISECONDS);
  }

  private void printRpcRTStatistics() {
    long totalTime = this.totalTime.get() / 1_000;
    long totalCount = this.totalCount.get();
    RPC_RT_LOGGER.info(
        "rpc total time: {}us, rpc total count: {}, rpc avg time: {}us",
        totalTime,
        totalCount,
        (totalTime / totalCount));
  }

  public void addCost(long costTimeInNanos) {
    totalTime.addAndGet(costTimeInNanos);
    totalCount.incrementAndGet();
  }

  public static RpcRT getInstance() {
    return RpcRTHolder.INSTANCE;
  }

  private static class RpcRTHolder {

    private static final RpcRT INSTANCE = new RpcRT();

    private RpcRTHolder() {}
  }
}
