/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.statistics;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class FixedScheduledOutputQueryPlanStatistics {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(FixedScheduledOutputQueryPlanStatistics.class);

  AtomicLong analyzeCost = new AtomicLong(0);
  AtomicLong fetchPartitionCost = new AtomicLong(0);
  AtomicLong fetchSchemaCost = new AtomicLong(0);
  AtomicLong logicalPlanCost = new AtomicLong(0);
  AtomicLong logicalOptimizationCost = new AtomicLong(0);
  AtomicLong distributionPlanCost = new AtomicLong(0);
  AtomicLong dispatchCost = new AtomicLong(0);
  AtomicLong num = new AtomicLong(0);

  public FixedScheduledOutputQueryPlanStatistics() {
    ScheduledExecutorService scheduledExecutor =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            "FixedScheduledOutputQueryPlanStatistics");
    ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
        scheduledExecutor, this::output, 0, 60_000, TimeUnit.MILLISECONDS);
  }

  private synchronized void output() {
    long count = num.get();
    if (count == 0) {
      return;
    }

    LOGGER.info(
        "\r\n======ScheduledOutputQueryPlanStatistics, num: {}, avg analyzeCost: {}, "
            + "avg fetchPartitionCost: {}, avg fetchSchemaCost: {}, avg logicalPlanCost: {}, "
            + "avg logicalOptimizationCost: {}, avg distributionPlanCost: {}, avg dispatchCost: {}",
        num.get(),
        format(analyzeCost, count),
        format(fetchPartitionCost, count),
        format(fetchPartitionCost, count),
        format(logicalPlanCost, count),
        format(logicalOptimizationCost, count),
        format(distributionPlanCost, count),
        format(dispatchCost, count));
    num.set(0);
    analyzeCost.set(0);
    fetchPartitionCost.set(0);
    fetchSchemaCost.set(0);
    logicalPlanCost.set(0);
    logicalOptimizationCost.set(0);
    distributionPlanCost.set(0);
    dispatchCost.set(0);
  }

  private String format(AtomicLong time, long count) {
    return time.get() / count + "ns";
  }

  public synchronized void recordCost(MPPQueryContext queryContext) {
    num.incrementAndGet();

    analyzeCost.getAndAdd(queryContext.getAnalyzeCost());
    fetchPartitionCost.getAndAdd(queryContext.getFetchPartitionCost());
    fetchSchemaCost.getAndAdd(queryContext.getFetchSchemaCost());
    logicalPlanCost.getAndAdd(queryContext.getLogicalPlanCost());
    logicalOptimizationCost.getAndAdd(queryContext.getLogicalOptimizationCost());
    distributionPlanCost.getAndAdd(queryContext.getDistributionPlanCost());
    dispatchCost.getAndAdd(queryContext.getDispatchCost());
  }
}
