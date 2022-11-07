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
package org.apache.iotdb.db.mpp.statistics;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.IoTDBConstant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
public class QueryStatistics {

  private static final long QUERY_STATISTICS_PRINT_INTERVAL_IN_MS = 10_000;

  private static final Logger QUERY_STATISTICS_LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.QUERY_STATISTICS_LOGGER_NAME);

  private final AtomicBoolean tracing = new AtomicBoolean(false);

  private final Map<String, OperationStatistic> operationStatistics = new ConcurrentHashMap<>();

  private QueryStatistics() {
    ScheduledExecutorService scheduledExecutor =
        IoTDBThreadPoolFactory.newScheduledThreadPool(1, "Query-Statistics-Print");
    ScheduledExecutorUtil.safelyScheduleAtFixedRate(
        scheduledExecutor,
        this::printQueryStatistics,
        0,
        QUERY_STATISTICS_PRINT_INTERVAL_IN_MS,
        TimeUnit.MILLISECONDS);
  }

  private void printQueryStatistics() {
    if (tracing.get()) {
      operationStatistics.forEach(
          (k, v) -> {
            QUERY_STATISTICS_LOGGER.info("Operation: {}, Statistics: {}", k, v);
          });
    }
  }

  public static QueryStatistics getInstance() {
    return QueryStatisticsHolder.INSTANCE;
  }

  public void addCost(String key, long costTimeInNanos) {
    if (tracing.get()) {
      operationStatistics
          .computeIfAbsent(key, k -> new OperationStatistic())
          .addTimeCost(costTimeInNanos);
    }
  }

  public void disableTracing() {
    tracing.set(false);
    operationStatistics.clear();
  }

  public void enableTracing() {
    tracing.set(true);
    operationStatistics.clear();
  }

  private static class OperationStatistic {
    // accumulated operation time in ns
    private final AtomicLong totalTime;
    private final AtomicLong totalCount;

    public OperationStatistic() {
      this.totalTime = new AtomicLong(0);
      this.totalCount = new AtomicLong(0);
    }

    public void addTimeCost(long costTimeInNanos) {
      totalTime.addAndGet(costTimeInNanos);
      totalCount.incrementAndGet();
    }

    @Override
    public String toString() {
      long time = totalTime.get() / 1_000;
      long count = totalCount.get();
      return "{"
          + "totalTime="
          + time
          + "us"
          + ", totalCount="
          + count
          + ", avgOperationTime="
          + (time / count)
          + "us"
          + '}';
    }
  }

  private static class QueryStatisticsHolder {

    private static final QueryStatistics INSTANCE = new QueryStatistics();

    private QueryStatisticsHolder() {}
  }
}
