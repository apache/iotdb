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
package org.apache.iotdb.db.service.basic.count;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryFrequency {

  // Record query count
  private static final Logger QUERY_FREQUENCY_LOGGER = LoggerFactory.getLogger("QUERY_FREQUENCY");
  private static final AtomicInteger queryCount = new AtomicInteger(0);

  public QueryFrequency(IoTDBConfig config) {
    ScheduledExecutorService timedQuerySqlCountThread =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("timedQuerySqlCount");
    timedQuerySqlCountThread.scheduleAtFixedRate(
        () -> {
          if (queryCount.get() != 0) {
            QUERY_FREQUENCY_LOGGER.info(
                "Query count in current 1 minute {} ", queryCount.getAndSet(0));
          }
        },
        config.getFrequencyIntervalInMinute(),
        config.getFrequencyIntervalInMinute(),
        TimeUnit.MINUTES);
  }

  public void incrementAndGet() {
    queryCount.incrementAndGet();
  }
}
