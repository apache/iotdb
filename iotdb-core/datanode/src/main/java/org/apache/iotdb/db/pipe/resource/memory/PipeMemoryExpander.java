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

package org.apache.iotdb.db.pipe.resource.memory;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PipeMemoryExpander {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeMemoryExpander.class);

  private static final long MEMORY_EXPANDER_INTERVAL_SECONDS =
      PipeConfig.getInstance().getPipeMemoryExpanderIntervalSeconds();

  private static final ScheduledExecutorService MEMORY_EXPANDER_EXECUTOR =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.PIPE_MEMORY_EXPANDER.getName());

  private Future<?> expanderFuture;

  public synchronized void start() {
    if (expanderFuture == null) {
      expanderFuture =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              MEMORY_EXPANDER_EXECUTOR,
              PipeResourceManager.memory()::tryExpandAll,
              MEMORY_EXPANDER_INTERVAL_SECONDS,
              MEMORY_EXPANDER_INTERVAL_SECONDS,
              TimeUnit.SECONDS);
      LOGGER.info("Pipe memory expander is started successfully.");
    }
  }

  public synchronized void stop() {
    if (expanderFuture != null) {
      expanderFuture.cancel(false);
      expanderFuture = null;
      LOGGER.info("Pipe memory expander is stopped successfully.");
    }
  }
}
