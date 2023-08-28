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

package org.apache.iotdb.db.pipe.agent.runtime;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.db.pipe.extractor.realtime.listener.PipeInsertionDataNodeListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PipeCronEventInjector {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeCronEventInjector.class);

  private static final int CRON_EVENT_INJECTOR_INTERVAL_SECONDS = 1;

  private static final ScheduledExecutorService CRON_EVENT_INJECTOR_EXECUTOR =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.PIPE_RUNTIME_CRON_EVENT_INJECTOR.getName());

  private Future<?> injectorFuture;

  public synchronized void start() {
    if (injectorFuture == null) {
      injectorFuture =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              CRON_EVENT_INJECTOR_EXECUTOR,
              this::inject,
              CRON_EVENT_INJECTOR_INTERVAL_SECONDS,
              CRON_EVENT_INJECTOR_INTERVAL_SECONDS,
              TimeUnit.SECONDS);
      LOGGER.info("Pipe cron event injector is started successfully.");
    }
  }

  private synchronized void inject() {
    PipeInsertionDataNodeListener.getInstance().listenToHeartbeat(false);
  }

  public synchronized void stop() {
    if (injectorFuture != null) {
      injectorFuture.cancel(false);
      injectorFuture = null;
      LOGGER.info("Pipe cron event injector is stopped successfully.");
    }
  }
}
