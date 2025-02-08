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

package org.apache.iotdb.commons.binaryallocator.evictor;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public abstract class Evictor implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(Evictor.class);

  private ScheduledFuture<?> scheduledFuture;
  private final String name;
  private final Duration evictorShutdownTimeoutDuration;

  private ScheduledExecutorService executor;

  public Evictor(String name, Duration evictorShutdownTimeoutDuration) {
    this.name = name;
    this.evictorShutdownTimeoutDuration = evictorShutdownTimeoutDuration;
  }

  /** Cancels the scheduled future. */
  void cancel() {
    scheduledFuture.cancel(false);
  }

  @Override
  public abstract void run();

  void setScheduledFuture(final ScheduledFuture<?> scheduledFuture) {
    this.scheduledFuture = scheduledFuture;
  }

  @Override
  public String toString() {
    return getClass().getName() + " [scheduledFuture=" + scheduledFuture + "]";
  }

  public void startEvictor(final Duration delay) {
    if (null == executor) {
      executor = IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(name);
    }
    final ScheduledFuture<?> scheduledFuture =
        ScheduledExecutorUtil.safelyScheduleAtFixedRate(
            executor, this, delay.toMillis(), delay.toMillis(), TimeUnit.MILLISECONDS);
    this.setScheduledFuture(scheduledFuture);
  }

  public void stopEvictor() {
    if (executor == null) {
      return;
    }

    LOGGER.info("Stopping {}", name);

    cancel();
    executor.shutdown();
    try {
      boolean result =
          executor.awaitTermination(
              evictorShutdownTimeoutDuration.toMillis(), TimeUnit.MILLISECONDS);
      if (!result) {
        LOGGER.info(
            "unable to stop evictor after {} ms", evictorShutdownTimeoutDuration.toMillis());
      }
    } catch (final InterruptedException ignored) {
      Thread.currentThread().interrupt();
    }
    executor = null;
  }
}
