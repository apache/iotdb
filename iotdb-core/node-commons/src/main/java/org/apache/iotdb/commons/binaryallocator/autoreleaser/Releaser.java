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

package org.apache.iotdb.commons.binaryallocator.autoreleaser;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public abstract class Releaser implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(Releaser.class);

  private Future<?> future;
  protected final String name;
  private final Duration shutdownTimeoutDuration;

  private ExecutorService executor;

  public Releaser(String name, Duration shutdownTimeoutDuration) {
    this.name = name;
    this.shutdownTimeoutDuration = shutdownTimeoutDuration;
  }

  /** Cancels the future. */
  void cancel() {
    future.cancel(false);
  }

  @Override
  public abstract void run();

  void setFuture(final Future<?> future) {
    this.future = future;
  }

  @Override
  public String toString() {
    return getClass().getName() + " [future=" + future + "]";
  }

  public void start() {
    if (null == executor) {
      executor = IoTDBThreadPoolFactory.newSingleThreadExecutor(name);
    }
    final Future<?> future = executor.submit(this);
    this.setFuture(future);
  }

  public void stop() {
    if (executor == null) {
      return;
    }

    LOGGER.info("Stopping {}", name);

    cancel();
    executor.shutdown();
    try {
      boolean result =
          executor.awaitTermination(shutdownTimeoutDuration.toMillis(), TimeUnit.MILLISECONDS);
      if (!result) {
        LOGGER.info("unable to stop auto releaser after {} ms", shutdownTimeoutDuration.toMillis());
      }
    } catch (final InterruptedException ignored) {
      Thread.currentThread().interrupt();
    }
    executor = null;
  }
}
