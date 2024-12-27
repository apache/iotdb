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

package org.apache.iotdb.commons.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractPeriodicalServiceWithAdvance {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractPeriodicalServiceWithAdvance.class);
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition condition = lock.newCondition();
  protected volatile boolean skipNextSleep = false;
  private volatile boolean allowSubmitListen = false;

  protected ExecutorService securityServiceExecutor;
  private final long intervalMillis;

  protected AbstractPeriodicalServiceWithAdvance(
      final ExecutorService securityServiceExecutor, final long intervalMillis) {
    this.securityServiceExecutor = securityServiceExecutor;
    this.intervalMillis = intervalMillis;
  }

  public void advanceExecution() {
    if (lock.tryLock()) {
      try {
        condition.signalAll();
      } finally {
        lock.unlock();
      }
    } else {
      skipNextSleep = true;
    }
  }

  private void execute() {
    lock.lock();
    try {
      executeTask();
      if (!skipNextSleep) {
        condition.await(intervalMillis, TimeUnit.MILLISECONDS);
      }
      skipNextSleep = false;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("Interrupted when waiting for the next device view update: {}", e.getMessage());
    } finally {
      lock.unlock();

      if (allowSubmitListen) {
        securityServiceExecutor.submit(this::execute);
      }
    }
  }

  protected abstract void executeTask();

  public void startService() {
    allowSubmitListen = true;
    securityServiceExecutor.submit(this::execute);

    LOGGER.info("{} is started successfully.", getClass().getSimpleName());
  }

  public void stopService() {
    allowSubmitListen = false;

    LOGGER.info("{} is stopped successfully.", getClass().getSimpleName());
  }
}
