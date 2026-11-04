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

package org.apache.iotdb.commons.concurrent.threadpool;

import org.apache.iotdb.commons.i18n.CommonMessages;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ScheduledExecutorUtilTest {

  @Test
  public void sameFailureLoggedOnlyAfterIntervalOrRecovery() {
    ScheduledExecutorUtil.FailureLogState failureLogState =
        new ScheduledExecutorUtil.FailureLogState();
    RuntimeException failure = new RuntimeException("same failure");
    long startTime = 1000;

    Assert.assertTrue(ScheduledExecutorUtil.shouldLogFailure(failureLogState, failure, startTime));
    Assert.assertFalse(
        ScheduledExecutorUtil.shouldLogFailure(failureLogState, failure, startTime + 1));

    Assert.assertTrue(
        ScheduledExecutorUtil.shouldLogFailure(
            failureLogState,
            failure,
            startTime + ScheduledExecutorUtil.SCHEDULE_TASK_FAILED_LOG_INTERVAL_MS));
    Assert.assertFalse(
        ScheduledExecutorUtil.shouldLogFailure(
            failureLogState,
            failure,
            startTime + ScheduledExecutorUtil.SCHEDULE_TASK_FAILED_LOG_INTERVAL_MS + 1));

    failureLogState.clear();
    Assert.assertTrue(
        ScheduledExecutorUtil.shouldLogFailure(failureLogState, failure, startTime + 2));
  }

  @Test
  public void differentFailuresLoggedImmediately() {
    ScheduledExecutorUtil.FailureLogState failureLogState =
        new ScheduledExecutorUtil.FailureLogState();
    long startTime = 1000;

    Assert.assertTrue(
        ScheduledExecutorUtil.shouldLogFailure(
            failureLogState, new RuntimeException("first"), startTime));
    Assert.assertFalse(
        ScheduledExecutorUtil.shouldLogFailure(
            failureLogState, new RuntimeException("first"), startTime + 1));
    Assert.assertTrue(
        ScheduledExecutorUtil.shouldLogFailure(
            failureLogState, new RuntimeException("second"), startTime + 2));
    Assert.assertTrue(
        ScheduledExecutorUtil.shouldLogFailure(
            failureLogState, new IllegalStateException("second"), startTime + 3));
  }

  @Test
  public void safeScheduledCommandSuppressesRepeatedFailureUntilSuccess() throws Exception {
    ch.qos.logback.classic.Logger logger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ScheduledExecutorUtil.class);
    Level previousLevel = logger.getLevel();
    logger.setLevel(Level.ERROR);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.setContext(logger.getLoggerContext());
    appender.start();
    logger.addAppender(appender);

    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    ScheduledFuture<?> future = null;
    try {
      AtomicInteger runs = new AtomicInteger();
      CountDownLatch latch = new CountDownLatch(5);
      future =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              executor,
              () -> {
                int run = runs.incrementAndGet();
                latch.countDown();
                if (run <= 2 || run == 4) {
                  throw new RuntimeException("transient failure");
                }
              },
              0,
              10,
              TimeUnit.MILLISECONDS);

      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
      future.cancel(true);

      Assert.assertEquals(2, countLogEvents(appender, CommonMessages.SCHEDULE_TASK_FAILED));
    } finally {
      if (future != null) {
        future.cancel(true);
      }
      executor.shutdownNow();
      logger.detachAppender(appender);
      logger.setLevel(previousLevel);
      appender.stop();
    }
  }

  private long countLogEvents(ListAppender<ILoggingEvent> appender, String messagePattern) {
    return appender.list.stream()
        .filter(event -> messagePattern.equals(event.getMessage()))
        .count();
  }
}
