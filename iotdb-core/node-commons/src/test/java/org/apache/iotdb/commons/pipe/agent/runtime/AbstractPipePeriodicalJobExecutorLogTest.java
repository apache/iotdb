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

package org.apache.iotdb.commons.pipe.agent.runtime;

import org.apache.iotdb.commons.concurrent.WrappedRunnable;
import org.apache.iotdb.commons.i18n.PipeMessages;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class AbstractPipePeriodicalJobExecutorLogTest {

  @Test
  public void periodicalJobFailureLoggedOnlyOnceUntilSuccess() {
    ch.qos.logback.classic.Logger logger =
        (ch.qos.logback.classic.Logger)
            LoggerFactory.getLogger(AbstractPipePeriodicalJobExecutor.class);
    Level previousLevel = logger.getLevel();
    logger.setLevel(Level.WARN);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.setContext(logger.getLoggerContext());
    appender.start();
    logger.addAppender(appender);

    AtomicBoolean shouldFail = new AtomicBoolean(true);
    WrappedRunnable job =
        AbstractPipePeriodicalJobExecutor.wrapPeriodicalJobWithFailureLogThrottle(
            "testJob",
            () -> {
              if (shouldFail.get()) {
                throw new RuntimeException("periodical job failure");
              }
            });

    try {
      job.run();
      job.run();
      Assert.assertEquals(1, countLogEvents(appender, PipeMessages.PERIODICAL_JOB_FAILED));

      shouldFail.set(false);
      job.run();

      shouldFail.set(true);
      job.run();
      Assert.assertEquals(2, countLogEvents(appender, PipeMessages.PERIODICAL_JOB_FAILED));
    } finally {
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
