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

package org.apache.iotdb.db.storageengine.dataregion.wal;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class WALManagerLogTest {

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static final String WAL_THROTTLE_WARNING_MESSAGE =
      "WAL disk usage {} is larger than the wal_throttle_threshold_in_byte * 0.8 {}, please check your write load, iot consensus and the pipe module. It's better to allocate more disk for WAL.";

  private long previousThrottleThreshold;

  @Before
  public void setUp() {
    previousThrottleThreshold = CONFIG.getThrottleThreshold();
    CONFIG.setThrottleThreshold(100);
    WALManager.getInstance().clear();
    WALManager.getInstance().resetWalThrottleWarningLogTime();
  }

  @After
  public void tearDown() {
    WALManager.getInstance().clear();
    WALManager.getInstance().resetWalThrottleWarningLogTime();
    CONFIG.setThrottleThreshold(previousThrottleThreshold);
  }

  @Test
  public void walThrottleWarningLoggedOnlyOnceUntilUsageRecovers() {
    ch.qos.logback.classic.Logger logger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(WALManager.class);
    Level previousLevel = logger.getLevel();
    logger.setLevel(Level.WARN);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.setContext(logger.getLoggerContext());
    appender.start();
    logger.addAppender(appender);

    WALManager walManager = WALManager.getInstance();
    try {
      long throttleThreshold = walManager.getThrottleThreshold();
      walManager.addTotalDiskUsage(throttleThreshold * 2);
      walManager.logWalThrottleWarningIfNecessary();
      walManager.logWalThrottleWarningIfNecessary();

      Assert.assertEquals(1, countLogEvents(appender, WAL_THROTTLE_WARNING_MESSAGE));

      walManager.subtractTotalDiskUsage(throttleThreshold / 2);
      walManager.logWalThrottleWarningIfNecessary();
      Assert.assertEquals(1, countLogEvents(appender, WAL_THROTTLE_WARNING_MESSAGE));

      walManager.subtractTotalDiskUsage(throttleThreshold + throttleThreshold / 2);
      walManager.logWalThrottleWarningIfNecessary();
      Assert.assertEquals(1, countLogEvents(appender, WAL_THROTTLE_WARNING_MESSAGE));

      walManager.addTotalDiskUsage(throttleThreshold);
      walManager.logWalThrottleWarningIfNecessary();
      Assert.assertEquals(2, countLogEvents(appender, WAL_THROTTLE_WARNING_MESSAGE));
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
