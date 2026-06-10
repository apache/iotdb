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

package org.apache.iotdb.db.storageengine.dataregion.tsfile.generator;

import org.apache.iotdb.commons.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.i18n.StorageEngineMessages;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class TsFileNameGeneratorLogTest {

  @Before
  public void setUp() {
    TsFileNameGenerator.resetAllDisksFullLogTime();
  }

  @After
  public void tearDown() {
    TsFileNameGenerator.resetAllDisksFullLogTime();
  }

  @Test
  public void allDisksFullTsFileDirErrorLoggedOnlyOnceUntilRecovery() {
    ch.qos.logback.classic.Logger logger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(TsFileNameGenerator.class);
    Level previousLevel = logger.getLevel();
    logger.setLevel(Level.ERROR);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.setContext(logger.getLoggerContext());
    appender.start();
    logger.addAppender(appender);

    DiskSpaceInsufficientException exception =
        new DiskSpaceInsufficientException(Arrays.asList("folder1", "folder2"));
    try {
      TsFileNameGenerator.logAllDisksFullCannotCreateTsFileDirIfNecessary(exception);
      TsFileNameGenerator.logAllDisksFullCannotCreateTsFileDirIfNecessary(exception);
      Assert.assertEquals(
          1,
          countLogEvents(appender, StorageEngineMessages.ALL_DISKS_FULL_CANNOT_CREATE_TSFILE_DIR));

      TsFileNameGenerator.resetAllDisksFullLogTime();
      TsFileNameGenerator.logAllDisksFullCannotCreateTsFileDirIfNecessary(exception);
      Assert.assertEquals(
          2,
          countLogEvents(appender, StorageEngineMessages.ALL_DISKS_FULL_CANNOT_CREATE_TSFILE_DIR));
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
