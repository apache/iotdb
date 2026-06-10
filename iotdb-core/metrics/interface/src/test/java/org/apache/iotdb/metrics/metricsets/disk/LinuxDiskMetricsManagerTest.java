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

package org.apache.iotdb.metrics.metricsets.disk;

import org.apache.iotdb.metrics.i18n.MetricsMessages;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;

public class LinuxDiskMetricsManagerTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void diskInfoFailureLoggedOnlyOnceUntilRecovery() throws Exception {
    File diskIdFolder = createDiskIdFolder();
    File processIoFile = tempFolder.newFile("process-io");
    writeProcessIoFile(processIoFile);
    File diskStatsFile = new File(tempFolder.getRoot(), "diskstats");

    LinuxDiskMetricsManager manager =
        new LinuxDiskMetricsManager(
            diskStatsFile.getAbsolutePath(),
            diskIdFolder.getAbsolutePath(),
            getDiskSectorSizePath(diskIdFolder),
            processIoFile.getAbsolutePath());

    ch.qos.logback.classic.Logger logger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(LinuxDiskMetricsManager.class);
    Level previousLevel = logger.getLevel();
    logger.setLevel(Level.WARN);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.setContext(logger.getLoggerContext());
    appender.start();
    logger.addAppender(appender);

    try {
      invokeUpdateInfo(manager);
      invokeUpdateInfo(manager);
      Assert.assertEquals(
          1, countLogEvents(appender, MetricsMessages.CANNOT_FIND_DISK_IO_STATUS_FILE));

      writeDiskStatsFile(diskStatsFile);
      invokeUpdateInfo(manager);

      Files.delete(diskStatsFile.toPath());
      invokeUpdateInfo(manager);
      Assert.assertEquals(
          2, countLogEvents(appender, MetricsMessages.CANNOT_FIND_DISK_IO_STATUS_FILE));
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(previousLevel);
      appender.stop();
    }
  }

  @Test
  public void missingProcessInfoLogsCannotFindOnlyOnceWithoutUpdateError() throws Exception {
    File diskIdFolder = createDiskIdFolder();
    File diskStatsFile = tempFolder.newFile("diskstats");
    writeDiskStatsFile(diskStatsFile);
    File processIoFile = new File(tempFolder.getRoot(), "missing-process-io");

    LinuxDiskMetricsManager manager =
        new LinuxDiskMetricsManager(
            diskStatsFile.getAbsolutePath(),
            diskIdFolder.getAbsolutePath(),
            getDiskSectorSizePath(diskIdFolder),
            processIoFile.getAbsolutePath());

    ch.qos.logback.classic.Logger logger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(LinuxDiskMetricsManager.class);
    Level previousLevel = logger.getLevel();
    logger.setLevel(Level.WARN);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.setContext(logger.getLoggerContext());
    appender.start();
    logger.addAppender(appender);

    try {
      invokeUpdateInfo(manager);
      invokeUpdateInfo(manager);

      Assert.assertEquals(
          1, countLogEvents(appender, MetricsMessages.CANNOT_FIND_PROCESS_IO_STATUS_FILE));
      Assert.assertEquals(
          0, countLogEvents(appender, MetricsMessages.ERROR_UPDATING_PROCESS_IO_INFO));
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(previousLevel);
      appender.stop();
    }
  }

  private File createDiskIdFolder() throws Exception {
    File diskIdFolder = tempFolder.newFolder("block");
    File sectorSizeFolder = new File(diskIdFolder, "sda" + File.separator + "queue");
    Assert.assertTrue(sectorSizeFolder.mkdirs());
    Files.write(
        new File(sectorSizeFolder, "hw_sector_size").toPath(),
        Collections.singletonList("512"),
        StandardCharsets.UTF_8);
    return diskIdFolder;
  }

  private String getDiskSectorSizePath(File diskIdFolder) {
    return new File(
            diskIdFolder, "%s" + File.separator + "queue" + File.separator + "hw_sector_size")
        .getAbsolutePath();
  }

  private void writeDiskStatsFile(File diskStatsFile) throws Exception {
    Files.write(
        diskStatsFile.toPath(),
        Collections.singletonList(" 8 0 sda 1 0 2 3 4 0 5 6 7 8 9"),
        StandardCharsets.UTF_8);
  }

  private void writeProcessIoFile(File processIoFile) throws Exception {
    Files.write(
        processIoFile.toPath(),
        Arrays.asList(
            "syscr: 1", "syscw: 2", "read_bytes: 3", "write_bytes: 4", "rchar: 5", "wchar: 6"),
        StandardCharsets.UTF_8);
  }

  private void invokeUpdateInfo(LinuxDiskMetricsManager manager) throws Exception {
    Method updateInfo = LinuxDiskMetricsManager.class.getDeclaredMethod("updateInfo");
    updateInfo.setAccessible(true);
    updateInfo.invoke(manager);
  }

  private long countLogEvents(ListAppender<ILoggingEvent> appender, String messagePattern) {
    return appender.list.stream()
        .filter(event -> messagePattern.equals(event.getMessage()))
        .count();
  }
}
