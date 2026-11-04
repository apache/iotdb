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

package org.apache.iotdb.commons.utils;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.i18n.UtilMessages;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class JVMCommonUtilsTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void getJdkVersionTest() {
    try {
      System.setProperty("java.version", "1.8.0_233");
      Assert.assertEquals(8, JVMCommonUtils.getJdkVersion());
      System.setProperty("java.version", "11.0.16");
      Assert.assertEquals(11, JVMCommonUtils.getJdkVersion());
      System.setProperty("java.version", "11.0.8-internal");
      Assert.assertEquals(11, JVMCommonUtils.getJdkVersion());
      System.setProperty("java.version", "17-internal");
      Assert.assertEquals(17, JVMCommonUtils.getJdkVersion());
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void getOccupiedSpaceMissingFolderReturnsZero() throws IOException {
    File missing = new File(tempFolder.getRoot(), "does-not-exist");
    Assert.assertFalse(missing.exists());
    // A non-existent folder must be treated as empty rather than throwing NoSuchFileException.
    Assert.assertEquals(0L, JVMCommonUtils.getOccupiedSpace(missing.getAbsolutePath()));
  }

  @Test
  public void getOccupiedSpaceSumsFileSizes() throws IOException {
    File dir = tempFolder.newFolder("data");
    byte[] payload = "hello-iotdb".getBytes(StandardCharsets.UTF_8);
    Files.write(new File(dir, "a.txt").toPath(), payload);
    Files.write(new File(dir, "b.txt").toPath(), payload);
    Assert.assertEquals(
        2L * payload.length, JVMCommonUtils.getOccupiedSpace(dir.getAbsolutePath()));
  }

  @Test
  public void unexpectedDiskSpaceErrorsLoggedOnlyOnceWhileErrorPersists() {
    ch.qos.logback.classic.Logger logger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(JVMCommonUtils.class);
    Level previousLevel = logger.getLevel();
    logger.setLevel(Level.ERROR);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.setContext(logger.getLoggerContext());
    appender.start();
    logger.addAppender(appender);

    JVMCommonUtils.resetDiskWarningLastPrintTimes();
    try {
      JVMCommonUtils.getUsableSpace(null);
      JVMCommonUtils.getUsableSpace(null);
      Assert.assertEquals(
          1, countLogEvents(appender, UtilMessages.UNEXPECTED_ERROR_CHECKING_DISK_SPACE_FOR_DIR));

      JVMCommonUtils.getDiskFreeRatio(null);
      JVMCommonUtils.getDiskFreeRatio(null);
      Assert.assertEquals(
          1, countLogEvents(appender, UtilMessages.UNEXPECTED_ERROR_CHECKING_DISK_SPACE));
    } finally {
      JVMCommonUtils.resetDiskWarningLastPrintTimes();
      logger.detachAppender(appender);
      logger.setLevel(previousLevel);
      appender.stop();
    }
  }

  @Test
  public void getDiskFreeRatioWarnsOnlyOnceWhileDiskWarningPersists() throws IOException {
    Path dir = Files.createTempDirectory("jvm-common-utils-test");
    ch.qos.logback.classic.Logger logger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(JVMCommonUtils.class);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.setContext(logger.getLoggerContext());
    appender.start();
    logger.addAppender(appender);

    JVMCommonUtils.resetDiskWarningLastPrintTimes();
    JVMCommonUtils.setDiskSpaceWarningThreshold(1.1);
    try {
      JVMCommonUtils.getDiskFreeRatio(dir.toString());
      JVMCommonUtils.getDiskFreeRatio(dir.toString());

      Assert.assertEquals(1, countLogEvents(appender, UtilMessages.DISK_ABOVE_WARNING_THRESHOLD));
    } finally {
      JVMCommonUtils.setDiskSpaceWarningThreshold(
          CommonDescriptor.getInstance().getConfig().getDiskSpaceWarningThreshold());
      JVMCommonUtils.resetDiskWarningLastPrintTimes();
      logger.detachAppender(appender);
      appender.stop();
      Files.deleteIfExists(dir);
    }
  }

  @Test
  public void getOccupiedSpaceIgnoresConcurrentlyDeletedEntries() throws Exception {
    File snapshotRoot = tempFolder.newFolder("snapshot");
    File subDir = new File(snapshotRoot, "tod_sod0-71");
    Assert.assertTrue(subDir.mkdirs());
    byte[] payload = "snapshot-data".getBytes(StandardCharsets.UTF_8);
    Files.write(new File(subDir, "a.bin").toPath(), payload);
    Files.write(new File(snapshotRoot, "other.bin").toPath(), payload);

    AtomicBoolean stop = new AtomicBoolean(false);
    Thread deleter =
        new Thread(
            () -> {
              while (!stop.get()) {
                try {
                  deleteRecursively(subDir.toPath());
                  Files.createDirectories(subDir.toPath());
                  Files.write(new File(subDir, "temp.bin").toPath(), payload);
                } catch (IOException ignored) {
                  // Concurrent create/delete is expected in this test.
                }
              }
            });
    deleter.start();

    try {
      for (int i = 0; i < 200; i++) {
        JVMCommonUtils.getOccupiedSpace(snapshotRoot.getAbsolutePath());
      }
    } finally {
      stop.set(true);
      deleter.join(5000);
    }
  }

  private long countLogEvents(ListAppender<ILoggingEvent> appender, String messagePattern) {
    return appender.list.stream()
        .filter(event -> messagePattern.equals(event.getMessage()))
        .count();
  }

  private static void deleteRecursively(Path path) throws IOException {
    if (!Files.exists(path)) {
      return;
    }
    try (Stream<Path> walk = Files.walk(path)) {
      walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }
  }
}
