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

import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class JVMCommonUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(JVMCommonUtils.class);
  private static final long DISK_WARNING_PRINT_INTERVAL_MS = 3600 * 1000L;

  /** Default executor pool maximum size. */
  public static final int MAX_EXECUTOR_POOL_SIZE = Math.max(100, getCpuCores() * 5);

  private static final int CPUS = Runtime.getRuntime().availableProcessors();

  private static double diskSpaceWarningThreshold =
      CommonDescriptor.getInstance().getConfig().getDiskSpaceWarningThreshold();
  private static final ConcurrentMap<String, Long> cannotGetFreeSpaceLastPrintTimeMap =
      new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, Long> diskAboveWarningThresholdLastPrintTimeMap =
      new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, Long> unexpectedGetUsableSpaceErrorLastPrintTimeMap =
      new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, Long> unexpectedGetDiskFreeRatioErrorLastPrintTimeMap =
      new ConcurrentHashMap<>();

  /**
   * get JDK version.
   *
   * @return JDK version (int type)
   */
  public static int getJdkVersion() {
    String[] javaVersionElements = System.getProperty("java.version").split("-")[0].split("\\.");
    if (Integer.parseInt(javaVersionElements[0]) == 1) {
      return Integer.parseInt(javaVersionElements[1]);
    } else {
      return Integer.parseInt(javaVersionElements[0]);
    }
  }

  /**
   * NOTICE: This method is currently used only for data dir, thus using FSFactory to get file
   *
   * @param dir directory path
   * @return
   */
  public static long getUsableSpace(String dir) {
    try {
      File dirFile = FSFactoryProducer.getFSFactory().getFile(dir);
      dirFile.mkdirs();
      long usableSpace =
          IOUtils.retryNoException(5, 2000L, dirFile::getFreeSpace, space -> space > 0).orElse(0L);
      unexpectedGetUsableSpaceErrorLastPrintTimeMap.remove(String.valueOf(dir));
      return usableSpace;
    } catch (Exception e) {
      if (shouldPrintDiskWarning(
          unexpectedGetUsableSpaceErrorLastPrintTimeMap, String.valueOf(dir))) {
        LOGGER.error(UtilMessages.UNEXPECTED_ERROR_CHECKING_DISK_SPACE_FOR_DIR, dir, e);
      }
      return 0L;
    }
  }

  public static double getDiskFreeRatio(String dir) {
    try {
      File dirFile = FSFactoryProducer.getFSFactory().getFile(dir);
      if (!dirFile.mkdirs()) {
        // This may solve getFreeSpace() == 0?
        dirFile = new File(dir);
      }
      long freeSpace =
          IOUtils.retryNoException(5, 2000L, dirFile::getFreeSpace, space -> space > 0).orElse(0L);
      if (freeSpace == 0) {
        if (shouldPrintDiskWarning(cannotGetFreeSpaceLastPrintTimeMap, dir)) {
          LOGGER.warn(UtilMessages.CANNOT_GET_FREE_SPACE, dir);
        }
      } else {
        cannotGetFreeSpaceLastPrintTimeMap.remove(dir);
      }
      long totalSpace = dirFile.getTotalSpace();
      double ratio = 1.0 * freeSpace / totalSpace;
      if (ratio <= diskSpaceWarningThreshold) {
        if (shouldPrintDiskWarning(diskAboveWarningThresholdLastPrintTimeMap, dir)) {
          LOGGER.warn(UtilMessages.DISK_ABOVE_WARNING_THRESHOLD, dir, freeSpace, totalSpace);
        }
      } else {
        diskAboveWarningThresholdLastPrintTimeMap.remove(dir);
      }
      unexpectedGetDiskFreeRatioErrorLastPrintTimeMap.remove(String.valueOf(dir));
      return ratio;
    } catch (Exception e) {
      if (shouldPrintDiskWarning(
          unexpectedGetDiskFreeRatioErrorLastPrintTimeMap, String.valueOf(dir))) {
        LOGGER.error(UtilMessages.UNEXPECTED_ERROR_CHECKING_DISK_SPACE, dir, e);
      }
      return 0;
    }
  }

  public static boolean hasSpace(String dir) {
    return getDiskFreeRatio(dir) > diskSpaceWarningThreshold;
  }

  public static long getOccupiedSpace(String folderPath) throws IOException {
    Path folder = Paths.get(folderPath);
    if (!Files.exists(folder)) {
      return 0;
    }
    final long[] occupiedSpace = {0L};
    Files.walkFileTree(
        folder,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
            occupiedSpace[0] += attrs.size();
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFileFailed(Path file, IOException exc) {
            // A file or directory may be deleted concurrently during traversal; ignore it.
            return FileVisitResult.CONTINUE;
          }
        });
    return occupiedSpace[0];
  }

  public static int getCpuCores() {
    return CPUS;
  }

  public static int getMaxExecutorPoolSize() {
    return MAX_EXECUTOR_POOL_SIZE;
  }

  @TestOnly
  public static void setDiskSpaceWarningThreshold(double threshold) {
    diskSpaceWarningThreshold = threshold;
  }

  @TestOnly
  static void resetDiskWarningLastPrintTimes() {
    cannotGetFreeSpaceLastPrintTimeMap.clear();
    diskAboveWarningThresholdLastPrintTimeMap.clear();
    unexpectedGetUsableSpaceErrorLastPrintTimeMap.clear();
    unexpectedGetDiskFreeRatioErrorLastPrintTimeMap.clear();
  }

  private static boolean shouldPrintDiskWarning(
      ConcurrentMap<String, Long> lastPrintTimeMap, String dir) {
    long now = System.currentTimeMillis();
    AtomicBoolean shouldPrint = new AtomicBoolean(false);
    lastPrintTimeMap.compute(
        dir,
        (key, lastPrintTime) -> {
          if (lastPrintTime == null || now - lastPrintTime > DISK_WARNING_PRINT_INTERVAL_MS) {
            shouldPrint.set(true);
            return now;
          }
          return lastPrintTime;
        });
    return shouldPrint.get();
  }
}
