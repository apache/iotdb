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
package org.apache.iotdb.db.storageengine.dataregion.flush;

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.utils.FilePathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is used to count, compute and persist the compression ratio of tsfiles. Whenever the
 * task of closing a file ends, the compression ratio of the file is calculated based on the total
 * MemTable size and the total size of the tsfile on disk. {@code totalMemorySize} records the data
 * size of memory, and {@code totalDiskSize} records the data size of disk. When the compression
 * rate of the current system is obtained, the average compression ratio is returned as the result,
 * that is {@code totalMemorySize}/{@code totalDiskSize}. At the same time, each time the
 * compression ratio statistics are updated, these two parameters are persisted on disk for system
 * recovery.
 */
public class CompressionRatio {

  private static final Logger LOGGER = LoggerFactory.getLogger(CompressionRatio.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  static final String COMPRESSION_RATIO_DIR = "compression_ratio";

  private static final String FILE_PREFIX_BEFORE_V121 = "Ratio-";
  private static final String FILE_PREFIX = "Compress-";

  private static final String SEPARATOR = "-";

  static final String RATIO_FILE_PATH_FORMAT = FILE_PREFIX + "%d" + SEPARATOR + "%d";

  /** The data size on memory */
  private static AtomicLong totalMemorySize = new AtomicLong(0);

  /** The data size on disk */
  private long totalDiskSize = 0L;

  private File directory;

  private String oldFileName = String.format(RATIO_FILE_PATH_FORMAT, 0, 0);

  private CompressionRatio() {
    directory =
        SystemFileFactory.INSTANCE.getFile(
            FilePathUtils.regularizePath(CONFIG.getSystemDir()) + COMPRESSION_RATIO_DIR);
    try {
      restore();
    } catch (IOException e) {
      LOGGER.error("restore file error caused by ", e);
    }
  }

  /**
   * Whenever the task of closing a file ends, the compression ratio of the file is calculated and
   * call this method.
   */
  public synchronized void updateRatio(long memorySize, long diskSize) throws IOException {
    File oldFile = SystemFileFactory.INSTANCE.getFile(directory, oldFileName);

    totalMemorySize.addAndGet(memorySize);
    totalDiskSize += diskSize;
    if (memorySize < 0 || totalMemorySize.get() < 0) {
      LOGGER.warn(
          "The compression ratio is negative, current memTableSize: {}, totalMemTableSize: {}",
          memorySize,
          totalMemorySize);
    }
    File newFile =
        SystemFileFactory.INSTANCE.getFile(
            directory,
            String.format(
                Locale.ENGLISH, RATIO_FILE_PATH_FORMAT, totalMemorySize.get(), totalDiskSize));
    persist(oldFile, newFile);
  }

  /** Get the average compression ratio for all closed files */
  public double getRatio() {
    return (double) totalMemorySize.get() / totalDiskSize;
  }

  private void persist(File oldFile, File newFile) throws IOException {
    checkDirectoryExist();
    if (!oldFile.exists()) {
      Files.createFile(newFile.toPath());
      LOGGER.debug(
          "Old ratio file {} doesn't exist, force create ratio file {}",
          oldFile.getAbsolutePath(),
          newFile.getAbsolutePath());
    } else {
      FileUtils.moveFile(oldFile, newFile);
      LOGGER.debug(
          "Compression ratio file updated, previous: {}, current: {}",
          oldFile.getAbsolutePath(),
          newFile.getAbsolutePath());
    }
    this.oldFileName = newFile.getName();
  }

  private void checkDirectoryExist() throws IOException {
    if (!directory.exists()) {
      FileUtils.forceMkdir(directory);
    }
  }

  /** Restore compression ratio statistics from disk when system restart */
  void restore() throws IOException {
    if (!directory.exists()) {
      return;
    }
    File[] ratioFiles = directory.listFiles((dir, name) -> name.startsWith(FILE_PREFIX));
    // First try to recover from the new version of the file, parse the file name, and get the file
    // with the largest disk size value
    if (ratioFiles != null && ratioFiles.length > 0) {
      int maxRatioIndex = 0;
      for (int i = 0; i < ratioFiles.length; i++) {
        String[] fileNameArray = ratioFiles[i].getName().split("-");
        // fileNameArray.length != 3 means the compression ratio may be negative, ignore it
        if (fileNameArray.length == 3) {
          try {
            long diskSize = Long.parseLong(fileNameArray[2]);
            if (diskSize > totalDiskSize) {
              totalMemorySize = new AtomicLong(Long.parseLong(fileNameArray[1]));
              totalDiskSize = diskSize;
              maxRatioIndex = i;
            }
          } catch (NumberFormatException ignore) {
            // ignore illegal compression file name
          }
        }
      }
      LOGGER.debug(
          "After restoring from compression ratio file, total memory size = {}, total disk size = {}",
          totalMemorySize,
          totalDiskSize);
      oldFileName = ratioFiles[maxRatioIndex].getName();
      deleteRedundantFilesByIndex(ratioFiles, maxRatioIndex);
    } else { // If there is no new file, try to restore from the old version file
      File[] ratioFilesBeforeV121 =
          directory.listFiles((dir, name) -> name.startsWith(FILE_PREFIX_BEFORE_V121));
      if (ratioFilesBeforeV121 != null && ratioFilesBeforeV121.length > 0) {
        int maxRatioIndex = 0;
        totalDiskSize = 1;
        for (int i = 0; i < ratioFilesBeforeV121.length; i++) {
          String[] fileNameArray = ratioFilesBeforeV121[i].getName().split("-");
          // fileNameArray.length != 3 means the compression ratio may be negative, ignore it
          if (fileNameArray.length == 3) {
            try {
              double currentCompressRatio =
                  Double.parseDouble(fileNameArray[1]) / Double.parseDouble(fileNameArray[2]);
              if (getRatio() < currentCompressRatio) {
                totalMemorySize = new AtomicLong((long) currentCompressRatio);
                maxRatioIndex = i;
              }
            } catch (NumberFormatException ignore) {
              // ignore illegal compression file name
            }
          }
        }
        deleteRedundantFilesByIndex(ratioFilesBeforeV121, maxRatioIndex);
      }
    }
  }

  public static void deleteRedundantFilesByIndex(File[] files, int index) throws IOException {
    for (int i = 0; i < files.length; i++) {
      if (i != index) {
        Files.delete(files[i].toPath());
      }
    }
  }

  @TestOnly
  void reset() throws IOException {
    if (!directory.exists()) {
      return;
    }
    File[] ratioFiles = directory.listFiles((dir, name) -> name.startsWith(FILE_PREFIX));
    if (ratioFiles == null) {
      return;
    }
    for (File file : ratioFiles) {
      Files.delete(file.toPath());
    }
    totalMemorySize = new AtomicLong(0);
    totalDiskSize = 0L;
  }

  public static CompressionRatio getInstance() {
    return CompressionRatioHolder.INSTANCE;
  }

  private static class CompressionRatioHolder {

    private static final CompressionRatio INSTANCE = new CompressionRatio();

    private CompressionRatioHolder() {}
  }
}
