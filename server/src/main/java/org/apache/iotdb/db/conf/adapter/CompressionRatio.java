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
package org.apache.iotdb.db.conf.adapter;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.tsfile.utils.FilePathUtils;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Locale;

/**
 * This class is used to count, compute and persist the compression ratio of tsfiles. Whenever the
 * task of closing a file ends, the compression ratio of the file is calculated based on the total
 * MemTable size and the total size of the tsfile on disk. {@code compressionRatioSum} records the
 * sum of these compression ratios, and {@Code calcTimes} records the number of closed file tasks.
 * When the compression rate of the current system is obtained, the average compression ratio is
 * returned as the result, that is {@code compressionRatioSum}/{@Code calcTimes}. At the same time,
 * each time the compression ratio statistics are updated, these two parameters are persisted on
 * disk for system recovery.
 */
public class CompressionRatio {

  private static final Logger LOGGER = LoggerFactory.getLogger(CompressionRatio.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  static final String COMPRESSION_RATIO_DIR = "compression_ratio";

  private static final String FILE_PREFIX = "Ratio-";

  private static final String SEPARATOR = "-";

  static final String RATIO_FILE_PATH_FORMAT = FILE_PREFIX + "%f" + SEPARATOR + "%d";

  private static final double DEFAULT_COMPRESSION_RATIO = 2.0;

  private AtomicDouble compressionRatio = new AtomicDouble(DEFAULT_COMPRESSION_RATIO);

  /** The total sum of all compression ratios. */
  private double compressionRatioSum;

  /** The number of compression ratios. */
  private long calcTimes;

  private File directory;

  private CompressionRatio() {
    directory =
        SystemFileFactory.INSTANCE.getFile(
            FilePathUtils.regularizePath(CONFIG.getSystemDir()) + COMPRESSION_RATIO_DIR);
    restore();
  }

  /**
   * Whenever the task of closing a file ends, the compression ratio of the file is calculated and
   * call this method.
   *
   * @param currentCompressionRatio the compression ratio of the closing file.
   */
  public synchronized void updateRatio(double currentCompressionRatio) throws IOException {
    File oldFile =
        SystemFileFactory.INSTANCE.getFile(
            directory,
            String.format(Locale.ENGLISH, RATIO_FILE_PATH_FORMAT, compressionRatioSum, calcTimes));
    compressionRatioSum += currentCompressionRatio;
    calcTimes++;
    File newFile =
        SystemFileFactory.INSTANCE.getFile(
            directory,
            String.format(Locale.ENGLISH, RATIO_FILE_PATH_FORMAT, compressionRatioSum, calcTimes));
    persist(oldFile, newFile);
    compressionRatio.set(compressionRatioSum / calcTimes);
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Compression ratio is {}", compressionRatio.get());
    }
  }

  /** Get the average compression ratio for all closed files */
  public double getRatio() {
    return compressionRatio.get();
  }

  private void persist(File oldFile, File newFile) throws IOException {
    checkDirectoryExist();
    if (!oldFile.exists()) {
      newFile.createNewFile();
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
  }

  private void checkDirectoryExist() throws IOException {
    if (!directory.exists()) {
      FileUtils.forceMkdir(directory);
    }
  }

  /** Restore compression ratio statistics from disk when system restart */
  void restore() {
    if (!directory.exists()) {
      return;
    }
    File[] ratioFiles = directory.listFiles((dir, name) -> name.startsWith(FILE_PREFIX));
    if (ratioFiles != null && ratioFiles.length > 0) {
      long maxTimes = 0;
      double maxCompressionRatioSum = 0;
      int maxRatioIndex = 0;
      for (int i = 0; i < ratioFiles.length; i++) {
        String[] splits = ratioFiles[i].getName().split("-");
        long times = Long.parseLong(splits[2]);
        if (times > maxTimes) {
          maxTimes = times;
          maxCompressionRatioSum = Double.parseDouble(splits[1]);
          maxRatioIndex = i;
        }
      }
      calcTimes = maxTimes;
      compressionRatioSum = maxCompressionRatioSum;
      if (calcTimes != 0) {
        compressionRatio.set(compressionRatioSum / calcTimes);
      }
      LOGGER.debug(
          "After restoring from compression ratio file, compressionRatioSum = {}, calcTimes = {}",
          compressionRatioSum,
          calcTimes);
      for (int i = 0; i < ratioFiles.length; i++) {
        if (i != maxRatioIndex) {
          ratioFiles[i].delete();
        }
      }
    }
  }

  /** Only for test */
  void reset() {
    calcTimes = 0;
    compressionRatioSum = 0;
  }

  public double getCompressionRatioSum() {
    return compressionRatioSum;
  }

  long getCalcTimes() {
    return calcTimes;
  }

  public static CompressionRatio getInstance() {
    return CompressionRatioHolder.INSTANCE;
  }

  private static class CompressionRatioHolder {

    private static final CompressionRatio INSTANCE = new CompressionRatio();

    private CompressionRatioHolder() {}
  }
}
