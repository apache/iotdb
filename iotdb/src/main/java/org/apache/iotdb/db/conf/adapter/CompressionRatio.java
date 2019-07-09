/**
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

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompressionRatio {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBConfigDynamicAdapter.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private static final String COMPRESSION_RATIO_NAME = "compression_ratio";

  private static final String FILE_PREFIX = "Ratio-";

  private static final String SEPARATOR = "-";

  private static final double DEFAULT_COMPRESSION_RATIO = 2.0f;

  private double compressionRatioSum;

  private long calcuTimes;

  private String directoryPath;

  private CompressionRatio() {
    directoryPath = FilePathUtils.regularizePath(CONFIG.getSystemDir()) + COMPRESSION_RATIO_NAME;
    try {
      restore();
    } catch (IOException e) {
      LOGGER.error("Can not restore CompressionRatio", e);
    }
  }

  public synchronized void updateRatio(int flushNum) throws IOException {
    File oldFile = new File(directoryPath,
        FILE_PREFIX + compressionRatioSum + SEPARATOR + calcuTimes);
    compressionRatioSum +=
        (TSFileConfig.groupSizeInByte * flushNum) / CONFIG.getTsFileSizeThreshold();
    calcuTimes++;
    File newFile = new File(directoryPath,
        FILE_PREFIX + compressionRatioSum + SEPARATOR + calcuTimes);
    persist(oldFile, newFile);
    IoTDBConfigDynamicAdapter.getInstance().tryToAdaptParameters();
  }

  public synchronized double getRatio() {
    return calcuTimes == 0 ? DEFAULT_COMPRESSION_RATIO : compressionRatioSum / calcuTimes;
  }

  private void persist(File oldFile, File newFile) throws IOException {
    if (!oldFile.exists()) {
      FileUtils.forceMkdir(newFile);
      LOGGER.debug("Old ratio file {} doesn't exist, force create ratio file {}",
          oldFile.getAbsolutePath(), newFile.getAbsolutePath());
    } else {
      FileUtils.moveFile(oldFile, newFile);
      LOGGER.debug("Compression ratio file updated, previous: {}, current: {}",
          oldFile.getAbsolutePath(), newFile.getAbsolutePath());
    }
  }

  private void restore() throws IOException {
    File directory = new File(directoryPath);
    File[] ratioFiles = directory.listFiles((dir, name) -> name.startsWith(FILE_PREFIX));
    File restoreFile;
    if (ratioFiles != null && ratioFiles.length > 0) {
      long maxTimes = 0;
      int maxRatioIndex = 0;
      for (int i = 0; i < ratioFiles.length; i++) {
        long times = Long.parseLong(ratioFiles[i].getName().split("-")[2]);
        if (times > maxTimes) {
          maxTimes = times;
          maxRatioIndex = i;
        }
      }
      calcuTimes = maxTimes;
      for (int i = 0; i < ratioFiles.length; i++) {
        if (i != maxRatioIndex) {
          ratioFiles[i].delete();
        }
      }
    } else {
      restoreFile = new File(directory, FILE_PREFIX + compressionRatioSum + SEPARATOR + calcuTimes);
      FileUtils.forceMkdir(restoreFile);
    }
  }

  public static CompressionRatio getInstance() {
    return CompressionRatioHolder.INSTANCE;
  }

  private static class CompressionRatioHolder {

    private static final CompressionRatio INSTANCE = new CompressionRatio();

    private CompressionRatioHolder() {

    }

  }
}
