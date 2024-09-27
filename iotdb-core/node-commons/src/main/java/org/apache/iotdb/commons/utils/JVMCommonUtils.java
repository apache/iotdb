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

import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class JVMCommonUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(JVMCommonUtils.class);

  /** Default executor pool maximum size. */
  public static final int MAX_EXECUTOR_POOL_SIZE = Math.max(100, getCpuCores() * 5);

  private static final int CPUS = Runtime.getRuntime().availableProcessors();

  private static final double diskSpaceWarningThreshold =
      CommonDescriptor.getInstance().getConfig().getDiskSpaceWarningThreshold();

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
    File dirFile = FSFactoryProducer.getFSFactory().getFile(dir);
    dirFile.mkdirs();
    return IOUtils.retryNoException(5, 2000L, dirFile::getFreeSpace, space -> space > 0).orElse(0L);
  }

  public static double getDiskFreeRatio(String dir) {
    File dirFile = new File(dir);
    if (!dirFile.mkdirs()) {
      // This may solve getFreeSpace() == 0?
      dirFile = new File(dir);
    }
    long freeSpace =
        IOUtils.retryNoException(5, 2000L, dirFile::getFreeSpace, space -> space > 0).orElse(0L);
    if (freeSpace == 0) {
      LOGGER.warn("Cannot get free space for {} after retries, please check the disk status", dir);
    }
    long totalSpace = dirFile.getTotalSpace();
    double ratio = 1.0 * freeSpace / totalSpace;
    if (ratio <= diskSpaceWarningThreshold) {
      LOGGER.warn(
          "{} is above the warning threshold, free space {}, total space {}",
          dir,
          freeSpace,
          totalSpace);
    }
    return ratio;
  }

  public static boolean hasSpace(String dir) {
    return getDiskFreeRatio(dir) > diskSpaceWarningThreshold;
  }

  public static long getOccupiedSpace(String folderPath) throws IOException {
    Path folder = Paths.get(folderPath);
    try (Stream<Path> s = Files.walk(folder)) {
      return s.filter(p -> p.toFile().isFile()).mapToLong(p -> p.toFile().length()).sum();
    }
  }

  public static int getCpuCores() {
    return CPUS;
  }

  public static int getMaxExecutorPoolSize() {
    return MAX_EXECUTOR_POOL_SIZE;
  }
}
