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

package org.apache.iotdb.db.storageengine.load.active;

import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ActiveLoadAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(ActiveLoadAgent.class);

  private final ActiveLoadTsFileLoader activeLoadTsFileLoader;
  private final ActiveLoadDirScanner activeLoadDirScanner;
  private final ActiveLoadMetricsCollector activeLoadMetricsCollector;

  public ActiveLoadAgent() {
    this.activeLoadTsFileLoader = new ActiveLoadTsFileLoader();
    this.activeLoadDirScanner = new ActiveLoadDirScanner(activeLoadTsFileLoader);
    this.activeLoadMetricsCollector =
        new ActiveLoadMetricsCollector(activeLoadTsFileLoader, activeLoadDirScanner);
  }

  public ActiveLoadTsFileLoader loader() {
    return activeLoadTsFileLoader;
  }

  public ActiveLoadDirScanner scanner() {
    return activeLoadDirScanner;
  }

  public ActiveLoadMetricsCollector metrics() {
    return activeLoadMetricsCollector;
  }

  public synchronized void start() {
    activeLoadDirScanner.start();
    activeLoadMetricsCollector.start();
  }

  /**
   * Clean up all listening directories for active load on DataNode first startup. This method will
   * clean up all files and subdirectories in the listening directories, including: 1. Pending
   * directories (configured by load_active_listening_dirs) 2. Pipe directory (for pipe data sync)
   * 3. Failed directory (for failed files)
   *
   * <p>This method is called during DataNode startup and must not throw any exceptions to ensure
   * startup can proceed normally. All exceptions are caught and logged internally.
   */
  public static void cleanupListeningDirectories() {
    try {
      final List<String> dirsToClean = new ArrayList<>();

      dirsToClean.addAll(
          Arrays.asList(IoTDBDescriptor.getInstance().getConfig().getLoadActiveListeningDirs()));

      // Add pipe dir
      dirsToClean.add(IoTDBDescriptor.getInstance().getConfig().getLoadActiveListeningPipeDir());

      // Add failed dir
      dirsToClean.add(IoTDBDescriptor.getInstance().getConfig().getLoadActiveListeningFailDir());

      // Clean up each directory
      for (final String dirPath : dirsToClean) {
        try {
          if (dirPath == null || dirPath.isEmpty()) {
            continue;
          }

          final File dir = new File(dirPath);

          // Check if directory exists and is a directory
          // These methods may throw SecurityException if access is denied
          try {
            if (!dir.exists() || !dir.isDirectory()) {
              continue;
            }
          } catch (Exception e) {
            LOGGER.debug("Failed to check directory: {}", dirPath, e);
            continue;
          }

          // Only delete contents inside the directory, not the directory itself
          // listFiles() may throw SecurityException if access is denied
          File[] files = null;
          try {
            files = dir.listFiles();
          } catch (Exception e) {
            LOGGER.warn("Failed to list files in directory: {}", dirPath, e);
            continue;
          }

          if (files != null) {
            for (final File file : files) {
              // FileUtils.deleteFileOrDirectory internally calls file.isDirectory() and
              // file.listFiles() without try-catch, so exceptions may propagate here.
              // We need to catch it to prevent one file failure from stopping the cleanup.
              try {
                FileUtils.deleteFileOrDirectory(file, true);
              } catch (Exception e) {
                LOGGER.debug("Failed to delete file or directory: {}", file.getAbsolutePath(), e);
              }
            }
          }
        } catch (Exception e) {
          LOGGER.warn("Failed to cleanup directory: {}", dirPath, e);
        }
      }

      LOGGER.info("Cleaned up active load listening directories");
    } catch (Throwable t) {
      // Catch all exceptions and errors (including OutOfMemoryError, etc.)
      // to ensure startup process is not affected
      LOGGER.warn("Unexpected error during cleanup of active load listening directories", t);
    }
  }
}
