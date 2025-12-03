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

import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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
   */
  public static void cleanupListeningDirectories() {
    try {
      final Set<String> dirsToClean = new HashSet<>();

      try {
        // Add configured listening dirs
        if (IoTDBDescriptor.getInstance().getConfig().getLoadActiveListeningEnable()) {
          dirsToClean.addAll(
              Arrays.asList(
                  IoTDBDescriptor.getInstance().getConfig().getLoadActiveListeningDirs()));
        }

        // Add pipe dir
        dirsToClean.add(IoTDBDescriptor.getInstance().getConfig().getLoadActiveListeningPipeDir());

        // Add failed dir
        dirsToClean.add(IoTDBDescriptor.getInstance().getConfig().getLoadActiveListeningFailDir());
      } catch (Exception e) {
        return;
      }

      int totalFilesDeleted = 0;
      int totalSubDirsDeleted = 0;

      for (final String dirPath : dirsToClean) {
        try {
          final File dir = new File(dirPath);

          if (!dir.exists() || !dir.isDirectory()) {
            continue;
          }

          final long[] fileCount = {0};
          final long[] subdirCount = {0};

          Files.walkFileTree(
              dir.toPath(),
              new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                  try {
                    Files.delete(file);
                    fileCount[0]++;
                  } catch (Exception e) {
                    // Ignore
                  }
                  return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                  if (exc == null && !dir.toFile().getAbsolutePath().equals(dirPath)) {
                    try {
                      Files.delete(dir);
                      subdirCount[0]++;
                    } catch (Exception e) {
                      // Ignore
                    }
                  }
                  return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) {
                  return FileVisitResult.CONTINUE;
                }
              });

          totalFilesDeleted += fileCount[0];
          totalSubDirsDeleted += subdirCount[0];
        } catch (Exception e) {
          // Ignore
        }
      }

      if (totalFilesDeleted > 0 || totalSubDirsDeleted > 0) {
        LOGGER.info(
            "Cleaned up active load listening directories, deleted {} files and {} subdirectories",
            totalFilesDeleted,
            totalSubDirsDeleted);
      }
    } catch (Throwable t) {
      // Ignore all errors to prevent any unexpected behavior
    }
  }
}
