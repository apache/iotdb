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

import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.db.storageengine.load.metrics.ActiveLoadingFilesMetricsSet;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.TsFileSequenceReader;
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
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;

public class ActiveLoadDirScanner extends ActiveLoadScheduledExecutorService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ActiveLoadDirScanner.class);

  private static final String RESOURCE = ".resource";
  private static final String MODS = ".mods";

  private final AtomicReference<String[]> listeningDirsConfig = new AtomicReference<>();
  private final Set<String> listeningDirs = new CopyOnWriteArraySet<>();

  private final ActiveLoadTsFileLoader activeLoadTsFileLoader;

  public ActiveLoadDirScanner(final ActiveLoadTsFileLoader activeLoadTsFileLoader) {
    super(ThreadName.ACTIVE_LOAD_DIR_SCANNER);
    this.activeLoadTsFileLoader = activeLoadTsFileLoader;

    register(this::scanSafely);
    LOGGER.info("Active load dir scanner periodical job registered");
  }

  private void scanSafely() {
    try {
      scan();
    } catch (final Exception e) {
      LOGGER.warn("Error occurred during active load dir scanning.", e);
    }
  }

  private void scan() throws IOException {
    hotReloadActiveLoadDirs();

    for (final String listeningDir : listeningDirs) {
      final int currentAllowedPendingSize = activeLoadTsFileLoader.getCurrentAllowedPendingSize();
      if (currentAllowedPendingSize <= 0) {
        return;
      }

      final boolean isGeneratedByPipe =
          listeningDir.equals(IOTDB_CONFIG.getLoadActiveListeningPipeDir());
      FileUtils.streamFiles(new File(listeningDir), true, (String[]) null)
          .map(
              file ->
                  (file.getName().endsWith(RESOURCE) || file.getName().endsWith(MODS))
                      ? getTsFilePath(file.getAbsolutePath())
                      : file.getAbsolutePath())
          .filter(this::isTsFileCompleted)
          .limit(currentAllowedPendingSize)
          .forEach(file -> activeLoadTsFileLoader.tryTriggerTsFileLoad(file, isGeneratedByPipe));
    }
  }

  private boolean isTsFileCompleted(final String file) {
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(file, false)) {
      return TSFileConfig.MAGIC_STRING.equals(reader.readTailMagic());
    } catch (final Exception e) {
      return false;
    }
  }

  private void hotReloadActiveLoadDirs() {
    try {
      // Hot reload active load listening dirs if active listening is enabled
      if (IOTDB_CONFIG.getLoadActiveListeningEnable()) {
        if (IOTDB_CONFIG.getLoadActiveListeningDirs() != listeningDirsConfig.get()) {
          synchronized (this) {
            if (IOTDB_CONFIG.getLoadActiveListeningDirs() != listeningDirsConfig.get()) {
              listeningDirsConfig.set(IOTDB_CONFIG.getLoadActiveListeningDirs());
              listeningDirs.addAll(Arrays.asList(IOTDB_CONFIG.getLoadActiveListeningDirs()));
            }
          }
        }
      }

      // Hot reload active load listening dir for pipe data sync
      // Active load is always enabled for pipe data sync
      listeningDirs.add(IOTDB_CONFIG.getLoadActiveListeningPipeDir());

      // Create directories if not exists
      listeningDirs.forEach(this::createDirectoriesIfNotExists);
    } catch (final Exception e) {
      LOGGER.warn(
          "Error occurred during hot reload active load dirs. "
              + "Current active load listening dirs: {}.",
          listeningDirs,
          e);
    }
  }

  private void createDirectoriesIfNotExists(final String dirPath) {
    try {
      FileUtils.forceMkdir(new File(dirPath));
    } catch (final IOException e) {
      LOGGER.warn("Error occurred during creating directory {} for active load.", dirPath, e);
    }
  }

  private static String getTsFilePath(final String filePathWithResourceOrModsTail) {
    return filePathWithResourceOrModsTail.endsWith(RESOURCE)
        ? filePathWithResourceOrModsTail.substring(
            0, filePathWithResourceOrModsTail.length() - RESOURCE.length())
        : filePathWithResourceOrModsTail.substring(
            0, filePathWithResourceOrModsTail.length() - MODS.length());
  }

  // Metrics
  public long countAndReportActiveListeningDirsFileNumber() {
    final long[] fileCount = {0};
    try {
      for (String dir : listeningDirs) {
        Files.walkFileTree(
            new File(dir).toPath(),
            new SimpleFileVisitor<Path>() {
              @Override
              public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                fileCount[0]++;
                return FileVisitResult.CONTINUE;
              }
            });
      }
      ActiveLoadingFilesMetricsSet.getInstance().recordPendingFileCounter(fileCount[0]);
    } catch (final IOException e) {
      LOGGER.warn("Failed to count active listening dirs file number.", e);
    }
    return fileCount[0];
  }
}
