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
import org.apache.iotdb.db.storageengine.load.metrics.ActiveLoadingFilesNumberMetricsSet;
import org.apache.iotdb.db.storageengine.load.metrics.ActiveLoadingFilesSizeMetricsSet;

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

  private final Set<String> noPermissionDirs = new CopyOnWriteArraySet<>();

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
      if (!checkPermission(listeningDir)) {
        continue;
      }

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
          .filter(file -> !activeLoadTsFileLoader.isFilePendingOrLoading(file))
          .filter(this::isTsFileCompleted)
          .limit(currentAllowedPendingSize)
          .forEach(file -> activeLoadTsFileLoader.tryTriggerTsFileLoad(file, isGeneratedByPipe));
    }
  }

  private boolean checkPermission(final String listeningDir) {
    try {
      final Path listeningDirPath = new File(listeningDir).toPath();

      if (!Files.isReadable(listeningDirPath)) {
        if (!noPermissionDirs.contains(listeningDir)) {
          LOGGER.error(
              "Current dir path is not readable: {}."
                  + "Skip scanning this dir. Please check the permission.",
              listeningDirPath);
          noPermissionDirs.add(listeningDir);
        }
        return false;
      }

      if (!Files.isWritable(listeningDirPath)) {
        if (!noPermissionDirs.contains(listeningDir)) {
          LOGGER.error(
              "Current dir path is not writable: {}."
                  + "Skip scanning this dir. Please check the permission.",
              listeningDirPath);
          noPermissionDirs.add(listeningDir);
        }
        return false;
      }

      noPermissionDirs.remove(listeningDir);
      return true;
    } catch (final Exception e) {
      LOGGER.error(
          "Error occurred during checking r/w permission of dir: {}. Skip scanning this dir.",
          listeningDir,
          e);
      return false;
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
              listeningDirs.clear();

              listeningDirsConfig.set(IOTDB_CONFIG.getLoadActiveListeningDirs());
              listeningDirs.addAll(Arrays.asList(IOTDB_CONFIG.getLoadActiveListeningDirs()));
            }
          }
        }
      } else {
        listeningDirs.clear();
      }
      // Hot reload active load listening dir for pipe data sync
      // Active load is always enabled for pipe data sync
      listeningDirs.add(IOTDB_CONFIG.getLoadActiveListeningPipeDir());

      // Create directories if not exists
      listeningDirs.forEach(this::createDirectoriesIfNotExists);

      ActiveLoadingFilesNumberMetricsSet.getInstance().updatePendingDirList(listeningDirs);
      ActiveLoadingFilesSizeMetricsSet.getInstance().updatePendingDirList(listeningDirs);
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
    long totalFileCount = 0;
    long totalFileSize = 0;

    try {
      for (final String dir : listeningDirs) {
        final long[] fileCountInDir = {0};
        final long[] fileSizeInDir = {0};

        Files.walkFileTree(
            new File(dir).toPath(),
            new SimpleFileVisitor<Path>() {
              @Override
              public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                fileCountInDir[0]++;
                try {
                  fileSizeInDir[0] += file.toFile().length();
                } catch (Exception e) {
                  LOGGER.debug("Failed to count active listening dirs file number.", e);
                }
                return FileVisitResult.CONTINUE;
              }
            });

        ActiveLoadingFilesNumberMetricsSet.getInstance()
            .updatePendingFileCounterInDir(dir, fileCountInDir[0]);
        ActiveLoadingFilesSizeMetricsSet.getInstance()
            .updatePendingFileCounterInDir(dir, fileSizeInDir[0]);

        totalFileCount += fileCountInDir[0];
        totalFileSize += fileSizeInDir[0];
      }

      ActiveLoadingFilesNumberMetricsSet.getInstance()
          .updateTotalPendingFileCounter(totalFileCount);
      ActiveLoadingFilesSizeMetricsSet.getInstance().updateTotalPendingFileCounter(totalFileSize);
    } catch (final IOException e) {
      LOGGER.debug("Failed to count active listening dirs file number.", e);
    }

    return totalFileCount;
  }
}
