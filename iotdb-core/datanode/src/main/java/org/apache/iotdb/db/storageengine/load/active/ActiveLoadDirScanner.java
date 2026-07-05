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
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.storageengine.load.metrics.ActiveLoadingFilesNumberMetricsSet;
import org.apache.iotdb.db.storageengine.load.metrics.ActiveLoadingFilesSizeMetricsSet;
import org.apache.iotdb.db.storageengine.load.util.LoadUtil;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class ActiveLoadDirScanner extends ActiveLoadScheduledExecutorService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ActiveLoadDirScanner.class);

  private final AtomicReference<String[]> listeningDirsConfig = new AtomicReference<>();
  private final AtomicReference<String> pipeListeningDirConfig = new AtomicReference<>();
  private final Set<String> listeningDirs = new CopyOnWriteArraySet<>();

  private final Set<String> noPermissionDirs = new CopyOnWriteArraySet<>();

  private final AtomicBoolean isReadOnlyLogPrinted = new AtomicBoolean(false);

  private final ActiveLoadTsFileLoader activeLoadTsFileLoader;

  public ActiveLoadDirScanner(final ActiveLoadTsFileLoader activeLoadTsFileLoader) {
    super(ThreadName.ACTIVE_LOAD_DIR_SCANNER);
    this.activeLoadTsFileLoader = activeLoadTsFileLoader;

    register(this::scanSafely);
    LOGGER.info(StorageEngineMessages.ACTIVE_LOAD_DIR_SCANNER_REGISTERED);
  }

  private void scanSafely() {
    try {
      scan();
    } catch (final Exception e) {
      LOGGER.warn(StorageEngineMessages.ERROR_ACTIVE_LOAD_DIR_SCANNING, e);
    }
  }

  private void scan() throws IOException {
    if (CommonDescriptor.getInstance().getConfig().isReadOnly()) {
      if (!isReadOnlyLogPrinted.get()) {
        LOGGER.warn(StorageEngineMessages.SYSTEM_READ_ONLY_SKIP_ACTIVE_SCAN);
        isReadOnlyLogPrinted.set(true);
      }
      return;
    }
    isReadOnlyLogPrinted.set(false);

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
      final File listeningDirFile = new File(listeningDir);
      try (final Stream<File> fileStream =
          FileUtils.streamFiles(listeningDirFile, true, (String[]) null)) {
        try {
          fileStream
              .filter(file -> !activeLoadTsFileLoader.isFilePendingOrLoading(file))
              .filter(File::exists)
              .map(file -> LoadUtil.getTsFilePath(file.getAbsolutePath()))
              .filter(this::isTsFileCompleted)
              .limit(currentAllowedPendingSize)
              .forEach(
                  filePath -> {
                    final File tsFile = new File(filePath);
                    final Map<String, String> attributes =
                        ActiveLoadPathHelper.parseAttributes(tsFile, listeningDirFile);

                    final File parentFile = tsFile.getParentFile();
                    final boolean isTableModel =
                        ActiveLoadPathHelper.containsDatabaseName(attributes)
                            || (parentFile != null
                                && !Objects.equals(
                                    parentFile.getAbsoluteFile(),
                                    listeningDirFile.getAbsoluteFile()));

                    activeLoadTsFileLoader.tryTriggerTsFileLoad(
                        tsFile.getAbsolutePath(),
                        listeningDirFile.getAbsolutePath(),
                        isTableModel,
                        isGeneratedByPipe);
                  });
        } catch (UncheckedIOException e) {
          LOGGER.debug(StorageEngineMessages.FILE_DELETED_IGNORE_EXCEPTION);
        } catch (final Exception e) {
          LOGGER.warn(StorageEngineMessages.EXCEPTION_SCANNING_DIR, listeningDir, e);
        }
      }
    }
  }

  private boolean checkPermission(final String listeningDir) {
    try {
      final Path listeningDirPath = new File(listeningDir).toPath();

      if (!Files.isReadable(listeningDirPath)) {
        if (!noPermissionDirs.contains(listeningDir)) {
          LOGGER.error(
              StorageEngineMessages
                  .STORAGE_LOG_CURRENT_DIR_PATH_IS_NOT_READABLE_SKIP_SCANNING_THIS_DIR_9C8B7E00,
              listeningDirPath);
          noPermissionDirs.add(listeningDir);
        }
        return false;
      }

      if (!Files.isWritable(listeningDirPath)) {
        if (!noPermissionDirs.contains(listeningDir)) {
          LOGGER.error(
              StorageEngineMessages
                  .STORAGE_LOG_CURRENT_DIR_PATH_IS_NOT_WRITABLE_SKIP_SCANNING_THIS_DIR_4885E78F,
              listeningDirPath);
          noPermissionDirs.add(listeningDir);
        }
        return false;
      }

      noPermissionDirs.remove(listeningDir);
      return true;
    } catch (final Exception e) {
      LOGGER.error(
          StorageEngineMessages
              .STORAGE_LOG_ERROR_OCCURRED_DURING_CHECKING_R_W_PERMISSION_OF_DIR_SKIP_3EC7FC7D,
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
              LoadUtil.updateLoadDiskSelector();
            }
          }
        }
      } else {
        listeningDirs.clear();
      }
      if (!Objects.equals(
          IOTDB_CONFIG.getLoadActiveListeningPipeDir(), pipeListeningDirConfig.get())) {
        synchronized (this) {
          if (!Objects.equals(
              IOTDB_CONFIG.getLoadActiveListeningPipeDir(), pipeListeningDirConfig.get())) {
            if (pipeListeningDirConfig.get() != null) {
              listeningDirs.remove(pipeListeningDirConfig.get());
            }
            pipeListeningDirConfig.set(IOTDB_CONFIG.getLoadActiveListeningPipeDir());
          }
        }
      }

      // Active load is always enabled for pipe data sync.
      listeningDirs.add(IOTDB_CONFIG.getLoadActiveListeningPipeDir());

      // Create directories if not exists
      listeningDirs.forEach(this::createDirectoriesIfNotExists);

      ActiveLoadingFilesNumberMetricsSet.getInstance().updatePendingDirList(listeningDirs);
      ActiveLoadingFilesSizeMetricsSet.getInstance().updatePendingDirList(listeningDirs);
    } catch (final Exception e) {
      LOGGER.warn(
          StorageEngineMessages
              .STORAGE_LOG_ERROR_OCCURRED_DURING_HOT_RELOAD_ACTIVE_LOAD_DIRS_CURRENT_673AFC0F,
          listeningDirs,
          e);
    }
  }

  private void createDirectoriesIfNotExists(final String dirPath) {
    try {
      FileUtils.forceMkdir(new File(dirPath));
    } catch (final IOException e) {
      LOGGER.warn(StorageEngineMessages.ERROR_CREATING_DIR_FOR_ACTIVE_LOAD, dirPath, e);
    }
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
                  LOGGER.debug(StorageEngineMessages.FAILED_COUNT_ACTIVE_DIRS_FILE_NUMBER, e);
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
      LOGGER.debug(StorageEngineMessages.FAILED_COUNT_ACTIVE_DIRS_FILE_NUMBER, e);
    }

    return totalFileCount;
  }
}
