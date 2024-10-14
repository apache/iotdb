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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.concurrent.IoTThreadFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.WrappedThreadPoolExecutor;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.queryengine.plan.statement.pipe.PipeEnrichedStatement;
import org.apache.iotdb.db.storageengine.load.metrics.ActiveLoadingFilesNumberMetricsSet;
import org.apache.iotdb.db.storageengine.load.metrics.ActiveLoadingFilesSizeMetricsSet;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.ZoneId;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

public class ActiveLoadTsFileLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(ActiveLoadTsFileLoader.class);

  private static final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private static final int MAX_PENDING_SIZE = 1000;
  private final ActiveLoadPendingQueue pendingQueue = new ActiveLoadPendingQueue();

  private final AtomicReference<WrappedThreadPoolExecutor> activeLoadExecutor =
      new AtomicReference<>();
  private final AtomicReference<String> failDir = new AtomicReference<>();

  /** A constant representing the default value for the tree model's database name. */
  private static final String EMPTY_DATA_BASE = "";

  /**
   * A thread-safe mapping of TS file paths to their corresponding database names. This mapping is
   * used to keep track of which database a TS file is associated with. The database name can be in
   * one of three states:
   *
   * <ul>
   *   <li>{@code null}: Indicates that the database name has not been initialized.
   *   <li>{@link #EMPTY_DATA_BASE ""}: Indicates that the tree model is being used.
   *   <li>Non-empty String: Contains the name of the database when the table model is being used.
   * </ul>
   */
  private final ConcurrentHashMap<String, String> tsFileDatabaseNameMapping =
      new ConcurrentHashMap<>();

  public int getCurrentAllowedPendingSize() {
    return MAX_PENDING_SIZE - pendingQueue.size();
  }

  public void tryTriggerTsFileLoad(String absolutePath, boolean isGeneratedByPipe) {
    if (CommonDescriptor.getInstance().getConfig().isReadOnly()) {
      return;
    }

    if (pendingQueue.enqueue(absolutePath, isGeneratedByPipe)) {
      initFailDirIfNecessary();
      adjustExecutorIfNecessary();
    }
  }

  private void initFailDirIfNecessary() {
    if (!Objects.equals(failDir.get(), IOTDB_CONFIG.getLoadActiveListeningFailDir())) {
      synchronized (failDir) {
        if (!Objects.equals(failDir.get(), IOTDB_CONFIG.getLoadActiveListeningFailDir())) {
          final File failDirFile = new File(IOTDB_CONFIG.getLoadActiveListeningFailDir());
          try {
            FileUtils.forceMkdir(failDirFile);
          } catch (final IOException e) {
            LOGGER.warn(
                "Error occurred during creating fail directory {} for active load.",
                failDirFile.getAbsoluteFile(),
                e);
          }
          failDir.set(IOTDB_CONFIG.getLoadActiveListeningFailDir());

          ActiveLoadingFilesSizeMetricsSet.getInstance().updateFailedDir(failDir.get());
          ActiveLoadingFilesNumberMetricsSet.getInstance().updateFailedDir(failDir.get());
        }
      }
    }
  }

  private void adjustExecutorIfNecessary() {
    if (activeLoadExecutor.get() == null) {
      synchronized (activeLoadExecutor) {
        if (activeLoadExecutor.get() == null) {
          activeLoadExecutor.set(
              new WrappedThreadPoolExecutor(
                  IOTDB_CONFIG.getLoadActiveListeningMaxThreadNum(),
                  IOTDB_CONFIG.getLoadActiveListeningMaxThreadNum(),
                  0L,
                  TimeUnit.SECONDS,
                  new LinkedBlockingQueue<>(),
                  new IoTThreadFactory(ThreadName.ACTIVE_LOAD_TSFILE_LOADER.name()),
                  ThreadName.ACTIVE_LOAD_TSFILE_LOADER.name()));
        }
      }
    }

    final int targetCorePoolSize =
        Math.min(pendingQueue.size(), IOTDB_CONFIG.getLoadActiveListeningMaxThreadNum());

    if (activeLoadExecutor.get().getCorePoolSize() != targetCorePoolSize) {
      activeLoadExecutor.get().setCorePoolSize(targetCorePoolSize);
    }

    // calculate how many threads need to be loaded
    final int threadsToBeAdded =
        Math.max(targetCorePoolSize - activeLoadExecutor.get().getActiveCount(), 0);
    for (int i = 0; i < threadsToBeAdded; i++) {
      activeLoadExecutor.get().execute(this::tryLoadPendingTsFiles);
    }
  }

  private void tryLoadPendingTsFiles() {
    while (true) {
      final Optional<Pair<String, Boolean>> filePair = tryGetNextPendingFile();
      if (!filePair.isPresent()) {
        return;
      }

      try {
        final TSStatus result = loadTsFile(filePair.get());
        if (result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
            || result.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
          LOGGER.info(
              "Successfully auto load tsfile {} (isGeneratedByPipe = {})",
              filePair.get().getLeft(),
              filePair.get().getRight());
          removePipeGeneratedTsFileMapping(filePair.get());
        } else {
          handleLoadFailure(filePair.get(), result);
        }
      } catch (final FileNotFoundException e) {
        handleFileNotFoundException(filePair.get());
      } catch (final Exception e) {
        handleOtherException(filePair.get(), e);
      } finally {
        pendingQueue.removeFromLoading(filePair.get().getLeft());
      }
    }
  }

  private Optional<Pair<String, Boolean>> tryGetNextPendingFile() {
    final long maxRetryTimes =
        Math.max(1, IOTDB_CONFIG.getLoadActiveListeningCheckIntervalSeconds() << 1);
    long currentRetryTimes = 0;

    while (true) {
      final Pair<String, Boolean> filePair = pendingQueue.dequeueFromPending();
      if (Objects.nonNull(filePair)) {
        return Optional.of(filePair);
      }

      LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));

      if (currentRetryTimes++ >= maxRetryTimes) {
        return Optional.empty();
      }
    }
  }

  private TSStatus loadTsFile(final Pair<String, Boolean> filePair) throws FileNotFoundException {
    // Tree model
    final LoadTsFileStatement statement = new LoadTsFileStatement(filePair.getLeft());
    statement.setDeleteAfterLoad(true);
    statement.setVerifySchema(true);
    statement.setAutoCreateDatabase(false);
    return executeStatement(filePair.getRight() ? new PipeEnrichedStatement(statement) : statement);
  }

  private TSStatus executeStatement(final Statement statement) {
    return Coordinator.getInstance()
        .executeForTreeModel(
            statement,
            SessionManager.getInstance().requestQueryId(),
            new SessionInfo(0, AuthorityChecker.SUPER_USER, ZoneId.systemDefault()),
            "",
            ClusterPartitionFetcher.getInstance(),
            ClusterSchemaFetcher.getInstance(),
            IOTDB_CONFIG.getQueryTimeoutThreshold())
        .status;
  }

  private void handleLoadFailure(final Pair<String, Boolean> filePair, final TSStatus status) {
    if (status.getMessage() != null && status.getMessage().contains("memory")) {
      LOGGER.info(
          "Rejecting auto load tsfile {} (isGeneratedByPipe = {}) due to memory constraints, will retry later.",
          filePair.getLeft(),
          filePair.getRight());
    } else if (status.getMessage() != null && status.getMessage().contains("read only")) {
      LOGGER.info(
          "Rejecting auto load tsfile {} (isGeneratedByPipe = {}) due to the system is read only, will retry later.",
          filePair.getLeft(),
          filePair.getRight());
    } else {
      LOGGER.warn(
          "Failed to auto load tsfile {} (isGeneratedByPipe = {}), status: {}. File will be moved to fail directory.",
          filePair.getLeft(),
          filePair.getRight(),
          status);
      removeFileAndResourceAndModsToFailDir(filePair);
    }
  }

  private void handleFileNotFoundException(final Pair<String, Boolean> filePair) {
    LOGGER.warn(
        "Failed to auto load tsfile {} (isGeneratedByPipe = {}) due to file not found, will skip this file.",
        filePair.getLeft(),
        filePair.getRight());
    removeFileAndResourceAndModsToFailDir(filePair);
  }

  private void handleOtherException(final Pair<String, Boolean> filePair, final Exception e) {
    if (e.getMessage() != null && e.getMessage().contains("memory")) {
      LOGGER.info(
          "Rejecting auto load tsfile {} (isGeneratedByPipe = {}) due to memory constraints, will retry later.",
          filePair.getLeft(),
          filePair.getRight());
    } else if (e.getMessage() != null && e.getMessage().contains("read only")) {
      LOGGER.info(
          "Rejecting auto load tsfile {} (isGeneratedByPipe = {}) due to the system is read only, will retry later.",
          filePair.getLeft(),
          filePair.getRight());
    } else {
      LOGGER.warn(
          "Failed to auto load tsfile {} (isGeneratedByPipe = {}) because of an unexpected exception. File will be moved to fail directory.",
          filePair.getLeft(),
          filePair.getRight(),
          e);
      removeFileAndResourceAndModsToFailDir(filePair);
    }
  }

  private void removeFileAndResourceAndModsToFailDir(final Pair<String, Boolean> filePair) {
    final String filePath = filePair.getLeft();
    removeToFailDir(filePath);
    removeToFailDir(filePath + ".resource");
    removeToFailDir(filePath + ".mods");
    removePipeGeneratedTsFileMapping(filePair);
  }

  private void removeToFailDir(final String filePath) {
    final File sourceFile = new File(filePath);
    // prevent the resource or mods not exist
    if (!sourceFile.exists()) {
      return;
    }

    final File targetDir = new File(failDir.get());
    try {
      org.apache.iotdb.commons.utils.FileUtils.moveFileWithMD5Check(sourceFile, targetDir);
    } catch (final IOException e) {
      LOGGER.warn("Error occurred during moving file {} to fail directory.", filePath, e);
    }
  }

  public boolean isFilePendingOrLoading(final String filePath) {
    return pendingQueue.isFilePendingOrLoading(filePath);
  }

  // Metrics
  public long countAndReportFailedFileNumber() {
    final long[] fileCount = {0};
    final long[] fileSize = {0};

    try {
      initFailDirIfNecessary();
      Files.walkFileTree(
          new File(failDir.get()).toPath(),
          new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
              fileCount[0]++;
              try {
                fileSize[0] += file.toFile().length();
              } catch (Exception e) {
                LOGGER.debug("Failed to count failed files in fail directory.", e);
              }
              return FileVisitResult.CONTINUE;
            }
          });

      ActiveLoadingFilesNumberMetricsSet.getInstance().updateTotalFailedFileCounter(fileCount[0]);
      ActiveLoadingFilesSizeMetricsSet.getInstance().updateTotalFailedFileCounter(fileSize[0]);
    } catch (final IOException e) {
      LOGGER.debug("Failed to count failed files in fail directory.", e);
    }

    return fileCount[0];
  }

  /**
   * Moves a provided TSFile to the active load listening pipe directory for asynchronous loading.
   * After moving the file, it updates the mapping of TSFiles to their corresponding database names.
   * If the TSFile is already in the target directory, no action is taken, and a log is recorded.
   *
   * @param sourceFile The TSFile to be moved for asynchronous loading.
   * @param dataBaseName The name of the database associated with the TSFile.
   * @throws IOException If an I/O error occurs during the file moving process.
   */
  public void pipeReceiverMoveTsFileForAsyncLoad(
      File sourceFile, boolean isModFile, String dataBaseName) throws IOException {
    final String loadActiveListeningPipeDir = IOTDB_CONFIG.getLoadActiveListeningPipeDir();
    if (sourceFile == null || !sourceFile.exists() || sourceFile.isDirectory()) {
      LOGGER.warn("The provided TSFile is null, does not exist, or is a directory: {}", sourceFile);
      return;
    }
    if (!Objects.equals(loadActiveListeningPipeDir, sourceFile.getParentFile().getAbsolutePath())) {
      org.apache.iotdb.commons.utils.FileUtils.moveFileWithMD5Check(
          sourceFile, new File(loadActiveListeningPipeDir));
      if (!isModFile) {
        tsFileDatabaseNameMapping.put(
            new File(loadActiveListeningPipeDir, sourceFile.getName()).getAbsolutePath(),
            dataBaseName == null ? EMPTY_DATA_BASE : dataBaseName);
      }
    }
    LOGGER.info(
        "The TSFile '{}' already exists in the target directory '{}'.",
        sourceFile.getName(),
        loadActiveListeningPipeDir);
  }

  /**
   * Removes the mapping of a TSFile to its database name if it was generated by the pipe. This
   * method is called to clean up mappings for files that were generated by the pipe. If a TSFile
   * needs to be reloaded, its mapping should be preserved and not removed.
   *
   * @param filePair A pair containing the file path and a flag indicating whether it's generated by
   *     a pipe.
   */
  private void removePipeGeneratedTsFileMapping(Pair<String, Boolean> filePair) {
    final String filePath = filePair.getLeft();
    final Boolean isGeneratedByPipe = filePair.getRight();
    if (isGeneratedByPipe == null) {
      LOGGER.warn("The isGeneratedByPipe flag for file '{}' has not been initialized.", filePath);
      return;
    }
    if (isGeneratedByPipe) {
      tsFileDatabaseNameMapping.remove(filePath);
    }
  }
}
