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
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.queryengine.plan.statement.pipe.PipeEnrichedStatement;
import org.apache.iotdb.db.storageengine.load.metrics.ActiveLoadingFilesNumberMetricsSet;
import org.apache.iotdb.db.storageengine.load.metrics.ActiveLoadingFilesSizeMetricsSet;
import org.apache.iotdb.db.storageengine.load.util.LoadUtil;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.external.commons.io.FileUtils;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

public class ActiveLoadTsFileLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(ActiveLoadTsFileLoader.class);

  private static final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private static final int MAX_PENDING_SIZE = 1000;
  private final ActiveLoadPendingQueue pendingQueue = new ActiveLoadPendingQueue();

  private final AtomicReference<WrappedThreadPoolExecutor> activeLoadExecutor =
      new AtomicReference<>();
  private final AtomicReference<String> failDir = new AtomicReference<>();
  private final boolean isVerify = IOTDB_CONFIG.isLoadActiveListeningVerifyEnable();

  public int getCurrentAllowedPendingSize() {
    return MAX_PENDING_SIZE - pendingQueue.size();
  }

  public void tryTriggerTsFileLoad(
      String absolutePath, String pendingDir, boolean isTabletMode, boolean isGeneratedByPipe) {
    if (CommonDescriptor.getInstance().getConfig().isReadOnly()) {
      return;
    }

    if (pendingQueue.enqueue(absolutePath, pendingDir, isGeneratedByPipe, isTabletMode)) {
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
            RetryUtils.retryOnException(
                () -> {
                  FileUtils.forceMkdir(failDirFile);
                  return null;
                });
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
    final IClientSession session =
        new InternalClientSession(
            String.format(
                "%s_%s",
                ActiveLoadTsFileLoader.class.getSimpleName(), Thread.currentThread().getName()));
    session.setUsername(AuthorityChecker.SUPER_USER);
    session.setClientVersion(IoTDBConstant.ClientVersion.V_1_0);
    session.setZoneId(ZoneId.systemDefault());

    try {
      while (true) {
        final Optional<ActiveLoadPendingQueue.ActiveLoadEntry> loadEntry = tryGetNextPendingFile();
        if (!loadEntry.isPresent()) {
          return;
        }

        try {
          final TSStatus result = loadTsFile(loadEntry.get(), session);
          if (result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
              || result.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
            LOGGER.info(
                "Successfully auto load tsfile {} (isGeneratedByPipe = {})",
                loadEntry.get().getFile(),
                loadEntry.get().isGeneratedByPipe());
          } else {
            handleLoadFailure(loadEntry.get(), result);
          }
        } catch (final FileNotFoundException e) {
          handleFileNotFoundException(loadEntry.get());
        } catch (final Exception e) {
          handleOtherException(loadEntry.get(), e);
        } finally {
          pendingQueue.removeFromLoading(loadEntry.get().getFile());
        }
      }
    } finally {
      SESSION_MANAGER.closeSession(session, Coordinator.getInstance()::cleanupQueryExecution);
    }
  }

  private Optional<ActiveLoadPendingQueue.ActiveLoadEntry> tryGetNextPendingFile() {
    final long maxRetryTimes =
        Math.max(1, IOTDB_CONFIG.getLoadActiveListeningCheckIntervalSeconds() << 1);
    long currentRetryTimes = 0;

    while (true) {
      final ActiveLoadPendingQueue.ActiveLoadEntry entry = pendingQueue.dequeueFromPending();
      if (Objects.nonNull(entry)) {
        return Optional.of(entry);
      }

      LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));

      if (currentRetryTimes++ >= maxRetryTimes) {
        return Optional.empty();
      }
    }
  }

  private TSStatus loadTsFile(
      final ActiveLoadPendingQueue.ActiveLoadEntry entry, final IClientSession session)
      throws FileNotFoundException {
    final File tsFile = new File(entry.getFile());
    final LoadTsFileStatement statement = new LoadTsFileStatement(tsFile.getAbsolutePath());
    final List<File> files = statement.getTsFiles();

    statement.setDeleteAfterLoad(true);
    statement.setAutoCreateDatabase(
        IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled());

    final File pendingDir =
        entry.getPendingDir() == null
            ? ActiveLoadPathHelper.findPendingDirectory(tsFile)
            : new File(entry.getPendingDir());
    final Map<String, String> attributes = ActiveLoadPathHelper.parseAttributes(tsFile, pendingDir);
    ActiveLoadPathHelper.applyAttributesToStatement(attributes, statement, isVerify);

    final File parentFile;
    if (statement.getDatabase() == null && entry.isTableModel()) {
      statement.setDatabase(
          files.isEmpty() || (parentFile = files.get(0).getParentFile()) == null
              ? null
              : parentFile.getName());
    }

    return executeStatement(
        entry.isGeneratedByPipe() ? new PipeEnrichedStatement(statement) : statement, session);
  }

  private TSStatus executeStatement(final Statement statement, final IClientSession session) {
    SESSION_MANAGER.registerSession(session);
    try {
      return Coordinator.getInstance()
          .executeForTreeModel(
              statement,
              SESSION_MANAGER.requestQueryId(),
              SESSION_MANAGER.getSessionInfo(session),
              "",
              ClusterPartitionFetcher.getInstance(),
              ClusterSchemaFetcher.getInstance(),
              IOTDB_CONFIG.getQueryTimeoutThreshold(),
              false)
          .status;
    } finally {
      SESSION_MANAGER.removeCurrSession();
    }
  }

  private void handleLoadFailure(
      final ActiveLoadPendingQueue.ActiveLoadEntry entry, final TSStatus status) {
    if (!ActiveLoadFailedMessageHandler.isExceptionMessageShouldRetry(entry, status.getMessage())) {
      LOGGER.warn(
          "Failed to auto load tsfile {} (isGeneratedByPipe = {}), status: {}. File will be moved to fail directory.",
          entry.getFile(),
          entry.isGeneratedByPipe(),
          status);
      removeFileAndResourceAndModsToFailDir(entry.getFile());
    }
  }

  private void handleFileNotFoundException(final ActiveLoadPendingQueue.ActiveLoadEntry entry) {
    LOGGER.warn(
        "Failed to auto load tsfile {} (isGeneratedByPipe = {}) due to file not found, will skip this file.",
        entry.getFile(),
        entry.isGeneratedByPipe());
    removeFileAndResourceAndModsToFailDir(entry.getFile());
  }

  private void handleOtherException(
      final ActiveLoadPendingQueue.ActiveLoadEntry entry, final Exception e) {
    if (!ActiveLoadFailedMessageHandler.isExceptionMessageShouldRetry(entry, e.getMessage())) {
      LOGGER.warn(
          "Failed to auto load tsfile {} (isGeneratedByPipe = {}) because of an unexpected exception. File will be moved to fail directory.",
          entry.getFile(),
          entry.isGeneratedByPipe(),
          e);
      removeFileAndResourceAndModsToFailDir(entry.getFile());
    }
  }

  private void removeFileAndResourceAndModsToFailDir(final String filePath) {
    removeToFailDir(filePath);
    removeToFailDir(LoadUtil.getTsFileResourcePath(filePath));
    removeToFailDir(LoadUtil.getTsFileModsV1Path(filePath));
    removeToFailDir(LoadUtil.getTsFileModsV2Path(filePath));
  }

  private void removeToFailDir(final String filePath) {
    final File sourceFile = new File(filePath);
    // prevent the resource or mods not exist
    if (!sourceFile.exists()) {
      return;
    }

    final File targetDir = new File(failDir.get());
    try {
      RetryUtils.retryOnException(
          () -> {
            org.apache.iotdb.commons.utils.FileUtils.moveFileWithMD5Check(sourceFile, targetDir);
            return null;
          });
    } catch (final IOException e) {
      LOGGER.warn("Error occurred during moving file {} to fail directory.", filePath, e);
    }
  }

  public boolean isFilePendingOrLoading(final File file) {
    return pendingQueue.isFilePendingOrLoading(file.getAbsolutePath());
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
}
