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
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.queryengine.plan.statement.pipe.PipeEnrichedStatement;
import org.apache.iotdb.db.storageengine.load.converter.PipeTsFileConversionTaskManager;
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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
  // Guarded by activeLoadExecutor, including tasks submitted but not yet started.
  private int activeLoadTasks;
  private final AtomicReference<String> failDir = new AtomicReference<>();
  private final AtomicReference<String> indexedFailDir = new AtomicReference<>();
  private final Map<String, FailedFileSnapshot> failedFileSnapshots = new ConcurrentHashMap<>();
  private final boolean isVerify = IOTDB_CONFIG.isLoadActiveListeningVerifyEnable();

  public int getCurrentAllowedPendingSize() {
    return MAX_PENDING_SIZE - pendingQueue.size();
  }

  public void tryTriggerTsFileLoad(
      String absolutePath, String pendingDir, boolean isTabletMode, boolean isGeneratedByPipe) {
    tryTriggerTsFileLoad(absolutePath, pendingDir, isTabletMode, isGeneratedByPipe, null);
  }

  public void tryTriggerTsFileLoad(
      String absolutePath,
      String pendingDir,
      boolean isTabletMode,
      boolean isGeneratedByPipe,
      String conversionTaskId) {
    if (CommonDescriptor.getInstance().getConfig().isReadOnly()) {
      return;
    }

    if (pendingQueue.enqueue(
        absolutePath, pendingDir, isGeneratedByPipe, isTabletMode, conversionTaskId)) {
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
                StorageEngineMessages
                    .STORAGE_LOG_ERROR_OCCURRED_DURING_CREATING_FAIL_DIRECTORY_FOR_ACTIVE_7D3BEB38,
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

      final WrappedThreadPoolExecutor executor = activeLoadExecutor.get();
      final int targetCorePoolSize =
          Math.min(pendingQueue.size(), IOTDB_CONFIG.getLoadActiveListeningMaxThreadNum());

      if (executor.getCorePoolSize() != targetCorePoolSize) {
        executor.setCorePoolSize(targetCorePoolSize);
      }

      // Executor active counts include a retiring worker, and exclude tasks not started yet.
      // Reserve each loader task here, then release and recheck the queue when it exits.
      while (activeLoadTasks < targetCorePoolSize) {
        activeLoadTasks++;
        try {
          executor.execute(() -> tryLoadPendingTsFiles(executor));
        } catch (final RuntimeException | Error e) {
          activeLoadTasks--;
          throw e;
        }
      }
    }
  }

  public void stop() {
    final WrappedThreadPoolExecutor executor;
    synchronized (activeLoadExecutor) {
      executor = activeLoadExecutor.getAndSet(null);
      activeLoadTasks = 0;
    }
    if (executor == null) {
      pendingQueue.clearPending();
      clearFailedFileIndex();
      return;
    }

    boolean isTerminated = false;
    try {
      executor.shutdownNow();
      isTerminated = executor.awaitTermination(30, TimeUnit.SECONDS);
      if (!isTerminated) {
        LOGGER.warn(
            StorageEngineMessages.STILL_NOT_EXIT_AFTER_30S,
            ThreadName.ACTIVE_LOAD_TSFILE_LOADER.getName());
      }
    } catch (final InterruptedException e) {
      LOGGER.warn(
          StorageEngineMessages.STILL_NOT_EXIT_AFTER_30S,
          ThreadName.ACTIVE_LOAD_TSFILE_LOADER.getName());
      Thread.currentThread().interrupt();
    } finally {
      if (isTerminated) {
        pendingQueue.clear();
      } else {
        pendingQueue.clearPending();
      }
      clearFailedFileIndex();
    }
  }

  private void tryLoadPendingTsFiles(final WrappedThreadPoolExecutor executor) {
    try {
      tryLoadPendingTsFiles();
    } finally {
      synchronized (activeLoadExecutor) {
        // A stopped executor must not restart the loader or change a replacement's task count.
        if (activeLoadExecutor.get() == executor) {
          activeLoadTasks--;
          adjustExecutorIfNecessary();
        }
      }
    }
  }

  private void clearFailedFileIndex() {
    failedFileSnapshots.clear();
    indexedFailDir.set(null);
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
                StorageEngineMessages
                    .STORAGE_LOG_SUCCESSFULLY_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_ADB5FEC9,
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
          cleanupEmptyDirectories(loadEntry.get());
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

    while (!Thread.currentThread().isInterrupted()) {
      final ActiveLoadPendingQueue.ActiveLoadEntry entry = pendingQueue.dequeueFromPending();
      if (Objects.nonNull(entry)) {
        return Optional.of(entry);
      }

      LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
      if (Thread.currentThread().isInterrupted()) {
        return Optional.empty();
      }

      if (currentRetryTimes++ >= maxRetryTimes) {
        return Optional.empty();
      }
    }
    return Optional.empty();
  }

  private TSStatus loadTsFile(
      final ActiveLoadPendingQueue.ActiveLoadEntry entry, final IClientSession session)
      throws FileNotFoundException {
    final File tsFile = new File(entry.getFile());
    final LoadTsFileStatement statement =
        LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath());
    final List<File> files = statement.getTsFiles();

    statement.setDeleteAfterLoad(true);
    statement.setAutoCreateDatabase(
        IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled());

    final File pendingDir =
        entry.getPendingDir() == null
            ? ActiveLoadPathHelper.findPendingDirectory(tsFile)
            : new File(entry.getPendingDir());
    final Map<String, String> attributes = ActiveLoadPathHelper.parseAttributes(tsFile, pendingDir);
    final String conversionTaskId = entry.getConversionTaskId();
    PipeTsFileConversionTaskManager.registerIfAbsent(conversionTaskId);
    PipeTsFileConversionTaskManager.markReceiverOwned(conversionTaskId);
    PipeTsFileConversionTaskManager.markRunning(conversionTaskId);
    PipeTsFileConversionTaskManager.enter(conversionTaskId);
    try {
      ActiveLoadPathHelper.applyAttributesToStatement(attributes, statement, isVerify);
      final String userName =
          attributes.getOrDefault(ActiveLoadPathHelper.USER_KEY, AuthorityChecker.SUPER_USER);
      final Optional<Long> userId = AuthorityChecker.getUserId(userName);
      if (!userId.isPresent()) {
        PipeTsFileConversionTaskManager.markFailed(
            conversionTaskId, new TSStatus(TSStatusCode.USER_NOT_EXIST.getStatusCode()));
        return new TSStatus(TSStatusCode.USER_NOT_EXIST.getStatusCode())
            .setMessage(StorageEngineMessages.USER_IN_ACTIVE_LOAD_PATH_DOES_NOT_EXIST);
      }
      session.setUserId(userId.get());
      session.setUsername(userName);

      final File parentFile;
      if (statement.getDatabase() == null && entry.isTableModel()) {
        statement.setDatabase(
            files.isEmpty() || (parentFile = files.get(0).getParentFile()) == null
                ? null
                : parentFile.getName());
      }

      final TSStatus result =
          executeStatement(
              entry.isGeneratedByPipe() ? new PipeEnrichedStatement(statement) : statement,
              session);
      if (result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
          || result.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        PipeTsFileConversionTaskManager.markSuccess(conversionTaskId);
      } else {
        PipeTsFileConversionTaskManager.markPaused(conversionTaskId, result);
      }
      return result;
    } finally {
      PipeTsFileConversionTaskManager.leave();
    }
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
              false,
              statement.isDebug())
          .status;
    } finally {
      SESSION_MANAGER.removeCurrSession();
    }
  }

  private void handleLoadFailure(
      final ActiveLoadPendingQueue.ActiveLoadEntry entry, final TSStatus status) {
    if (!ActiveLoadFailedMessageHandler.isStatusShouldRetry(entry, status)) {
      PipeTsFileConversionTaskManager.markFailed(entry.getConversionTaskId(), status);
      LOGGER.warn(
          StorageEngineMessages
              .STORAGE_LOG_FAILED_TO_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_STATUS_FILE_F43E9EF7,
          entry.getFile(),
          entry.isGeneratedByPipe(),
          status);
      removeFileAndResourceAndModsToFailDir(entry);
    }
  }

  private void handleFileNotFoundException(final ActiveLoadPendingQueue.ActiveLoadEntry entry) {
    PipeTsFileConversionTaskManager.markFailed(
        entry.getConversionTaskId(), new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode()));
    LOGGER.warn(
        StorageEngineMessages
            .STORAGE_LOG_FAILED_TO_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_DUE_TO_FILE_5EE1FA08,
        entry.getFile(),
        entry.isGeneratedByPipe());
    removeFileAndResourceAndModsToFailDir(entry);
  }

  private void handleOtherException(
      final ActiveLoadPendingQueue.ActiveLoadEntry entry, final Exception e) {
    if (ActiveLoadFailedMessageHandler.isExceptionMessageShouldRetry(entry, e.getMessage())) {
      PipeTsFileConversionTaskManager.markPaused(
          entry.getConversionTaskId(),
          new TSStatus(TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
              .setMessage(e.getMessage()));
    } else {
      PipeTsFileConversionTaskManager.markFailed(
          entry.getConversionTaskId(),
          new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode()).setMessage(e.getMessage()));
      LOGGER.warn(
          StorageEngineMessages
              .STORAGE_LOG_FAILED_TO_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_BECAUSE_OF_07946D74,
          entry.getFile(),
          entry.isGeneratedByPipe(),
          e);
      removeFileAndResourceAndModsToFailDir(entry);
    }
  }

  private synchronized void removeFileAndResourceAndModsToFailDir(
      final ActiveLoadPendingQueue.ActiveLoadEntry entry) {
    final String filePath = entry.getFile();
    final File tsFile = new File(filePath);
    final List<File> sourceFiles =
        List.of(
            tsFile,
            new File(LoadUtil.getTsFileResourcePath(filePath)),
            new File(LoadUtil.getTsFileModsV1Path(filePath)),
            new File(LoadUtil.getTsFileModsV2Path(filePath)));
    final boolean hasSourceFiles = sourceFiles.stream().anyMatch(File::exists);
    if (!hasSourceFiles) {
      return;
    }

    final File failDirectory = new File(failDir.get());
    final String failureInfoSuffix = ".load-failure";
    final String failureInfoFileName =
        tsFile.getName().getBytes(StandardCharsets.UTF_8).length + failureInfoSuffix.length() > 180
            ? "active-load-" + UUID.randomUUID() + failureInfoSuffix
            : tsFile.getName() + failureInfoSuffix;
    File stagingDirectory = null;
    final File targetDirectory = new File(failDirectory, "active-load-" + UUID.randomUUID());
    try {
      FileUtils.forceMkdir(failDirectory);
      stagingDirectory =
          Files.createTempDirectory(failDirectory.toPath(), ".active-load-staging-").toFile();

      for (final File sourceFile : sourceFiles) {
        if (!sourceFile.exists()) {
          continue;
        }
        Files.copy(
            sourceFile.toPath(),
            new File(stagingDirectory, sourceFile.getName()).toPath(),
            StandardCopyOption.COPY_ATTRIBUTES);
      }
      writeFailureInfo(new File(stagingDirectory, failureInfoFileName), entry, true);

      try {
        Files.move(
            stagingDirectory.toPath(), targetDirectory.toPath(), StandardCopyOption.ATOMIC_MOVE);
      } catch (final AtomicMoveNotSupportedException e) {
        Files.move(stagingDirectory.toPath(), targetDirectory.toPath());
      }
      stagingDirectory = null;
    } catch (final IOException e) {
      if (targetDirectory.exists()) {
        try {
          FileUtils.deleteDirectory(targetDirectory);
        } catch (final IOException cleanupException) {
          e.addSuppressed(cleanupException);
        }
      }
      if (stagingDirectory != null && stagingDirectory.exists()) {
        try {
          FileUtils.deleteDirectory(stagingDirectory);
        } catch (final IOException cleanupException) {
          e.addSuppressed(cleanupException);
        }
      }
      LOGGER.warn(StorageEngineMessages.ERROR_MOVING_FILE_TO_FAIL_DIR, filePath, e);
      return;
    }

    deletePublishedSourceFiles(filePath, tsFile, sourceFiles, targetDirectory);
  }

  private void deletePublishedSourceFiles(
      final String filePath,
      final File tsFile,
      final List<File> sourceFiles,
      final File publishedGroup) {
    try {
      Files.deleteIfExists(tsFile.toPath());
    } catch (final IOException e) {
      if (tsFile.exists()) {
        pendingQueue.markFileFailed(filePath);
        failedFileSnapshots.put(filePath, new FailedFileSnapshot(publishedGroup));
      }
      LOGGER.warn(StorageEngineMessages.ERROR_MOVING_FILE_TO_FAIL_DIR, filePath, e);
      return;
    }

    IOException cleanupException = null;
    for (final File sourceFile : sourceFiles) {
      if (sourceFile.equals(tsFile)) {
        continue;
      }
      try {
        Files.deleteIfExists(sourceFile.toPath());
      } catch (final IOException e) {
        if (cleanupException == null) {
          cleanupException = e;
        } else {
          cleanupException.addSuppressed(e);
        }
      }
    }
    if (cleanupException != null) {
      LOGGER.warn(StorageEngineMessages.ERROR_MOVING_FILE_TO_FAIL_DIR, filePath, cleanupException);
    }
  }

  private void writeFailureInfo(
      final File failureInfoFile,
      final ActiveLoadPendingQueue.ActiveLoadEntry entry,
      final boolean hasFiles)
      throws IOException {
    if (!hasFiles) {
      return;
    }

    final Properties properties = new Properties();
    properties.setProperty("activeLoadFile", entry.getFile());
    properties.setProperty("generatedByPipe", Boolean.toString(entry.isGeneratedByPipe()));
    properties.setProperty("tableModel", Boolean.toString(entry.isTableModel()));
    if (entry.getConversionTaskId() != null) {
      properties.setProperty("conversionTaskId", entry.getConversionTaskId());
    }
    if (entry.getPendingDir() != null) {
      properties.setProperty("pendingDir", entry.getPendingDir());
    }
    try (final OutputStream outputStream = Files.newOutputStream(failureInfoFile.toPath())) {
      properties.store(outputStream, "Active LOAD failure source");
    }
  }

  private void cleanupEmptyDirectories(final ActiveLoadPendingQueue.ActiveLoadEntry entry) {
    final File pendingDir =
        entry.getPendingDir() == null
            ? ActiveLoadPathHelper.findPendingDirectory(new File(entry.getFile()))
            : new File(entry.getPendingDir());
    if (pendingDir == null) {
      return;
    }

    final Path pendingPath = pendingDir.toPath().toAbsolutePath().normalize();
    Path currentPath = new File(entry.getFile()).toPath().toAbsolutePath().normalize().getParent();
    while (currentPath != null
        && currentPath.startsWith(pendingPath)
        && !currentPath.equals(pendingPath)) {
      try {
        Files.delete(currentPath);
      } catch (final IOException e) {
        if (Files.exists(currentPath)) {
          LOGGER.debug(StorageEngineMessages.FAILED_DELETE_FOLDER_CLEANING_UP, currentPath, e);
        }
        return;
      }
      currentPath = currentPath.getParent();
    }
  }

  public boolean isFilePendingOrLoading(final File file) {
    final String filePath = file.getAbsolutePath();
    if (!file.exists()) {
      pendingQueue.clearFailedFile(filePath);
      failedFileSnapshots.remove(filePath);
    } else {
      indexPublishedFailedFilesIfNecessary();
      final FailedFileSnapshot failedFileSnapshot = failedFileSnapshots.get(filePath);
      if (failedFileSnapshot != null && !failedFileSnapshot.matches(file)) {
        pendingQueue.clearFailedFile(filePath);
        failedFileSnapshots.remove(filePath, failedFileSnapshot);
      }
    }
    return pendingQueue.isFilePendingOrLoading(filePath);
  }

  private void indexPublishedFailedFilesIfNecessary() {
    final String configuredFailDir = IOTDB_CONFIG.getLoadActiveListeningFailDir();
    if (configuredFailDir == null || configuredFailDir.isEmpty()) {
      return;
    }
    if (Objects.equals(indexedFailDir.get(), configuredFailDir)) {
      return;
    }

    synchronized (indexedFailDir) {
      if (Objects.equals(indexedFailDir.get(), configuredFailDir)) {
        return;
      }

      final File[] failedGroups = new File(configuredFailDir).listFiles(File::isDirectory);
      if (failedGroups == null) {
        return;
      }

      boolean scanCompleted = true;
      for (final File failedGroup : failedGroups) {
        if (!failedGroup.getName().startsWith("active-load-")) {
          continue;
        }
        final File[] failureInfoFiles =
            failedGroup.listFiles((dir, name) -> name.endsWith(".load-failure"));
        if (failureInfoFiles == null) {
          scanCompleted = false;
          continue;
        }
        for (final File failureInfoFile : failureInfoFiles) {
          final Properties failureInfo = new Properties();
          try (final InputStream inputStream = Files.newInputStream(failureInfoFile.toPath())) {
            failureInfo.load(inputStream);
            final String activeLoadFile = failureInfo.getProperty("activeLoadFile");
            if (activeLoadFile == null) {
              continue;
            }
            final String absoluteSourceFile = new File(activeLoadFile).getAbsolutePath();
            final File sourceTsFile = new File(absoluteSourceFile);
            if (sourceTsFile.isFile() && matchesPublishedFileGroup(sourceTsFile, failedGroup)) {
              pendingQueue.markFileFailed(absoluteSourceFile);
              failedFileSnapshots.put(absoluteSourceFile, new FailedFileSnapshot(failedGroup));
            }
          } catch (final IOException e) {
            scanCompleted = false;
            LOGGER.debug(
                StorageEngineMessages.LOG_FAILED_TO_READ_ACTIVE_LOAD_FAILURE_METADATA_ARG_E152544E,
                failureInfoFile,
                e);
          } catch (final RuntimeException e) {
            LOGGER.debug(
                StorageEngineMessages.LOG_FAILED_TO_READ_ACTIVE_LOAD_FAILURE_METADATA_ARG_E152544E,
                failureInfoFile,
                e);
          }
        }
      }
      if (scanCompleted) {
        indexedFailDir.set(configuredFailDir);
      }
    }
  }

  private static boolean matchesPublishedFileGroup(
      final File sourceTsFile, final File failedGroup) {
    for (final File sourceFile : getTsFileGroupFiles(sourceTsFile)) {
      final File failedFile = new File(failedGroup, sourceFile.getName());
      if (sourceFile.exists() != failedFile.exists()) {
        return false;
      }
      if (sourceFile.exists()) {
        try {
          if (Files.mismatch(sourceFile.toPath(), failedFile.toPath()) != -1) {
            return false;
          }
        } catch (final IOException e) {
          return false;
        }
      }
    }
    return true;
  }

  private static List<File> getTsFileGroupFiles(final File tsFile) {
    final String filePath = tsFile.getAbsolutePath();
    return List.of(
        tsFile,
        new File(LoadUtil.getTsFileResourcePath(filePath)),
        new File(LoadUtil.getTsFileModsV1Path(filePath)),
        new File(LoadUtil.getTsFileModsV2Path(filePath)));
  }

  private static class FailedFileSnapshot {
    private final File publishedGroup;

    private FailedFileSnapshot(final File publishedGroup) {
      this.publishedGroup = publishedGroup;
    }

    private boolean matches(final File tsFile) {
      // A replacement can preserve both size and mtime (for example, a restored sidecar).
      // Only unchanged contents should remain suppressed.
      return matchesPublishedFileGroup(tsFile, publishedGroup);
    }
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
                LOGGER.debug(StorageEngineMessages.FAILED_COUNT_FILES_IN_FAIL_DIR, e);
              }
              return FileVisitResult.CONTINUE;
            }
          });

      ActiveLoadingFilesNumberMetricsSet.getInstance().updateTotalFailedFileCounter(fileCount[0]);
      ActiveLoadingFilesSizeMetricsSet.getInstance().updateTotalFailedFileCounter(fileSize[0]);
    } catch (final IOException e) {
      LOGGER.debug(StorageEngineMessages.FAILED_COUNT_FILES_IN_FAIL_DIR, e);
    }

    return fileCount[0];
  }
}
