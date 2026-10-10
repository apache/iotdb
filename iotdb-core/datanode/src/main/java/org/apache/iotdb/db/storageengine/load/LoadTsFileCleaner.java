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

package org.apache.iotdb.db.storageengine.load;

import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusOp;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.ModificationFileV1;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Singleton cleanup service for staged LOAD TsFile directories. Periodically cleans up completed or
 * aborted staged directories once WAL watermarks permit.
 */
public class LoadTsFileCleaner {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileCleaner.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static final LoadTsFileCleaner INSTANCE = new LoadTsFileCleaner();
  private static final String SWEEP_JOB_ID = "LoadTsFileCleaner#sweep";

  private final Map<String, RetainedTask> loadId2RetainedTask = new ConcurrentHashMap<>();
  private final AtomicBoolean sweepJobRegistered = new AtomicBoolean(false);

  private volatile boolean sweeping;

  private LoadTsFileCleaner() {}

  public static LoadTsFileCleaner getInstance() {
    return INSTANCE;
  }

  // -------------------------------------------------------------------------
  // Service Lifecycle & Task Registration
  // -------------------------------------------------------------------------

  /** Starts the background cleanup job by registering with the runtime executor. */
  public void start() {
    sweeping = true;
    if (!sweepJobRegistered.compareAndSet(false, true)) {
      return;
    }
    PipeDataNodeAgent.runtime()
        .registerPeriodicalJob(
            SWEEP_JOB_ID,
            this::sweepSafely,
            Math.max(1L, CONFIG.getLoadCleanupTaskExecutionDelayTimeSeconds()));
  }

  /** Stops the background cleaning loop and clears tracked tasks. */
  public void stop() {
    sweeping = false;
    loadId2RetainedTask.clear();
  }

  /** Registers a finished load task for delayed physical directory deletion. */
  public void register(
      final String loadId,
      final DataRegion dataRegion,
      final File taskDir,
      final long searchIndex,
      final LoadTsFileConsensusOp terminalOp) {
    Objects.requireNonNull(loadId, StorageEngineMessages.EXCEPTION_LOADID_CANNOT_BE_NULL_22AFCDC7);
    Objects.requireNonNull(
        dataRegion, StorageEngineMessages.EXCEPTION_DATAREGION_CANNOT_BE_NULL_0B936879);
    Objects.requireNonNull(
        taskDir, StorageEngineMessages.EXCEPTION_TASKDIR_CANNOT_BE_NULL_11671FD9);
    loadId2RetainedTask.put(
        loadId, new RetainedTask(loadId, dataRegion, taskDir, searchIndex, terminalOp));
  }

  // -------------------------------------------------------------------------
  // Periodic & Manual Sweeping
  // -------------------------------------------------------------------------

  private void sweepSafely() {
    if (!sweeping) {
      return;
    }
    try {
      sweep();
    } catch (final Throwable t) {
      LOGGER.warn(StorageEngineMessages.LOG_LOAD_CONSENSUS_CLEANER_SWEEP_FAILED_7C3E2E6D, t);
    }
  }

  /** Sweeps both in-memory registered tasks and unmanaged disk staging directories. */
  public void sweep() {
    // 1. Clean registered memory-tracked tasks
    for (final Map.Entry<String, RetainedTask> entry : loadId2RetainedTask.entrySet()) {
      final RetainedTask retainedTask = entry.getValue();
      if (canDelete(retainedTask) && loadId2RetainedTask.remove(entry.getKey(), retainedTask)) {
        delete(retainedTask);
      }
    }

    // 2. Discover and sweep unmanaged directories left behind by unexpected crashes
    scanRegions();
  }

  private void scanRegions() {
    final List<DataRegion> regions = StorageEngine.getInstance().getAllDataRegions();
    if (regions == null || regions.isEmpty()) {
      return;
    }

    for (final DataRegion dataRegion : regions) {
      for (final String baseDir : LoadStagingDirs.configuredBaseDirs()) {
        final File regionDir =
            LoadStagingDirs.regionLoadDir(
                new File(baseDir),
                dataRegion.getDatabaseName(),
                dataRegion.getDataRegionIdString());

        final File[] taskDirs = regionDir.listFiles();
        if (taskDirs == null) {
          continue;
        }

        for (final File taskDir : taskDirs) {
          if (!taskDir.isDirectory() || loadId2RetainedTask.containsKey(taskDir.getName())) {
            continue;
          }

          final LoadTsFileProgress.TerminalRecord terminal = readTerminal(taskDir);
          if (terminal == null) {
            continue;
          }

          final RetainedTask unmanagedTask =
              new RetainedTask(
                  taskDir.getName(), dataRegion, taskDir, terminal.searchIndex(), terminal.op());

          if (canDelete(unmanagedTask)) {
            delete(unmanagedTask);
          }
        }
      }
    }
  }

  // -------------------------------------------------------------------------
  // Deletion Qualification & Safety Checks
  // -------------------------------------------------------------------------

  /** Evaluates if a staged directory can be safely reclaimed. */
  private static boolean canDelete(final RetainedTask retainedTask) {
    if (!replicasReached(retainedTask.dataRegion(), retainedTask.searchIndex())) {
      return false;
    }
    // Aborted tasks are pure garbage once synchronized; committed tasks must also be complete
    return retainedTask.terminalOp() == LoadTsFileConsensusOp.ABORT
        || isComplete(retainedTask.taskDir());
  }

  /** Verifies that every staged TsFile in the directory has no missing chunks. */
  private static boolean isComplete(final File taskDir) {
    final File[] files = taskDir.listFiles();
    if (files == null) {
      return false;
    }

    for (final File file : files) {
      final String name = file.getName();
      final int progressSuffixAt = name.lastIndexOf(LoadTsFileProgress.PROGRESS_SUFFIX);
      if (progressSuffixAt < 0) {
        continue;
      }

      final File tsFile = new File(file.getParentFile(), name.substring(0, progressSuffixAt));
      try {
        if (!new LoadTsFileProgress(tsFile).isReady(tsFile.length())) {
          return false;
        }
      } catch (final IOException e) {
        LOGGER.warn(StorageEngineMessages.LOG_FAILED_TO_DELETE_ARG_3A7BD6FD, tsFile.getPath(), e);
        return false;
      }
    }
    return true;
  }

  private static boolean replicasReached(final DataRegion dataRegion, final long searchIndex) {
    final long safelyDeletedSearchIndex =
        dataRegion
            .getWALNode()
            .map(wal -> wal.getSafelyDeletedSearchIndex())
            .orElse(ConsensusReqReader.DEFAULT_SAFELY_DELETED_SEARCH_INDEX);

    return safelyDeletedSearchIndex == ConsensusReqReader.DEFAULT_SAFELY_DELETED_SEARCH_INDEX
        || safelyDeletedSearchIndex >= searchIndex;
  }

  private static LoadTsFileProgress.TerminalRecord readTerminal(final File taskDir) {
    final File[] files = taskDir.listFiles();
    if (files == null) {
      return null;
    }

    for (final File file : files) {
      if (!file.getName().endsWith(LoadTsFileProgress.PROGRESS_SUFFIX)) {
        continue;
      }
      try {
        return LoadTsFileProgress.readTerminal(file);
      } catch (final IOException e) {
        LOGGER.warn(StorageEngineMessages.LOG_FAILED_TO_DELETE_ARG_3A7BD6FD, file.getPath(), e);
        return null;
      }
    }
    return null;
  }

  // -------------------------------------------------------------------------
  // Physical Deletion Operations
  // -------------------------------------------------------------------------

  private static void delete(final RetainedTask retainedTask) {
    LOGGER.info(
        StorageEngineMessages
            .LOG_RELEASED_THE_STAGED_DIRECTORY_ARG_OF_LOAD_TASK_ARG_BECAUSE_THE_SAFE_DELETION_SEARCH_INDEX_ARG_IS_REACHED_0CF83F8A,
        retainedTask.taskDir().getAbsolutePath(),
        retainedTask.loadId(),
        retainedTask.searchIndex());
    cleanTaskDir(retainedTask.taskDir());
  }

  /** Recursively deletes an entire staged directory and all its files using NIO walkFileTree. */
  public static void cleanTaskDir(final File taskDir) {
    if (taskDir == null || !taskDir.exists()) {
      return;
    }
    try {
      Files.walkFileTree(
          taskDir.toPath(),
          new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs)
                throws IOException {
              Files.deleteIfExists(file);
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(final Path dir, final IOException exc)
                throws IOException {
              if (exc != null) {
                throw exc;
              }
              Files.deleteIfExists(dir);
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (final IOException e) {
      LOGGER.warn(StorageEngineMessages.LOG_FAILED_TO_DELETE_ARG_3A7BD6FD, taskDir.getPath(), e);
    }
    LoadStagingDirs.deleteTaskDir(taskDir);
  }

  /** Deletes the artifacts belonging to a single staged TsFile. */
  public static void cleanTsFile(final File tsFile) {
    if (tsFile == null) {
      return;
    }
    try {
      Files.deleteIfExists(tsFile.toPath());
      Files.deleteIfExists(
          new File(tsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).toPath());
      Files.deleteIfExists(ModificationFile.getExclusiveMods(tsFile).toPath());
      Files.deleteIfExists(
          new File(tsFile.getAbsolutePath() + ModificationFileV1.FILE_SUFFIX).toPath());
      Files.deleteIfExists(LoadTsFileProgress.progressFileFor(tsFile).toPath());
    } catch (final IOException e) {
      LOGGER.warn(
          StorageEngineMessages.LOG_FAILED_TO_DELETE_ARG_3A7BD6FD, tsFile.getAbsolutePath(), e);
    }
  }

  // -------------------------------------------------------------------------
  // Model Record
  // -------------------------------------------------------------------------

  private record RetainedTask(
      String loadId,
      DataRegion dataRegion,
      File taskDir,
      long searchIndex,
      LoadTsFileConsensusOp terminalOp) {}
}
