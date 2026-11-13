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

package org.apache.iotdb.db.pipe.resource;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.resource.snapshot.PipeSnapshotResourceManager;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class PipeDataNodeHardlinkOrCopiedFileDirStartupCleaner {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeDataNodeHardlinkOrCopiedFileDirStartupCleaner.class);
  private static final String STALE_PIPE_DIR_SUFFIX = ".startup-cleaning-";
  private static final String PERIODICAL_CLEANUP_JOB_ID =
      "PipeDataNodeHardlinkOrCopiedFileDirStartupCleaner#cleanTsFileDir()";
  private static final long DELETE_MAX_PATH_COUNT_PER_ROUND = 100_000L;
  private static final long DELETE_MAX_TIME_PER_ROUND_MS = 1_000L;
  private static final PeriodicalStalePipeDirCleaner PERIODICAL_STALE_PIPE_DIR_CLEANER =
      new PeriodicalStalePipeDirCleaner();
  private static final AtomicBoolean PERIODICAL_CLEANUP_JOB_REGISTERED = new AtomicBoolean(false);

  /**
   * Delete the data directory and all of its subdirectories that contain the
   * PipeConfig.PIPE_TSFILE_DIR_NAME directory.
   */
  public static void clean(final PeriodicalJobRegistrar periodicalJobRegistrar) {
    cleanTsFileDir(periodicalJobRegistrar);
    cleanSnapshotDir();
  }

  private static void cleanTsFileDir(final PeriodicalJobRegistrar periodicalJobRegistrar) {
    final String pipeHardlinkBaseDirName = PipeConfig.getInstance().getPipeHardlinkBaseDirName();
    final String pipeHardlinkTsFileDirName =
        PipeConfig.getInstance().getPipeHardlinkTsFileDirName();
    final List<File> stalePipeDirs = new ArrayList<>();
    for (final File pipeBaseDir : getPipeBaseDirs(pipeHardlinkBaseDirName)) {
      // Stale pipe dirs left by an older version, e.g., <dataDir>/pipe.startup-cleaning-...
      collectInterruptedStalePipeDirs(
          pipeBaseDir.getParentFile(), pipeHardlinkBaseDirName, stalePipeDirs);
      // Stale tsfile dirs under the pipe base dir, e.g., <pipeDir>/tsfile.startup-cleaning-...
      collectInterruptedStalePipeDirs(pipeBaseDir, pipeHardlinkTsFileDirName, stalePipeDirs);

      final File pipeTsFileDir = new File(pipeBaseDir, pipeHardlinkTsFileDirName);
      if (pipeTsFileDir.isDirectory()) {
        moveAsideAndCollect(pipeTsFileDir, stalePipeDirs);
      }
    }
    PERIODICAL_STALE_PIPE_DIR_CLEANER.addStalePipeDirs(stalePipeDirs);
    registerPeriodicalCleanupJob(periodicalJobRegistrar);
  }

  /** Returns the pipe base directory of every local data dir, i.e. one per disk. */
  private static Set<File> getPipeBaseDirs(final String pipeHardlinkBaseDirName) {
    final Set<File> pipeBaseDirs = new LinkedHashSet<>();
    for (final String dataDir : IoTDBDescriptor.getInstance().getConfig().getLocalDataDirs()) {
      pipeBaseDirs.add(new File(new File(dataDir), pipeHardlinkBaseDirName));
    }
    return pipeBaseDirs;
  }

  private static void collectInterruptedStalePipeDirs(
      final @Nullable File dir, final String movedDirName, final List<File> stalePipeDirs) {
    if (Objects.isNull(dir)) {
      return;
    }
    // Only the directories this cleaner has moved aside are matched, not any similar name.
    final String stalePipeDirNamePrefix = movedDirName + STALE_PIPE_DIR_SUFFIX;
    final File[] stalePipeDirFiles =
        dir.listFiles(
            file -> file.isDirectory() && file.getName().startsWith(stalePipeDirNamePrefix));
    if (stalePipeDirFiles == null) {
      return;
    }

    for (final File stalePipeDir : stalePipeDirFiles) {
      LOGGER.info(
          DataNodePipeMessages.PIPE_STALE_HARDLINK_DIR_FOUND_REGISTERING_PERIODICAL_DELETE,
          stalePipeDir);
      stalePipeDirs.add(stalePipeDir);
    }
  }

  private static void moveAsideAndCollect(final File pipeDir, final List<File> stalePipeDirs) {
    try {
      final File stalePipeDir = moveAside(pipeDir);
      LOGGER.info(
          DataNodePipeMessages.PIPE_HARDLINK_DIR_FOUND_MOVED_TO_PERIODICAL_DELETE,
          pipeDir,
          stalePipeDir);
      stalePipeDirs.add(stalePipeDir);
    } catch (final IOException e) {
      LOGGER.warn(DataNodePipeMessages.PIPE_HARDLINK_DIR_MOVE_FAILED_DELETING_SYNC, pipeDir, e);
      org.apache.iotdb.commons.utils.FileUtils.deleteFileOrDirectory(pipeDir, true);
      LOGGER.info(
          DataNodePipeMessages.PIPE_HARDLINK_DIR_FOUND_DELETING_IT_RESULT,
          pipeDir,
          !pipeDir.exists());
    }
  }

  private static File moveAside(final File pipeDir) throws IOException {
    final File parentDir = pipeDir.getParentFile();
    if (Objects.isNull(parentDir)) {
      throw new IOException(
          DataNodePipeMessages.EXCEPTION_FAILED_TO_GET_PARENT_DIR_OF_8CE21C1D + pipeDir);
    }

    final long timestamp = System.currentTimeMillis();
    for (int i = 0; ; ++i) {
      final File stalePipeDir =
          new File(parentDir, pipeDir.getName() + STALE_PIPE_DIR_SUFFIX + timestamp + "-" + i);
      if (!stalePipeDir.exists()) {
        Files.move(pipeDir.toPath(), stalePipeDir.toPath());
        return stalePipeDir;
      }
    }
  }

  private static void registerPeriodicalCleanupJob(
      final PeriodicalJobRegistrar periodicalJobRegistrar) {
    if (!PERIODICAL_CLEANUP_JOB_REGISTERED.compareAndSet(false, true)) {
      return;
    }

    periodicalJobRegistrar.register(
        PERIODICAL_CLEANUP_JOB_ID,
        PERIODICAL_STALE_PIPE_DIR_CLEANER::cleanOneRound,
        PipeConfig.getInstance().getPipeSubtaskExecutorCronHeartbeatEventIntervalSeconds());
  }

  public static void submitStalePipeDirForPeriodicalCleanup(final File stalePipeDir) {
    if (stalePipeDir == null) {
      return;
    }

    final File stalePipeDirToDelete;
    if (stalePipeDir.isDirectory()) {
      try {
        stalePipeDirToDelete = moveAside(stalePipeDir);
        LOGGER.info(
            DataNodePipeMessages.PIPE_HARDLINK_DIR_FOUND_MOVED_TO_PERIODICAL_DELETE,
            stalePipeDir,
            stalePipeDirToDelete);
      } catch (final IOException e) {
        LOGGER.warn(
            DataNodePipeMessages.PIPE_HARDLINK_DIR_MOVE_FAILED_SKIPPING_PERIODICAL_DELETE,
            stalePipeDir,
            e);
        return;
      }
    } else {
      stalePipeDirToDelete = stalePipeDir;
    }

    LOGGER.info(
        DataNodePipeMessages.PIPE_STALE_HARDLINK_DIR_FOUND_REGISTERING_PERIODICAL_DELETE,
        stalePipeDirToDelete);
    PERIODICAL_STALE_PIPE_DIR_CLEANER.addStalePipeDir(stalePipeDirToDelete);
  }

  private static CleanupRoundResult deleteQuietlyWithThrottle(final File stalePipeDir) {
    if (!stalePipeDir.exists()) {
      return CleanupRoundResult.finished();
    }

    final AtomicBoolean deleteResult = new AtomicBoolean(true);
    final AtomicLong deletedPathCount = new AtomicLong(0);
    final long deadlineNanos = System.nanoTime() + DELETE_MAX_TIME_PER_ROUND_MS * 1_000_000L;
    try {
      Files.walkFileTree(
          stalePipeDir.toPath(),
          new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(
                final Path dir, final BasicFileAttributes attrs) {
              return shouldStop(deletedPathCount, deadlineNanos)
                  ? FileVisitResult.TERMINATE
                  : FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) {
              return deletePath(file, deleteResult, deletedPathCount, deadlineNanos);
            }

            @Override
            public FileVisitResult visitFileFailed(final Path file, final IOException exc) {
              deleteResult.set(false);
              return deletePath(file, deleteResult, deletedPathCount, deadlineNanos);
            }

            @Override
            public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) {
              if (exc != null) {
                deleteResult.set(false);
              }
              return deletePath(dir, deleteResult, deletedPathCount, deadlineNanos);
            }
          });
    } catch (final IOException e) {
      deleteResult.set(false);
    }

    return new CleanupRoundResult(
        deletedPathCount.get(), deleteResult.get() && !stalePipeDir.exists(), deleteResult.get());
  }

  private static FileVisitResult deletePath(
      final Path path,
      final AtomicBoolean deleteResult,
      final AtomicLong deletedPathCount,
      final long deadlineNanos) {
    if (shouldStop(deletedPathCount, deadlineNanos)) {
      return FileVisitResult.TERMINATE;
    }

    try {
      if (Files.deleteIfExists(path)) {
        deletedPathCount.incrementAndGet();
      }
    } catch (final IOException e) {
      deleteResult.set(false);
    }
    return shouldStop(deletedPathCount, deadlineNanos)
        ? FileVisitResult.TERMINATE
        : FileVisitResult.CONTINUE;
  }

  private static boolean shouldStop(final AtomicLong deletedPathCount, final long deadlineNanos) {
    return Thread.currentThread().isInterrupted()
        || deletedPathCount.get() >= DELETE_MAX_PATH_COUNT_PER_ROUND
        || System.nanoTime() >= deadlineNanos;
  }

  @FunctionalInterface
  public interface PeriodicalJobRegistrar {

    void register(String id, Runnable periodicalJob, long intervalInSeconds);
  }

  private static class PeriodicalStalePipeDirCleaner {

    private final ConcurrentLinkedQueue<File> stalePipeDirs = new ConcurrentLinkedQueue<>();
    private File currentStalePipeDir;

    private void addStalePipeDir(final File stalePipeDir) {
      stalePipeDirs.offer(stalePipeDir);
    }

    private void addStalePipeDirs(final List<File> stalePipeDirs) {
      stalePipeDirs.forEach(this::addStalePipeDir);
    }

    private void cleanOneRound() {
      long deletedPathCount = 0;
      while (true) {
        if (currentStalePipeDir == null) {
          currentStalePipeDir = stalePipeDirs.poll();
        }
        if (currentStalePipeDir == null) {
          return;
        }

        final CleanupRoundResult result = deleteQuietlyWithThrottle(currentStalePipeDir);
        deletedPathCount += result.deletedPathCount;

        if (result.finished) {
          LOGGER.info(
              DataNodePipeMessages.PIPE_HARDLINK_DIR_PERIODICAL_DELETE_FINISHED,
              currentStalePipeDir,
              result.success);
          currentStalePipeDir = null;
          continue;
        }

        if (deletedPathCount > 0 || !result.success) {
          LOGGER.info(
              DataNodePipeMessages.PIPE_HARDLINK_DIR_PERIODICAL_DELETE_PROGRESS,
              deletedPathCount,
              currentStalePipeDir,
              result.success);
        }
        return;
      }
    }
  }

  private static class CleanupRoundResult {

    private final long deletedPathCount;
    private final boolean finished;
    private final boolean success;

    private CleanupRoundResult(
        final long deletedPathCount, final boolean finished, final boolean success) {
      this.deletedPathCount = deletedPathCount;
      this.finished = finished;
      this.success = success;
    }

    private static CleanupRoundResult finished() {
      return new CleanupRoundResult(0, true, true);
    }
  }

  private static void cleanSnapshotDir() {
    final File iotConsensusV2Dir =
        new File(
            IoTDBDescriptor.getInstance().getConfig().getConsensusDir()
                + File.separator
                + PipeSnapshotResourceManager.PIPE_SNAPSHOT_DIR_NAME);
    if (iotConsensusV2Dir.isDirectory()) {
      LOGGER.info(DataNodePipeMessages.PIPE_SNAPSHOT_DIR_FOUND_DELETING_IT, iotConsensusV2Dir);
      org.apache.iotdb.commons.utils.FileUtils.deleteFileOrDirectory(iotConsensusV2Dir);
    }
  }

  private PipeDataNodeHardlinkOrCopiedFileDirStartupCleaner() {
    // util class
  }
}
