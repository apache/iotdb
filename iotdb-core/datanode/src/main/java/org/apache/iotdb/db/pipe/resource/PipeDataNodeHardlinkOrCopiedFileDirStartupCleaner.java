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

import org.apache.tsfile.external.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
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
    final List<File> stalePipeDirs = new ArrayList<>();
    for (final String dataDir : IoTDBDescriptor.getInstance().getConfig().getDataDirs()) {
      final File localDataDir = new File(dataDir);
      collectInterruptedStalePipeDirs(localDataDir, pipeHardlinkBaseDirName, stalePipeDirs);

      final File pipeHardLinkDir = new File(localDataDir, pipeHardlinkBaseDirName);
      if (pipeHardLinkDir.isDirectory()) {
        moveAsideAndCollect(pipeHardLinkDir, pipeHardlinkBaseDirName, stalePipeDirs);
      }
    }
    registerPeriodicalCleanupJob(periodicalJobRegistrar, stalePipeDirs);
  }

  private static void collectInterruptedStalePipeDirs(
      final File localDataDir,
      final String pipeHardlinkBaseDirName,
      final List<File> stalePipeDirs) {
    final File[] stalePipeDirFiles =
        localDataDir.listFiles(
            file ->
                file.isDirectory()
                    && file.getName().startsWith(pipeHardlinkBaseDirName + STALE_PIPE_DIR_SUFFIX));
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

  private static void moveAsideAndCollect(
      final File pipeHardLinkDir,
      final String pipeHardlinkBaseDirName,
      final List<File> stalePipeDirs) {
    try {
      final File stalePipeDir = moveAside(pipeHardLinkDir, pipeHardlinkBaseDirName);
      LOGGER.info(
          DataNodePipeMessages.PIPE_HARDLINK_DIR_FOUND_MOVED_TO_PERIODICAL_DELETE,
          pipeHardLinkDir,
          stalePipeDir);
      stalePipeDirs.add(stalePipeDir);
    } catch (final IOException e) {
      LOGGER.warn(
          DataNodePipeMessages.PIPE_HARDLINK_DIR_MOVE_FAILED_DELETING_SYNC, pipeHardLinkDir, e);
      LOGGER.info(
          DataNodePipeMessages.PIPE_HARDLINK_DIR_FOUND_DELETING_IT_RESULT,
          pipeHardLinkDir,
          FileUtils.deleteQuietly(pipeHardLinkDir));
    }
  }

  private static File moveAside(final File pipeHardLinkDir, final String pipeHardlinkBaseDirName)
      throws IOException {
    final File parentDir = pipeHardLinkDir.getParentFile();
    if (parentDir == null) {
      throw new IOException("Failed to get parent dir of " + pipeHardLinkDir);
    }

    final long timestamp = System.currentTimeMillis();
    for (int i = 0; ; ++i) {
      final File stalePipeDir =
          new File(
              parentDir, pipeHardlinkBaseDirName + STALE_PIPE_DIR_SUFFIX + timestamp + "-" + i);
      if (!stalePipeDir.exists()) {
        Files.move(pipeHardLinkDir.toPath(), stalePipeDir.toPath());
        return stalePipeDir;
      }
    }
  }

  private static void registerPeriodicalCleanupJob(
      final PeriodicalJobRegistrar periodicalJobRegistrar, final List<File> stalePipeDirs) {
    if (stalePipeDirs.isEmpty()) {
      return;
    }

    periodicalJobRegistrar.register(
        PERIODICAL_CLEANUP_JOB_ID,
        new PeriodicalStalePipeDirCleaner(stalePipeDirs)::cleanOneRound,
        PipeConfig.getInstance().getPipeSubtaskExecutorCronHeartbeatEventIntervalSeconds());
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

    private final List<File> stalePipeDirs;
    private int currentDirIndex;
    private boolean finished;

    private PeriodicalStalePipeDirCleaner(final List<File> stalePipeDirs) {
      this.stalePipeDirs = stalePipeDirs;
      currentDirIndex = 0;
      finished = false;
    }

    private void cleanOneRound() {
      if (finished) {
        return;
      }

      long deletedPathCount = 0;
      while (currentDirIndex < stalePipeDirs.size()) {
        final File stalePipeDir = stalePipeDirs.get(currentDirIndex);
        final CleanupRoundResult result = deleteQuietlyWithThrottle(stalePipeDir);
        deletedPathCount += result.deletedPathCount;

        if (result.finished) {
          LOGGER.info(
              DataNodePipeMessages.PIPE_HARDLINK_DIR_PERIODICAL_DELETE_FINISHED,
              stalePipeDir,
              result.success);
          ++currentDirIndex;
          continue;
        }

        if (deletedPathCount > 0 || !result.success) {
          LOGGER.info(
              DataNodePipeMessages.PIPE_HARDLINK_DIR_PERIODICAL_DELETE_PROGRESS,
              deletedPathCount,
              stalePipeDir,
              result.success);
        }
        return;
      }

      finished = true;
      LOGGER.info(DataNodePipeMessages.PIPE_HARDLINK_DIR_PERIODICAL_DELETE_ALL_FINISHED);
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
