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
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The one cleanup service of a DataNode for the staged directories of LOAD tasks.
 *
 * <p>A task writes its staged TsFiles into its own directory and records, in the tail of every
 * progress file, the COMMIT or ABORT command it finished with together with the consensus index of
 * that command. From then on the directory is garbage as soon as no replica can read its bytes back
 * any more, that is, once every replica has applied that index. This service scans the directories
 * of every DataRegion, reads those tails, and deletes the directory once the region reports that
 * the index has been reached.
 */
public class LoadTsFileCleaner {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileCleaner.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static final LoadTsFileCleaner INSTANCE = new LoadTsFileCleaner();

  /** Tasks whose directory is known to be finished, so that a scan is not always needed. */
  private final Map<String, RetainedTask> loadId2RetainedTask = new ConcurrentHashMap<>();

  private volatile Thread sweepThread;

  private LoadTsFileCleaner() {}

  public static LoadTsFileCleaner getInstance() {
    return INSTANCE;
  }

  /**
   * Starts the DataNode level sweeping thread. The thread is a daemon: a node that goes down
   * mid-sweep leaves nothing but directories that the next scan finds again.
   */
  public synchronized void start() {
    if (sweepThread != null) {
      return;
    }
    sweepThread = new Thread(this::sweepLoop, "load-tsfile-cleaner");
    sweepThread.setDaemon(true);
    sweepThread.start();
  }

  public synchronized void stop() {
    final Thread thread = sweepThread;
    sweepThread = null;
    if (thread != null) {
      thread.interrupt();
    }
    loadId2RetainedTask.clear();
  }

  /**
   * Remembers a directory that belongs to a task which already reached COMMIT or ABORT, so that the
   * next scan can delete it as soon as its consensus index has been applied everywhere.
   */
  public void register(
      final String loadId,
      final DataRegion dataRegion,
      final File taskDir,
      final long searchIndex,
      final LoadTsFileConsensusOp terminalOp) {
    loadId2RetainedTask.put(
        loadId, new RetainedTask(loadId, dataRegion, taskDir, searchIndex, terminalOp));
  }

  private void sweepLoop() {
    final long intervalInMs =
        Math.max(1000L, CONFIG.getLoadCleanupTaskExecutionDelayTimeSeconds() * 1000L);
    while (!Thread.currentThread().isInterrupted()) {
      try {
        Thread.sleep(intervalInMs);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      try {
        sweep();
      } catch (final Throwable t) {
        LOGGER.warn(StorageEngineMessages.LOG_LOAD_CONSENSUS_CLEANER_SWEEP_FAILED_7C3E2E6D, t);
      }
    }
  }

  /** Scans the staged directories of every DataRegion and deletes the ones that are finished. */
  public void sweep() {
    for (final Map.Entry<String, RetainedTask> entry : loadId2RetainedTask.entrySet()) {
      final RetainedTask retainedTask = entry.getValue();
      if (canDelete(retainedTask) && loadId2RetainedTask.remove(entry.getKey(), retainedTask)) {
        delete(retainedTask);
      }
    }
    scanRegions();
  }

  /**
   * Looks for finished directories that the registry does not know about, which is what a restart
   * leaves behind when the sweep did not get to run.
   */
  private void scanRegions() {
    for (final DataRegion dataRegion : StorageEngine.getInstance().getAllDataRegions()) {
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
          if (!taskDir.isDirectory()) {
            continue;
          }
          final LoadTsFileProgress.TerminalRecord terminal = readTerminal(taskDir);
          if (terminal == null) {
            continue;
          }
          final RetainedTask found =
              new RetainedTask(
                  taskDir.getName(), dataRegion, taskDir, terminal.searchIndex(), terminal.op());
          if (canDelete(found)) {
            delete(found);
          }
        }
      }
    }
  }

  /** Reads the tail of the finished task that owns a staged directory, or null when it has none. */
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
        LOGGER.warn(LoadStagingDirs.MESSAGE_DELETE_FAIL, file.getPath(), e);
        return null;
      }
    }
    return null;
  }

  /**
   * The two ways a staged directory becomes garbage.
   *
   * <p>An aborted task is never imported, so its bytes are plain garbage as soon as the ABORT
   * command was applied - which is also the moment the command is on its way to the followers and
   * is recorded in the tail of the progress files. A committed task, on the other hand, is imported
   * from a copy of those bytes and a replica that catches up by reading this node's WAL expands its
   * entries from them, so it is deleted only once the staged files are complete and the
   * safe-deletion watermark reports that every replica got past the COMMIT.
   */
  private static boolean canDelete(final RetainedTask retainedTask) {
    if (!replicasReached(retainedTask.dataRegion, retainedTask.searchIndex)) {
      // The command is not on every replica yet: a replica that catches up by reading this node's
      // WAL still reads these bytes back to build the request it receives, whether the task was
      // committed or aborted. An ABORT therefore waits for its own synchronization too.
      return false;
    }
    // Once every replica got past the command an aborted task is plain garbage, because an aborted
    // task is never imported. A committed one is imported from a copy of those bytes, so it is kept
    // until the staged files are known to be complete as well.
    return retainedTask.terminalOp == LoadTsFileConsensusOp.ABORT
        || isComplete(retainedTask.taskDir);
  }

  /**
   * Whether every replica already applied the command, so that nobody reads these bytes any more.
   */
  /**
   * Whether every staged TsFile of a directory arrived completely: its progress records cover the
   * file without a hole, up to its end. A hole means a piece never arrived, and deleting the
   * directory then would lose the bytes a replica still has to read back.
   */
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
        LOGGER.warn(LoadStagingDirs.MESSAGE_DELETE_FAIL, tsFile.getPath(), e);
        return false;
      }
    }
    // A finished task drops its progress files - they exist only while a file is still being
    // written, and a file that is still being written keeps them until the task ends - so a
    // directory without them has nothing left to verify. A directory that still holds them has to
    // cover its staged files without a hole: a hole means a piece never arrived, and deleting the
    // directory then would lose the bytes a replica still has to read back.
    return true;
  }

  private static boolean replicasReached(final DataRegion dataRegion, final long searchIndex) {
    final long safelyDeletedSearchIndex =
        dataRegion
            .getWALNode()
            .map(wal -> wal.getSafelyDeletedSearchIndex())
            .orElse(ConsensusReqReader.DEFAULT_SAFELY_DELETED_SEARCH_INDEX);
    // Consensus V2, and every protocol without a WAL watermark, reports the default index: there is
    // no follower to wait for, so a COMMIT or an ABORT may be deleted as soon as it was executed.
    return safelyDeletedSearchIndex == ConsensusReqReader.DEFAULT_SAFELY_DELETED_SEARCH_INDEX
        || safelyDeletedSearchIndex >= searchIndex;
  }

  private static void delete(final RetainedTask retainedTask) {
    LOGGER.info(
        StorageEngineMessages
            .LOG_RELEASED_THE_STAGED_DIRECTORY_ARG_OF_LOAD_TASK_ARG_BECAUSE_THE_SAFE_DELETION_SEARCH_INDEX_ARG_IS_REACHED_0CF83F8A,
        retainedTask.taskDir.getAbsolutePath(),
        retainedTask.loadId,
        retainedTask.searchIndex);
    cleanTaskDir(retainedTask.taskDir);
  }

  /**
   * Deletes a staged directory and every file that belongs to the TsFile it holds: the file itself,
   * its resource, both flavours of its modification file and its progress files.
   */
  public static void cleanTaskDir(final File taskDir) {
    final File[] files = taskDir.listFiles();
    if (files != null) {
      for (final File file : files) {
        if (file.isDirectory()) {
          cleanTaskDir(file);
          continue;
        }
        try {
          Files.deleteIfExists(file.toPath());
        } catch (final IOException e) {
          LOGGER.warn(LoadStagingDirs.MESSAGE_DELETE_FAIL, file.getPath(), e);
        }
      }
    }
    LoadStagingDirs.deleteTaskDir(taskDir);
  }

  /** Deletes the staged files that belong to one TsFile, the way unloading a TsFile does. */
  public static void cleanTsFile(final File tsFile) {
    try {
      Files.deleteIfExists(tsFile.toPath());
      Files.deleteIfExists(
          new File(tsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).toPath());
      Files.deleteIfExists(ModificationFile.getExclusiveMods(tsFile).toPath());
      Files.deleteIfExists(
          new File(tsFile.getAbsolutePath() + ModificationFileV1.FILE_SUFFIX).toPath());
      Files.deleteIfExists(LoadTsFileProgress.progressFileFor(tsFile).toPath());
    } catch (final IOException e) {
      LOGGER.warn(LoadStagingDirs.MESSAGE_DELETE_FAIL, tsFile.getAbsolutePath(), e);
    }
  }

  private static class RetainedTask {
    private final String loadId;
    private final DataRegion dataRegion;
    private final File taskDir;
    private final long searchIndex;
    private final LoadTsFileConsensusOp terminalOp;

    private RetainedTask(
        final String loadId,
        final DataRegion dataRegion,
        final File taskDir,
        final long searchIndex,
        final LoadTsFileConsensusOp terminalOp) {
      this.loadId = loadId;
      this.dataRegion = dataRegion;
      this.taskDir = taskDir;
      this.searchIndex = searchIndex;
      this.terminalOp = terminalOp;
    }
  }
}
