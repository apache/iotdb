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
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusOp;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.IWALNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Retains staged directories of finished LOAD tasks until WAL watermarks confirm that follower
 * replicas have synchronized the required chunks, preventing premature deletion.
 */
final class LoadTaskRetention {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTaskRetention.class);

  private static final String TERMINAL_MARKER_FILE_NAME = "terminal.marker";

  private final DataRegion dataRegion;
  private final Map<String, RetainedTaskDir> loadId2RetainedTaskDir = new ConcurrentHashMap<>();
  private final AtomicBoolean listenerRegistered = new AtomicBoolean(false);

  LoadTaskRetention(final DataRegion dataRegion) {
    this.dataRegion =
        Objects.requireNonNull(
            dataRegion, StorageEngineMessages.EXCEPTION_DATAREGION_CANNOT_BE_NULL_0B936879);
  }

  // -------------------------------------------------------------------------
  // Retention Verification & Lifecycle
  // -------------------------------------------------------------------------

  /** Checks whether the staged directory must be retained for follower WAL synchronization. */
  boolean mustRetain(final long searchIndex) {
    final long safelyDeletedSearchIndex = currentSafelyDeletedSearchIndex();
    return safelyDeletedSearchIndex != ConsensusReqReader.DEFAULT_SAFELY_DELETED_SEARCH_INDEX
        && safelyDeletedSearchIndex < searchIndex;
  }

  /** Marks a directory as finished to ensure crash recovery will delete rather than resume it. */
  void markTerminal(
      final File taskDir, final LoadTsFileConsensusOp terminalOp, final long searchIndex) {
    if (taskDir == null || !taskDir.isDirectory()) {
      return;
    }

    final File[] files = taskDir.listFiles();
    if (files != null) {
      for (final File file : files) {
        final String name = file.getName();
        final int progressSuffixAt = name.lastIndexOf(LoadTsFileProgress.PROGRESS_SUFFIX);
        if (progressSuffixAt < 0) {
          continue;
        }
        try {
          final File stagedFile =
              new File(file.getParentFile(), name.substring(0, progressSuffixAt));
          new LoadTsFileProgress(stagedFile).recordTerminal(terminalOp, searchIndex);
        } catch (final IOException e) {
          LOGGER.warn(
              StorageEngineMessages.LOG_LOAD_CONSENSUS_TERMINAL_MARKER_WRITE_FAILED_4D6D7433,
              taskDir.getName(),
              taskDir.getAbsolutePath(),
              e);
        }
      }
    }

    try {
      final File marker = new File(taskDir, TERMINAL_MARKER_FILE_NAME);
      Files.write(marker.toPath(), Long.toString(searchIndex).getBytes(StandardCharsets.UTF_8));
    } catch (final IOException e) {
      LOGGER.warn(
          StorageEngineMessages.LOG_LOAD_CONSENSUS_TERMINAL_MARKER_WRITE_FAILED_4D6D7433,
          taskDir.getName(),
          taskDir.getAbsolutePath(),
          e);
    }
  }

  /** Takes ownership of a staged directory, postponing deletion until WAL watermarks allow it. */
  void retain(
      final String loadId,
      final File taskDir,
      final LoadTsFileConsensusOp terminalOp,
      final long searchIndex) {
    Objects.requireNonNull(loadId, StorageEngineMessages.EXCEPTION_LOADID_CANNOT_BE_NULL_22AFCDC7);
    Objects.requireNonNull(
        taskDir, StorageEngineMessages.EXCEPTION_TASKDIR_CANNOT_BE_NULL_11671FD9);

    loadId2RetainedTaskDir.put(loadId, new RetainedTaskDir(taskDir, searchIndex, terminalOp));
    LOGGER.info(
        StorageEngineMessages
            .LOG_KEEPING_THE_STAGED_DIRECTORY_ARG_OF_LOAD_TASK_ARG_UNTIL_THE_SAFE_DELETION_SEARCH_INDEX_ARG_IS_REACHED_AFE2BC88,
        taskDir.getAbsolutePath(),
        loadId,
        searchIndex);

    ensureWalListenerRegistered();
    release();
  }

  /** Restores a retained task from a terminal marker after node restart. */
  boolean recoverTerminalTask(final String loadId, final File taskDir) {
    final RetainedTaskDir retained = readTerminalMarker(taskDir);
    if (retained == null) {
      return false;
    }
    loadId2RetainedTaskDir.put(loadId, retained);
    ensureWalListenerRegistered();
    return true;
  }

  // -------------------------------------------------------------------------
  // Watermark Sweeping & Cleanup Dispatch
  // -------------------------------------------------------------------------

  /** Evaluates consensus watermarks and delegates eligible directories to the cleaner. */
  void release() {
    if (loadId2RetainedTaskDir.isEmpty()) {
      return;
    }

    final long safelyDeletedSearchIndex = currentSafelyDeletedSearchIndex();
    final long boundary =
        safelyDeletedSearchIndex == ConsensusReqReader.DEFAULT_SAFELY_DELETED_SEARCH_INDEX
            ? Long.MAX_VALUE
            : safelyDeletedSearchIndex;

    for (final Map.Entry<String, RetainedTaskDir> entry : loadId2RetainedTaskDir.entrySet()) {
      final RetainedTaskDir retained = entry.getValue();
      if (retained.reachedSearchIndex() <= boundary
          && loadId2RetainedTaskDir.remove(entry.getKey(), retained)) {
        LOGGER.info(
            StorageEngineMessages
                .LOG_RELEASED_THE_STAGED_DIRECTORY_ARG_OF_LOAD_TASK_ARG_BECAUSE_THE_SAFE_DELETION_SEARCH_INDEX_ARG_IS_REACHED_0CF83F8A,
            retained.dir().getAbsolutePath(),
            entry.getKey(),
            safelyDeletedSearchIndex);
        dispatchToCleaner(entry.getKey(), retained);
      }
    }

    LoadTsFileCleaner.getInstance().start();
    LoadTsFileCleaner.getInstance().sweep();
  }

  private void dispatchToCleaner(final String loadId, final RetainedTaskDir retained) {
    LoadTsFileConsensusOp op = retained.terminalOp();
    if (op == null) {
      op = resolveTerminalOpFromProgressFiles(retained.dir());
    }
    LoadTsFileCleaner.getInstance()
        .register(loadId, dataRegion, retained.dir(), retained.reachedSearchIndex(), op);
  }

  private LoadTsFileConsensusOp resolveTerminalOpFromProgressFiles(final File taskDir) {
    final File[] files = taskDir.listFiles();
    if (files != null) {
      for (final File file : files) {
        if (!file.getName().endsWith(LoadTsFileProgress.PROGRESS_SUFFIX)) {
          continue;
        }
        try {
          final LoadTsFileProgress.TerminalRecord terminal = LoadTsFileProgress.readTerminal(file);
          if (terminal != null) {
            return terminal.op();
          }
        } catch (final IOException e) {
          LOGGER.warn(StorageEngineMessages.LOG_FAILED_TO_DELETE_ARG_3A7BD6FD, file.getPath(), e);
        }
      }
    }
    return LoadTsFileConsensusOp.COMMIT;
  }

  private void ensureWalListenerRegistered() {
    if (listenerRegistered.compareAndSet(false, true)) {
      dataRegion
          .getWALNode()
          .ifPresent(wal -> wal.setSafeDeletedSearchIndexListener(index -> release()));
    }
  }

  private long currentSafelyDeletedSearchIndex() {
    return dataRegion
        .getWALNode()
        .map(IWALNode::getSafelyDeletedSearchIndex)
        .orElse(ConsensusReqReader.DEFAULT_SAFELY_DELETED_SEARCH_INDEX);
  }

  private RetainedTaskDir readTerminalMarker(final File taskDir) {
    final File marker = new File(taskDir, TERMINAL_MARKER_FILE_NAME);
    if (!marker.isFile()) {
      return null;
    }
    long reachedSearchIndex = ConsensusReqReader.DEFAULT_SAFELY_DELETED_SEARCH_INDEX;
    try {
      final String content =
          new String(Files.readAllBytes(marker.toPath()), StandardCharsets.UTF_8).trim();
      reachedSearchIndex = Long.parseLong(content);
    } catch (final IOException | NumberFormatException e) {
      LOGGER.warn(
          StorageEngineMessages.LOG_LOAD_CONSENSUS_RECOVER_TASK_META_FAILED_C39E04BB,
          taskDir.getName(),
          e);
    }
    return new RetainedTaskDir(taskDir, reachedSearchIndex, null);
  }

  // -------------------------------------------------------------------------
  // Data Records
  // -------------------------------------------------------------------------

  /** Tracks a finished staged directory waiting for the consensus watermark. */
  private record RetainedTaskDir(
      File dir, long reachedSearchIndex, LoadTsFileConsensusOp terminalOp) {}
}
