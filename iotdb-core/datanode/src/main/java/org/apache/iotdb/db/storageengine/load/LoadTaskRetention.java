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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Retains staged directories of finished LOAD tasks until WAL watermarks confirm that follower
 * replicas have synchronized the required chunks, preventing premature deletion.
 *
 * <p>A retained directory keeps the staged files and their progress files, whose tail holds the
 * command the task ended with: a restart reads that record to queue the directory for release
 * instead of resuming the task.
 */
final class LoadTaskRetention {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTaskRetention.class);

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
    return currentSafelyDeletedSearchIndex() < searchIndex;
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

  /**
   * Rebuilds the in-memory bookkeeping of a task at startup, once per staged directory: the map
   * dies with the process but the directory does not, so its terminal record is what tells this
   * restart to release the directory instead of resuming a finished task.
   */
  boolean recoverTerminalTask(final String loadId, final File taskDir) {
    final LoadTsFileProgress.TerminalRecord terminal =
        LoadTsFileProgress.readTerminalOfTaskDir(taskDir);
    if (terminal == null) {
      return false;
    }
    loadId2RetainedTaskDir.put(
        loadId, new RetainedTaskDir(taskDir, terminal.searchIndex(), terminal.op()));
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
    // The default V1 watermark means no replica progress has been confirmed yet.
    if (safelyDeletedSearchIndex == ConsensusReqReader.DEFAULT_SAFELY_DELETED_SEARCH_INDEX) {
      return;
    }

    for (final Map.Entry<String, RetainedTaskDir> entry : loadId2RetainedTaskDir.entrySet()) {
      final RetainedTaskDir retained = entry.getValue();
      if (retained.reachedSearchIndex() <= safelyDeletedSearchIndex
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
    // Only the registered tasks can become releasable here; the disk is swept on its own period.
    LoadTsFileCleaner.getInstance().sweepRegisteredTasks();
  }

  private void dispatchToCleaner(final String loadId, final RetainedTaskDir retained) {
    LoadTsFileCleaner.getInstance()
        .register(
            loadId,
            dataRegion,
            retained.dir(),
            retained.reachedSearchIndex(),
            retained.terminalOp());
  }

  private void ensureWalListenerRegistered() {
    if (listenerRegistered.compareAndSet(false, true)) {
      dataRegion
          .getWALNode()
          .ifPresent(wal -> wal.setSafeDeletedSearchIndexListener(index -> release()));
    }
  }

  private long currentSafelyDeletedSearchIndex() {
    // V2 replicates through pipe. Other protocols without a V1 WAL (for example Ratis) do not
    // expand staged references from this node's WAL either, so neither has a watermark to wait for.
    if (LoadConsensusCompat.usesIoTConsensusV2()) {
      return Long.MAX_VALUE;
    }
    return dataRegion
        .getWALNode()
        .map(IWALNode::getSafelyDeletedSearchIndex)
        .orElse(Long.MAX_VALUE);
  }

  // -------------------------------------------------------------------------
  // Data Records
  // -------------------------------------------------------------------------

  /**
   * Tracks a finished staged directory waiting for the consensus watermark. The op decides whether
   * completeness is still required before deleting, since an aborted directory is garbage while a
   * committed one has to be complete, and the index is the watermark its command reached.
   */
  private record RetainedTaskDir(
      File dir, long reachedSearchIndex, LoadTsFileConsensusOp terminalOp) {}
}
