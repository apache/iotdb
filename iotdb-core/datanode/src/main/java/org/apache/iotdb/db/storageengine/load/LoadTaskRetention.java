/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps the staged directories of finished LOAD tasks alive for as long as the consensus layer may
 * still need them, and deletes them once it does not.
 *
 * <p>A task that reached COMMIT or ABORT owns no resumable state any more, but the WAL entry of
 * that task still refers to the bytes it staged. A follower that catches up by reading this node's
 * WAL reads those bytes back to build the request it receives, so a directory is released only
 * after the safe-deletion watermark of the WAL got past the search index of the command that ended
 * the task. Every replica decides this for its own staged directory: the watermark it observes is
 * its own, and the bytes another replica may need are the ones that replica staged itself.
 *
 * <p>Those bytes therefore have to survive the command that ends the task: a COMMIT imports a copy
 * of the staged file while this instance owns it, so the staging directory keeps the very file the
 * WAL entries point into until the release.
 *
 * <p>A directory is remembered across a restart by a terminal marker written next to the staged
 * files, which also records the search index the command carried, so a restart finishes a release
 * that was still pending instead of resuming a task that is already over.
 */
final class LoadTaskRetention {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTaskRetention.class);

  /**
   * Marker written into the staged directory of a load that already reached COMMIT or ABORT. While
   * the marker exists the directory holds no resumable task, and it must be deleted instead of
   * being recovered after a restart.
   */
  private static final String TERMINAL_MARKER_FILE_NAME = "terminal.marker";

  private final DataRegion dataRegion;

  /**
   * Staged directories of loads that already reached COMMIT or ABORT, mapped to the search index of
   * that command. They are kept until the consensus layer reports that every follower got past that
   * index, because until then a follower may still need the referenced bytes to catch up from the
   * WAL.
   */
  private final Map<String, RetainedTaskDir> loadId2RetainedTaskDir = new ConcurrentHashMap<>();

  /** Whether the WAL of the region already reports its watermark to this instance. */
  private volatile boolean listenerRegistered;

  LoadTaskRetention(final DataRegion dataRegion) {
    this.dataRegion = dataRegion;
  }

  /**
   * @return whether the staged directory of a task that just reached COMMIT or ABORT must survive,
   *     because a follower may still read its bytes back from this node's WAL.
   */
  boolean mustRetain(final long searchIndex) {
    final long safelyDeletedSearchIndex = currentSafelyDeletedSearchIndex();
    // No consensus watermark is reported (single replica), or every follower already got past this
    // command, so nothing can read those bytes back any more.
    return safelyDeletedSearchIndex != ConsensusReqReader.DEFAULT_SAFELY_DELETED_SEARCH_INDEX
        && safelyDeletedSearchIndex < searchIndex;
  }

  /**
   * Marks a directory as finished before its progress files are dropped, so that a crash in between
   * still makes the restart delete the directory instead of resuming the finished task.
   */
  void markTerminal(
      final File taskDir, final LoadTsFileConsensusOp terminalOp, final long searchIndex) {
    // The tail of every progress file of the task records the command it finished with, which is
    // what a scan of the directories reads back after a restart.
    final File[] files = taskDir.listFiles();
    if (files != null) {
      for (final File file : files) {
        final String name = file.getName();
        final int progressSuffixAt = name.lastIndexOf(LoadTsFileProgress.PROGRESS_SUFFIX);
        if (progressSuffixAt < 0) {
          continue;
        }
        try {
          new LoadTsFileProgress(
                  new File(file.getParentFile(), name.substring(0, progressSuffixAt)))
              .recordTerminal(terminalOp, searchIndex);
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
      Files.write(
          taskDir.toPath().resolve(TERMINAL_MARKER_FILE_NAME),
          Long.toString(searchIndex).getBytes(StandardCharsets.UTF_8));
    } catch (final IOException e) {
      LOGGER.warn(
          StorageEngineMessages.LOG_LOAD_CONSENSUS_TERMINAL_MARKER_WRITE_FAILED_4D6D7433,
          taskDir.getName(),
          taskDir.getAbsolutePath(),
          e);
    }
  }

  /**
   * Takes ownership of a staged directory that already reached COMMIT or ABORT, which the caller
   * has closed for writing. The staged bytes are still on disk — the import of a committed task was
   * made from a copy of them — so they stay readable until the watermark allows the deletion.
   */
  void retain(
      final String loadId,
      final File taskDir,
      final LoadTsFileConsensusOp terminalOp,
      final long searchIndex) {
    // The dedicated cleaner owns the deletion: an aborted task goes away as soon as the command was
    // applied, a committed one once every replica got past it and the staged files are complete.
    LoadTsFileCleaner.getInstance().register(loadId, dataRegion, taskDir, searchIndex, terminalOp);
    LoadTsFileCleaner.getInstance().start();
    loadId2RetainedTaskDir.put(loadId, new RetainedTaskDir(taskDir, searchIndex));
    LOGGER.info(
        StorageEngineMessages
            .LOG_KEEPING_THE_STAGED_DIRECTORY_ARG_OF_LOAD_TASK_ARG_UNTIL_THE_SAFE_DELETION_SEARCH_INDEX_ARG_IS_REACHED_AFE2BC88,
        taskDir.getAbsolutePath(),
        loadId,
        searchIndex);
    if (!listenerRegistered) {
      // Every retained directory is released through the same callback, so registering it once is
      // enough: the callback releases whatever is still waiting, not only the directory that
      // registered it.
      listenerRegistered = true;
      dataRegion
          .getWALNode()
          .ifPresent(wal -> wal.setSafeDeletedSearchIndexListener(index -> release()));
    }
    // The watermark may have got past this command between the decision to retain and the
    // registration above, and a region that stops writing reports no new watermark at all: trying
    // the release right away deletes such a directory instead of leaving it on disk until the next
    // restart.
    release();
  }

  /**
   * Restores a pending release recorded on disk by a previous run.
   *
   * @return whether the directory held a terminal marker, that is, whether it belongs to a task
   *     that reached COMMIT or ABORT before the restart and must not be resumed
   */
  boolean recoverTerminalTask(final String loadId, final File taskDir) {
    final RetainedTaskDir retained = readTerminalMarker(taskDir);
    if (retained == null) {
      return false;
    }
    loadId2RetainedTaskDir.put(loadId, retained);
    return true;
  }

  /**
   * Deletes the staged directories of finished loads once the consensus layer reports that every
   * follower got past the COMMIT or ABORT command they belong to.
   */
  void release() {
    if (loadId2RetainedTaskDir.isEmpty()) {
      return;
    }
    // The deletion rule lives in the cleaner, which reads the COMMIT or ABORT every directory
    // finished with from the tail of its progress files and deletes it only once that command
    // reached every replica. Without a consensus watermark nothing can be served from the WAL any
    // more, so the directories are released right away, which is the behaviour of consensus V2 and
    // of regions without replication.
    final long safelyDeletedSearchIndex = currentSafelyDeletedSearchIndex();
    final long boundary =
        safelyDeletedSearchIndex == ConsensusReqReader.DEFAULT_SAFELY_DELETED_SEARCH_INDEX
            ? Long.MAX_VALUE
            : safelyDeletedSearchIndex;
    for (final Map.Entry<String, RetainedTaskDir> entry : loadId2RetainedTaskDir.entrySet()) {
      final RetainedTaskDir retained = entry.getValue();
      if (retained.reachedSearchIndex <= boundary
          && loadId2RetainedTaskDir.remove(entry.getKey(), retained)) {
        LOGGER.info(
            StorageEngineMessages
                .LOG_RELEASED_THE_STAGED_DIRECTORY_ARG_OF_LOAD_TASK_ARG_BECAUSE_THE_SAFE_DELETION_SEARCH_INDEX_ARG_IS_REACHED_0CF83F8A,
            retained.dir.getAbsolutePath(),
            entry.getKey(),
            safelyDeletedSearchIndex);
        registerWithCleaner(entry.getKey(), retained);
      }
    }
    LoadTsFileCleaner.getInstance().start();
    LoadTsFileCleaner.getInstance().sweep();
  }

  /**
   * Hands a directory that waited for the watermark to the cleaner, which decides whether its bytes
   * are still needed: an aborted task is deleted right away, a committed one only once its staged
   * files are complete as well.
   */
  private void registerWithCleaner(final String loadId, final RetainedTaskDir retained) {
    LoadTsFileConsensusOp terminalOp = LoadTsFileConsensusOp.COMMIT;
    final File[] files = retained.dir.listFiles();
    if (files != null) {
      for (final File file : files) {
        final String name = file.getName();
        final int progressSuffixAt = name.lastIndexOf(LoadTsFileProgress.PROGRESS_SUFFIX);
        if (progressSuffixAt < 0) {
          continue;
        }
        try {
          final LoadTsFileProgress.TerminalRecord terminal = LoadTsFileProgress.readTerminal(file);
          if (terminal != null) {
            terminalOp = terminal.op();
            break;
          }
        } catch (final IOException e) {
          LOGGER.warn(LoadStagingDirs.MESSAGE_DELETE_FAIL, file.getPath(), e);
        }
      }
    }
    LoadTsFileCleaner.getInstance()
        .register(loadId, dataRegion, retained.dir, retained.reachedSearchIndex, terminalOp);
  }

  private long currentSafelyDeletedSearchIndex() {
    return dataRegion
        .getWALNode()
        .map(IWALNode::getSafelyDeletedSearchIndex)
        .orElse(ConsensusReqReader.DEFAULT_SAFELY_DELETED_SEARCH_INDEX);
  }

  /**
   * @return the retained directory recorded by a terminal marker, or null if the directory holds no
   *     marker and thus may still be an in-progress task.
   */
  private RetainedTaskDir readTerminalMarker(final File taskDir) {
    final File marker = new File(taskDir, TERMINAL_MARKER_FILE_NAME);
    if (!marker.isFile()) {
      return null;
    }
    long reachedSearchIndex = ConsensusReqReader.DEFAULT_SAFELY_DELETED_SEARCH_INDEX;
    try {
      reachedSearchIndex = Long.parseLong(new String(Files.readAllBytes(marker.toPath())).trim());
    } catch (final IOException | NumberFormatException e) {
      LOGGER.warn(
          StorageEngineMessages.LOG_LOAD_CONSENSUS_RECOVER_TASK_META_FAILED_C39E04BB,
          taskDir.getName(),
          e);
    }
    return new RetainedTaskDir(taskDir, reachedSearchIndex);
  }

  /**
   * A staged directory of a load whose COMMIT or ABORT command has already been applied, together
   * with the search index of that command.
   */
  private static class RetainedTaskDir {

    private final File dir;
    private final long reachedSearchIndex;

    private RetainedTaskDir(final File dir, final long reachedSearchIndex) {
      this.dir = dir;
      this.reachedSearchIndex = reachedSearchIndex;
    }
  }
}
