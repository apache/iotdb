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

import org.apache.iotdb.commons.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Owns everything about carrying in-progress LOAD staging files through a DataRegion snapshot:
 * snapshot inclusion, restore registration and the {@code snapshot.meta} format. Live data writing
 * never touches this class, which is what lets it be factored out of the LOAD facade.
 */
final class LoadSnapshotManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadSnapshotManager.class);

  private static final String LOAD_SNAPSHOT_META_NAME = "snapshot.meta";
  private static final String APPLIED_PIECES_PREFIX = "#applied ";

  private final LoadTaskRegistry registry;
  private final TaskDirAllocator taskDirAllocator;

  LoadSnapshotManager(LoadTaskRegistry registry, TaskDirAllocator taskDirAllocator) {
    this.registry = registry;
    this.taskDirAllocator = taskDirAllocator;
  }

  /**
   * Includes the in-progress LOAD staging files owned by the given DataRegion into a snapshot.
   *
   * <p>Only the already-synced byte prefix {@code [0, syncedOffset)} of every staged partition file
   * is copied: bytes written after the last chunk-group boundary are still owned by the write path
   * and will be captured by the next PIECE ref, so a replica restored from this snapshot can keep
   * appending from exactly the snapshot length without a hole or an overlap. The registry write
   * lock plus the per-task lock serializes this against concurrent LOAD applies so the synced
   * cursor is stable.
   */
  void snapshotLoadTasksForRegion(DataRegion dataRegion, File snapshotDir) throws IOException {
    final int[] taskCount = new int[1];
    final int[] stagedFileCount = new int[1];
    registry.snapshot(
        writerManager -> {
          if (!writerManager.belongsTo(dataRegion)) {
            return;
          }
          final File taskSnapshotDir =
              new File(
                  snapshotDir,
                  LoadTsFileManager.LOAD_SNAPSHOT_DIR_NAME
                      + File.separator
                      + writerManager.getTaskName());
          if (!taskSnapshotDir.exists() && !taskSnapshotDir.mkdirs()) {
            throw new IOException(
                String.format(
                    StorageEngineMessages.FAILED_TO_CREATE_DIR, taskSnapshotDir.getAbsolutePath()));
          }
          final TaskSnapshot taskSnapshot = writerManager.snapshotTask(taskSnapshotDir);
          if (taskSnapshot.stagedFiles.isEmpty()) {
            return;
          }
          writeSnapshotMeta(new File(taskSnapshotDir, LOAD_SNAPSHOT_META_NAME), taskSnapshot);
          taskCount[0]++;
          stagedFileCount[0] += taskSnapshot.stagedFiles.size();
        });
    if (taskCount[0] > 0) {
      LOGGER.info(
          StorageEngineMessages.LOG_LOAD_CONSENSUS_SNAPSHOT_TAKEN_09A7DD4C,
          taskCount[0],
          stagedFileCount[0],
          dataRegion.getDataRegionIdString(),
          snapshotDir);
    }
  }

  /**
   * Restores the in-progress LOAD staging files carried by a snapshot's {@value
   * LoadTsFileManager#LOAD_SNAPSHOT_DIR_NAME} directory. The restored task dirs are registered so
   * that the coordinator can continue the load (subsequent PIECE refs append to the restored files
   * and COMMIT binds them to the DataRegion).
   */
  void restoreLoadTasksFromSnapshot(File loadSnapshotDir) throws IOException {
    final File[] taskDirs = loadSnapshotDir.listFiles(File::isDirectory);
    if (taskDirs == null) {
      return;
    }
    int taskCount = 0;
    int stagedFileCount = 0;
    for (File taskSnapshotDir : taskDirs) {
      final File metaFile = new File(taskSnapshotDir, LOAD_SNAPSHOT_META_NAME);
      if (!metaFile.isFile()) {
        continue;
      }
      final String uuid = taskSnapshotDir.getName();
      final TaskSnapshot taskSnapshot = parseSnapshotMeta(metaFile);
      TsFileWriterManager writerManager = registry.get(uuid).orElse(null);
      if (writerManager == null) {
        try {
          writerManager =
              registry.getOrCreate(
                  uuid,
                  id -> {
                    final File targetTaskDir = taskDirAllocator.allocate(id);
                    copyLoadSnapshotTaskFiles(taskSnapshotDir, targetTaskDir);
                    return new TsFileWriterManager(targetTaskDir, false);
                  });
        } catch (IOException e) {
          if (e.getCause() instanceof DiskSpaceInsufficientException) {
            throw new IOException(
                String.format(
                    StorageEngineMessages.EXCEPTION_LOAD_CONSENSUS_SNAPSHOT_RESTORE_FAILED_F8C29C64,
                    loadSnapshotDir,
                    e.getCause().getMessage()),
                e);
          }
          throw e;
        }
        taskCount++;
      } else {
        // A snapshot may be spread across several receive folders: merge the remaining files
        // into the task dir created by an earlier fragment of the same load.
        copyLoadSnapshotTaskFiles(taskSnapshotDir, writerManager.getTaskDir());
      }
      writerManager.registerRestoredPartitions(taskSnapshot.stagedFiles);
      writerManager.restoreAppliedPieces(taskSnapshot.appliedPieces);
      // Persist the merged state into the task meta so a later restart rebuilds the same applied
      // prefix and staged file list instead of discarding the load.
      writerManager.persistTaskMeta();
      stagedFileCount += taskSnapshot.stagedFiles.size();
    }
    if (taskCount > 0) {
      LOGGER.info(
          StorageEngineMessages.LOG_LOAD_CONSENSUS_SNAPSHOT_RESTORED_90ABC1BF,
          taskCount,
          stagedFileCount,
          loadSnapshotDir);
    }
  }

  private void copyLoadSnapshotTaskFiles(File sourceTaskDir, File targetTaskDir)
      throws IOException {
    if (!targetTaskDir.exists() && !targetTaskDir.mkdirs()) {
      throw new IOException(
          String.format(
              StorageEngineMessages.FAILED_TO_CREATE_DIR, targetTaskDir.getAbsolutePath()));
    }
    final File[] files = sourceTaskDir.listFiles();
    if (files == null) {
      return;
    }
    for (File file : files) {
      if (file.getName().equals(LOAD_SNAPSHOT_META_NAME)) {
        continue;
      }
      if (file.isDirectory()) {
        copyDirectoryRecursively(file, new File(targetTaskDir, file.getName()));
      } else if (file.isFile()) {
        Files.copy(
            file.toPath(),
            new File(targetTaskDir, file.getName()).toPath(),
            StandardCopyOption.REPLACE_EXISTING);
      }
    }
  }

  private static void copyDirectoryRecursively(File sourceDir, File targetDir) throws IOException {
    if (!targetDir.exists() && !targetDir.mkdirs()) {
      throw new IOException(
          String.format(StorageEngineMessages.FAILED_TO_CREATE_DIR, targetDir.getAbsolutePath()));
    }
    final File[] files = sourceDir.listFiles();
    if (files == null) {
      return;
    }
    for (final File file : files) {
      if (file.isDirectory()) {
        copyDirectoryRecursively(file, new File(targetDir, file.getName()));
      } else if (file.isFile()) {
        Files.copy(
            file.toPath(),
            new File(targetDir, file.getName()).toPath(),
            StandardCopyOption.REPLACE_EXISTING);
      }
    }
  }

  static void writeSnapshotMeta(File metaFile, TaskSnapshot taskSnapshot) throws IOException {
    final StringBuilder sb = new StringBuilder();
    sb.append(APPLIED_PIECES_PREFIX).append(taskSnapshot.appliedPieces).append('\n');
    for (StagedFileSnapshot snapshot : taskSnapshot.stagedFiles) {
      sb.append(snapshot.fileName)
          .append('\t')
          .append(snapshot.database)
          .append('\t')
          .append(snapshot.regionId)
          .append('\t')
          .append(snapshot.timePartitionStart)
          .append('\t')
          .append(snapshot.finalized)
          .append('\n');
    }
    Files.write(metaFile.toPath(), sb.toString().getBytes(StandardCharsets.UTF_8));
  }

  static TaskSnapshot parseSnapshotMeta(File metaFile) throws IOException {
    final List<StagedFileSnapshot> stagedFiles = new ArrayList<>();
    final StringBuilder appliedPieces = new StringBuilder();
    for (String line : Files.readAllLines(metaFile.toPath(), StandardCharsets.UTF_8)) {
      if (line.isEmpty()) {
        continue;
      }
      if (line.startsWith(APPLIED_PIECES_PREFIX)) {
        appliedPieces.append(line.substring(APPLIED_PIECES_PREFIX.length()));
        continue;
      }
      final String[] parts = line.split("\t", -1);
      if (parts.length != 5) {
        throw new IOException(
            String.format(
                StorageEngineMessages.EXCEPTION_LOAD_CONSENSUS_SNAPSHOT_RESTORE_FAILED_F8C29C64,
                metaFile,
                line));
      }
      stagedFiles.add(
          new StagedFileSnapshot(
              parts[0],
              parts[1],
              parts[2],
              Long.parseLong(parts[3]),
              Boolean.parseBoolean(parts[4])));
    }
    return new TaskSnapshot(stagedFiles, appliedPieces.toString());
  }

  /** One task's snapshot payload: the staged-file metadata plus the applied-piece prefix. */
  static final class TaskSnapshot {
    private final List<StagedFileSnapshot> stagedFiles;
    private final String appliedPieces;

    TaskSnapshot(List<StagedFileSnapshot> stagedFiles, String appliedPieces) {
      this.stagedFiles = stagedFiles;
      this.appliedPieces = appliedPieces;
    }

    List<StagedFileSnapshot> getStagedFiles() {
      return stagedFiles;
    }

    String getAppliedPieces() {
      return appliedPieces;
    }
  }

  /** Immutable description of one staged partition file captured by a snapshot. */
  static final class StagedFileSnapshot {
    private final String fileName;
    private final String database;
    private final String regionId;
    private final long timePartitionStart;
    private final boolean finalized;

    StagedFileSnapshot(
        String fileName,
        String database,
        String regionId,
        long timePartitionStart,
        boolean finalized) {
      this.fileName = fileName;
      this.database = database;
      this.regionId = regionId;
      this.timePartitionStart = timePartitionStart;
      this.finalized = finalized;
    }

    String getFileName() {
      return fileName;
    }

    String getDatabase() {
      return database;
    }

    String getRegionId() {
      return regionId;
    }

    long getTimePartitionStart() {
      return timePartitionStart;
    }

    boolean isFinalized() {
      return finalized;
    }
  }

  @FunctionalInterface
  interface TaskDirAllocator {
    File allocate(String uuid) throws Exception;
  }
}
