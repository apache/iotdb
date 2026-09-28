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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;

import org.apache.tsfile.external.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import static org.apache.iotdb.db.storageengine.load.LoadTsFileProgress.PROGRESS_SUFFIX;

/**
 * Snapshot manager for active LOAD staged files during replica migration and backup. Provides safe
 * non-blocking file copies, crash consistency, and auto-catchup against live writes.
 */
public final class LoadTsFileSnapshot {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileSnapshot.class);

  public static final String SNAPSHOT_SUBDIR_NAME = IoTDBConstant.LOAD_TSFILE_FOLDER_NAME;
  private static final String TEMP_FILE_SUFFIX = ".copying.";
  private static final String ROOTS_MANIFEST_NAME = "roots";

  private static final int MAX_STAGED_FILE_REPAIRS = 8;
  private static final long STAGED_FILE_CATCH_UP_WAIT_MS = 5L;

  private static final Set<String> SUPPORTED_PROTOCOLS =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  ConsensusFactory.IOT_CONSENSUS,
                  ConsensusFactory.IOT_CONSENSUS_V2,
                  ConsensusFactory.LEGACY_IOT_CONSENSUS_V2,
                  ConsensusFactory.REAL_IOT_CONSENSUS_V2)));

  private LoadTsFileSnapshot() {}

  // -------------------------------------------------------------------------
  // Snapshot Entrypoint
  // -------------------------------------------------------------------------

  /** Captures an atomic snapshot of active staged tasks under the DataRegion. */
  public static boolean snapshot(final DataRegion dataRegion, final File snapshotDir) {
    Objects.requireNonNull(
        dataRegion, StorageEngineMessages.EXCEPTION_DATAREGION_CANNOT_BE_NULL_0B936879);
    Objects.requireNonNull(
        snapshotDir, StorageEngineMessages.EXCEPTION_SNAPSHOTDIR_CANNOT_BE_NULL_283211CB);

    if (!SUPPORTED_PROTOCOLS.contains(
        IoTDBDescriptor.getInstance().getConfig().getDataRegionConsensusProtocolClass())) {
      return true;
    }

    final LoadTsFileManager manager = dataRegion.getLoadTsFileManagerIfPresent().orElse(null);
    if (manager == null) {
      return true;
    }

    final List<File> taskDirs = manager.getActiveTaskDirs();
    if (taskDirs.isEmpty()) {
      return true;
    }

    final File loadSnapshotDir = new File(snapshotDir, SNAPSHOT_SUBDIR_NAME);
    try {
      writeRootsManifest(loadSnapshotDir, taskDirs);
      for (final File taskDir : taskDirs) {
        final File targetDir = new File(loadSnapshotDir, taskDir.getName());
        copyTask(taskDir, targetDir);
      }

      LOGGER.info(
          String.format(
              StorageEngineMessages.LOG_LOAD_CONSENSUS_SNAPSHOT_TAKEN_09A7DD4C,
              taskDirs.size(),
              countFiles(loadSnapshotDir),
              dataRegion.getDatabaseName()
                  + IoTDBConstant.FILE_NAME_SEPARATOR
                  + dataRegion.getDataRegionIdString(),
              snapshotDir.getAbsolutePath()));
      return true;
    } catch (final IOException e) {
      LOGGER.warn(StorageEngineMessages.CATCH_IO_EXCEPTION_CREATING_SNAPSHOT, e);
      return false;
    }
  }

  // -------------------------------------------------------------------------
  // Task Copy Pipeline & Live Catch-Up
  // -------------------------------------------------------------------------

  /**
   * Copies task files in strict order (data files -> progress logs) and repairs lagging lengths.
   */
  private static void copyTask(final File taskDir, final File targetDir) throws IOException {
    // 1. Snapshot raw staged and modification files first
    for (final File file : listFiles(taskDir)) {
      if (file.isFile() && !isProgressLog(file)) {
        publishAtomicCopy(file, new File(targetDir, file.getName()));
      }
    }

    // 2. Snapshot progress logs afterwards so they describe an existing snapshot boundary
    for (final File file : listFiles(taskDir)) {
      if (file.isFile() && isProgressLog(file)) {
        publishAtomicCopy(file, new File(targetDir, file.getName()));
      }
    }

    // 3. Catch up staged files if progress logs advanced during file copy
    catchUpStagedFiles(taskDir, targetDir);
  }

  /**
   * Catches up snapshot staged files against live source files if progress logs record more bytes.
   */
  private static void catchUpStagedFiles(final File sourceTaskDir, final File snapshotTaskDir)
      throws IOException {
    for (final File progressFile : listFiles(snapshotTaskDir)) {
      if (!progressFile.isFile() || !isProgressLog(progressFile)) {
        continue;
      }

      final String baseName =
          progressFile
              .getName()
              .substring(0, progressFile.getName().length() - PROGRESS_SUFFIX.length());
      final File snapshotStagedFile = new File(snapshotTaskDir, baseName);
      final File sourceStagedFile = new File(sourceTaskDir, baseName);

      if (!snapshotStagedFile.isFile() || !sourceStagedFile.isFile()) {
        continue;
      }

      long expectedLength = recordedLengthOf(snapshotStagedFile);
      for (int i = 0;
          i < MAX_STAGED_FILE_REPAIRS && expectedLength > snapshotStagedFile.length();
          i++) {
        copyPrefix(sourceStagedFile, snapshotStagedFile, expectedLength);
        if (expectedLength > snapshotStagedFile.length()) {
          waitForCatchUp();
        }
        expectedLength = recordedLengthOf(snapshotStagedFile);
      }
    }
  }

  private static long recordedLengthOf(final File snapshotStagedFile) {
    try {
      return new LoadTsFileProgress(snapshotStagedFile)
          .readAllRecordsRepairingTornTail().stream()
              .mapToLong(LoadTsFileProgress.ChunkRangeRecord::physicalEnd)
              .max()
              .orElse(-1L);
    } catch (final IOException e) {
      LOGGER.warn(
          StorageEngineMessages.LOG_LOAD_CONSENSUS_SNAPSHOT_PROGRESS_UNREADABLE_ARG_ARG_509E36FD,
          snapshotStagedFile.getAbsolutePath(),
          e.getMessage());
      return -1L;
    }
  }

  private static void waitForCatchUp() {
    try {
      Thread.sleep(STAGED_FILE_CATCH_UP_WAIT_MS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  // -------------------------------------------------------------------------
  // Restore & Cleanup Operations
  // -------------------------------------------------------------------------

  public static void clear(final String databaseName, final String dataRegionIdString)
      throws IOException {
    for (final String loadBaseDir : LoadStagingDirs.baseDirs()) {
      final File regionLoadDir =
          LoadStagingDirs.regionLoadDir(new File(loadBaseDir), databaseName, dataRegionIdString);
      if (regionLoadDir.exists()) {
        FileUtils.forceDelete(regionLoadDir);
      }
    }
  }

  public static void restore(
      final String databaseName, final String dataRegionIdString, final File snapshotDir)
      throws IOException {
    final File loadSnapshotDir = new File(snapshotDir, SNAPSHOT_SUBDIR_NAME);
    if (!loadSnapshotDir.isDirectory()) {
      return;
    }

    final String[] loadBaseDirs = LoadStagingDirs.baseDirs();
    if (loadBaseDirs.length == 0) {
      return;
    }

    final Map<String, Integer> taskRoots = readRootsManifest(loadSnapshotDir);
    final File[] taskDirs = loadSnapshotDir.listFiles();
    if (taskDirs == null) {
      return;
    }

    int taskCount = 0;
    int fileCount = 0;
    for (final File taskDir : taskDirs) {
      if (!taskDir.isDirectory()) {
        continue;
      }

      final File[] files = taskDir.listFiles();
      if (files == null) {
        continue;
      }

      final int rootIndex = resolveRootIndex(taskRoots, taskDir.getName(), loadBaseDirs.length);
      final File targetDir =
          new File(
              LoadStagingDirs.regionLoadDir(
                  new File(loadBaseDirs[rootIndex]), databaseName, dataRegionIdString),
              taskDir.getName());

      taskCount++;
      for (final File file : files) {
        if (file.isFile()) {
          publishAtomicCopy(file, new File(targetDir, file.getName()));
          fileCount++;
        }
      }
    }

    if (taskCount > 0) {
      LOGGER.info(
          String.format(
              StorageEngineMessages.LOG_LOAD_CONSENSUS_SNAPSHOT_RESTORED_90ABC1BF,
              taskCount,
              fileCount,
              snapshotDir.getAbsolutePath()));
    }
  }

  public static List<File> collectSnapshotFiles(final File snapshotDir) throws IOException {
    final File loadSnapshotDir = new File(snapshotDir, SNAPSHOT_SUBDIR_NAME);
    if (!loadSnapshotDir.isDirectory()) {
      return Collections.emptyList();
    }

    final List<File> fileList = new ArrayList<>();
    Files.walkFileTree(
        loadSnapshotDir.toPath(),
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) {
            if (!file.getFileName().toString().contains(TEMP_FILE_SUFFIX)) {
              fileList.add(file.toFile());
            }
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFileFailed(final Path file, final IOException exc)
              throws IOException {
            throw new IOException(
                String.format(
                    StorageEngineMessages
                        .EXCEPTION_FAILED_TO_ENUMERATE_THE_LOAD_SNAPSHOT_FILE_ARG_ARG_9105BFC5,
                    file,
                    exc.getMessage()),
                exc);
          }
        });
    return fileList;
  }

  // -------------------------------------------------------------------------
  // Atomic File Copy & Low-Level NIO Operations
  // -------------------------------------------------------------------------

  /** Copies a file to a temp target and atomically renames it upon completion. */
  private static void publishAtomicCopy(final File source, final File target) throws IOException {
    final File parent = target.getParentFile();
    if (!parent.exists() && !parent.mkdirs()) {
      throw new IOException(
          String.format(StorageEngineMessages.FAILED_TO_CREATE_DIR, parent.getAbsolutePath()));
    }

    final File tempTarget =
        new File(parent, target.getName() + TEMP_FILE_SUFFIX + UUID.randomUUID());
    try {
      Files.copy(source.toPath(), tempTarget.toPath(), StandardCopyOption.REPLACE_EXISTING);
      atomicMoveOrFallback(tempTarget, target);
    } catch (final IOException e) {
      Files.deleteIfExists(tempTarget.toPath());
      throw e;
    }
  }

  /** Copies up to {@code length} bytes from source to target via zero-copy channel transfer. */
  private static void copyPrefix(final File source, final File target, final long length)
      throws IOException {
    final File parent = target.getParentFile();
    final File tempTarget =
        new File(parent, target.getName() + TEMP_FILE_SUFFIX + UUID.randomUUID());

    try (final FileChannel srcChannel = FileChannel.open(source.toPath(), StandardOpenOption.READ);
        final FileChannel dstChannel =
            FileChannel.open(
                tempTarget.toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

      long position = 0L;
      while (position < length) {
        final long transferred = srcChannel.transferTo(position, length - position, dstChannel);
        if (transferred <= 0L) {
          break;
        }
        position += transferred;
      }
      atomicMoveOrFallback(tempTarget, target);
    } catch (final IOException e) {
      Files.deleteIfExists(tempTarget.toPath());
      throw e;
    }
  }

  private static void atomicMoveOrFallback(final File source, final File target)
      throws IOException {
    try {
      Files.move(
          source.toPath(),
          target.toPath(),
          StandardCopyOption.REPLACE_EXISTING,
          StandardCopyOption.ATOMIC_MOVE);
    } catch (final AtomicMoveNotSupportedException e) {
      Files.move(source.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }
  }

  // -------------------------------------------------------------------------
  // Manifest & Helper Functions
  // -------------------------------------------------------------------------

  private static void writeRootsManifest(final File loadSnapshotDir, final List<File> taskDirs)
      throws IOException {
    final File manifestFile = new File(loadSnapshotDir, ROOTS_MANIFEST_NAME);
    if (!loadSnapshotDir.exists() && !loadSnapshotDir.mkdirs()) {
      throw new IOException(
          String.format(
              StorageEngineMessages.FAILED_TO_CREATE_DIR, loadSnapshotDir.getAbsolutePath()));
    }

    final File tempFile =
        new File(loadSnapshotDir, ROOTS_MANIFEST_NAME + TEMP_FILE_SUFFIX + UUID.randomUUID());
    try (final BufferedWriter writer =
        new BufferedWriter(new FileWriter(tempFile, StandardCharsets.UTF_8))) {
      for (final File taskDir : taskDirs) {
        writer.write(taskDir.getName());
        writer.write(' ');
        writer.write(String.valueOf(LoadStagingDirs.baseDirIndexOf(taskDir)));
        writer.newLine();
      }
    }
    atomicMoveOrFallback(tempFile, manifestFile);
  }

  private static Map<String, Integer> readRootsManifest(final File loadSnapshotDir) {
    final File manifestFile = new File(loadSnapshotDir, ROOTS_MANIFEST_NAME);
    if (!manifestFile.isFile()) {
      return Collections.emptyMap();
    }

    final Map<String, Integer> taskRoots = new HashMap<>();
    try (final BufferedReader reader =
        new BufferedReader(new FileReader(manifestFile, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        final int sep = line.lastIndexOf(' ');
        if (sep > 0) {
          try {
            taskRoots.put(line.substring(0, sep), Integer.parseInt(line.substring(sep + 1)));
          } catch (final NumberFormatException ignored) {
            // Skip invalid root index hints
          }
        }
      }
    } catch (final IOException e) {
      LOGGER.warn(StorageEngineMessages.CATCH_IO_EXCEPTION_CREATING_SNAPSHOT, e);
      return Collections.emptyMap();
    }
    return taskRoots;
  }

  private static int resolveRootIndex(
      final Map<String, Integer> taskRoots, final String taskName, final int rootCount) {
    final Integer recorded = taskRoots.get(taskName);
    if (recorded == null || recorded < 0) {
      return 0;
    }
    return recorded < rootCount ? recorded : recorded % rootCount;
  }

  private static boolean isProgressLog(final File file) {
    return file.getName().endsWith(PROGRESS_SUFFIX);
  }

  private static File[] listFiles(final File dir) throws IOException {
    final File[] files = dir.listFiles();
    if (files == null && !dir.exists()) {
      return new File[0];
    }
    if (files == null) {
      throw new IOException(
          String.format(
              StorageEngineMessages
                  .EXCEPTION_FAILED_TO_ENUMERATE_THE_LOAD_SNAPSHOT_DIRECTORY_ARG_ARG_E2890E70,
              dir.getAbsolutePath(),
              StorageEngineMessages.MESSAGE_THE_DIRECTORY_IS_UNREADABLE_OR_MISSING_54036A44));
    }
    return files;
  }

  private static int countFiles(final File dir) throws IOException {
    int count = 0;
    for (final File file : listFiles(dir)) {
      count += file.isDirectory() ? countFiles(file) : 1;
    }
    return count;
  }
}
