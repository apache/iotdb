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

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.iotdb.db.storageengine.load.LoadTsFileProgress.PROGRESS_SUFFIX;

/**
 * Snapshot support for the in-progress LOAD staged files.
 *
 * <p>LOAD keeps its staging files in a dedicated directory tree ({@link
 * LoadTsFileManager#getLoadBaseDirs()}) that is independent of the DataRegion {@code
 * sequence}/{@code unsequence} data layout. Its snapshot/restore is therefore kept here, in the
 * {@code load} package, instead of being mixed into the general DataRegion snapshot logic ({@code
 * SnapshotTaker}/{@code SnapshotLoader}), which only knows about regular storage files.
 */
public final class LoadTsFileSnapshot {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileSnapshot.class);

  /**
   * The dedicated sub-directory under a snapshot dir that holds the staged files of in-progress
   * LOAD tasks. Keeping it apart from {@code sequence}/{@code unsequence} ensures LOAD never mixes
   * with regular storage files inside a snapshot.
   */
  public static final String SNAPSHOT_SUBDIR_NAME = IoTDBConstant.LOAD_TSFILE_FOLDER_NAME;

  /** The infix of a file that is still being copied into the snapshot, see {@link #copy}. */
  private static final String TEMP_FILE_SUFFIX = ".copying.";

  /**
   * How many times a copied staged file is re-copied because a progress log that arrived while the
   * snapshot was running describes bytes past the end of the copy. Every repetition copies the
   * prefix the log asks for, so each one either settles the pair or grows the copy towards the
   * length the log already had on disk; the loop below is bounded because a staged file only ever
   * grows.
   */
  private static final int MAX_STAGED_FILE_REPAIRS = 8;

  /**
   * How long a repair of a copied staged file waits for the bytes its progress log already names to
   * appear in it, so a piece caught in the middle of its own write is not snapshotted short.
   */
  private static final long STAGED_FILE_CATCH_UP_WAIT_MS = 5L;

  /**
   * The manifest that records which staging root each task of the snapshot was staged in: one
   * {@code <task directory name> <root index>} line per task. A DataNode may stage its tasks in
   * several roots, and the roots of the node that restores the snapshot need not be the same ones,
   * so the index is a hint that keeps a restored task on the disk it came from; the references of a
   * task resolve under whichever root it ends up in, see {@link
   * LoadStagingDirs#recordedPath(File)}.
   */
  private static final String ROOTS_MANIFEST_NAME = "roots";

  private LoadTsFileSnapshot() {}

  /**
   * The protocols whose snapshot carries the staging directory of a region.
   *
   * <p>A consensus LOAD stages its pieces on the write node, and a replica of IoTConsensus rebuilds
   * the staged files from the entries of the log it applies while it is a member of the region. A
   * replica that joins the region in the middle of a task is not a member for the pieces that were
   * applied before it joined, so it inherits the staged state - and the progress its recovery
   * resumes - from the snapshot that the migration transfers. Ratis replicates the full piece
   * payload to every replica instead, so a payload reaches the staging area of a replica without a
   * transfer of that area, and the load is restarted anyway when a migration is detected while it
   * is running (see {@code LoadTsFileScheduler}).
   */
  private static final Set<String> PROTOCOLS_WITH_STAGING_SNAPSHOT =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  ConsensusFactory.IOT_CONSENSUS,
                  ConsensusFactory.IOT_CONSENSUS_V2,
                  ConsensusFactory.LEGACY_IOT_CONSENSUS_V2,
                  ConsensusFactory.REAL_IOT_CONSENSUS_V2)));

  /**
   * Copies the staged files of the in-progress LOAD tasks of {@code dataRegion} into {@code
   * snapshotDir/load/}. Returns {@code false} on an IO error so the caller can fail and clean up
   * the whole snapshot, mirroring the other snapshot steps.
   */
  public static boolean snapshot(final DataRegion dataRegion, final File snapshotDir) {
    if (!PROTOCOLS_WITH_STAGING_SNAPSHOT.contains(
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

  /**
   * Copies one task directory into a snapshot that is read while the task keeps being applied.
   *
   * <p>The copy cannot be serialized with the piece writer - holding the write path while a
   * snapshot is transferred would stall the region - so what makes it safe is which files are
   * copied in which order, and that the bytes of a staged file that are copied are at least the
   * bytes its own progress log claims:
   *
   * <ol>
   *   <li>the staged files and their modification files are copied first, so the set of staged
   *       files of the snapshot is taken before the logs that describe them are looked at;
   *   <li>the progress logs are copied afterwards, so a log is copied no earlier than the staged
   *       file it describes: the reverse order can catch a log whose staged file is not enumerated
   *       afterwards, which is the one combination a reader cannot repair - the bytes of that piece
   *       are lost from the snapshot while the WAL of the restoring node replays the commands that
   *       follow its snapshot point, not the piece itself;
   *   <li>every copied log is then checked against the copied staged file: a log that arrived while
   *       the copy was running can describe bytes past the end of the copy, and the staged file is
   *       copied again up to the length the log needs. A piece writes its bytes and only then
   *       appends the entry that records them, so a log never describes bytes that are not in the
   *       staged file.
   * </ol>
   *
   * <p>A staged file that arrived between the two scans is therefore never left without its log,
   * and a log is never left describing bytes the snapshot does not hold. What the snapshot does not
   * have to be is a complete task: a piece whose command the restoring node replays is written into
   * the restored staged file at the absolute offsets its layout defines, and a piece whose command
   * the restoring node does not replay was never applied here either.
   */
  private static void copyTask(final File taskDir, final File targetDir) throws IOException {
    copyContentFiles(taskDir, targetDir);
    // The directory is listed again for the logs on purpose: a time partition whose first piece
    // arrived after the task was enumerated still contributes its progress log, and its staged file
    // was picked up by the scan above.
    copyProgressLogs(taskDir, targetDir);
    repairStagedFilesAgainstProgressLogs(targetDir);
  }

  private static void copyContentFiles(final File taskDir, final File targetDir)
      throws IOException {
    for (final File file : listFiles(taskDir)) {
      if (file.isFile() && !isProgressLog(file)) {
        copy(new File(targetDir, file.getName()), file);
      }
    }
  }

  private static void copyProgressLogs(final File taskDir, final File targetDir)
      throws IOException {
    for (final File file : listFiles(taskDir)) {
      if (file.isFile() && isProgressLog(file)) {
        copy(new File(targetDir, file.getName()), file);
      }
    }
  }

  /**
   * Grows every copied staged file that a copied progress log describes beyond its end, so that the
   * snapshot never holds a log whose recorded ranges reach past the bytes that were copied with it.
   *
   * <p>A log that records more bytes than the staged file holds is the state a piece is in while it
   * is being written: the writer emits the header, the payload and the entry that records them one
   * after the other, so a log can name bytes that a reader does not see in the file yet. The copy
   * is repeated for as long as the bytes the log needs keep appearing, and a log that stays ahead
   * of its file is left as it is: the restoring node reads the same short file, sees the recorded
   * ranges it cannot back with bytes, and refuses to seal it (see {@code
   * LoadTsFileProgress#isReady}) instead of importing a file with a hole in it.
   */
  private static void repairStagedFilesAgainstProgressLogs(final File targetDir)
      throws IOException {
    for (final File progressFile : listFiles(targetDir)) {
      if (!progressFile.isFile() || !isProgressLog(progressFile)) {
        continue;
      }
      final File stagedFile =
          new File(
              targetDir,
              progressFile
                  .getName()
                  .substring(0, progressFile.getName().length() - PROGRESS_SUFFIX.length()));
      if (!stagedFile.isFile()) {
        // A log without its staged file: the log is copied after the file scan, so the file existed
        // then and the failure belongs to that copy rather than to this repair.
        continue;
      }
      long recordedLength = recordedLengthOf(stagedFile);
      for (int repair = 0;
          repair < MAX_STAGED_FILE_REPAIRS && recordedLength > stagedFile.length();
          repair++) {
        copyRange(stagedFile, recordedLength);
        if (recordedLength > stagedFile.length()) {
          waitForTheStagedFileToCatchUp();
        }
        recordedLength = recordedLengthOf(stagedFile);
      }
    }
  }

  /**
   * The end of the ranges a copied progress log records, or {@code -1} when it records none or
   * cannot be read at all.
   *
   * <p>The tail is repaired rather than rejected: the log of a task that keeps being applied can be
   * copied in the middle of an entry, and the entries before that fragment describe bytes that are
   * in the staged file. Reading the copy the way the restoring node reads it keeps the two agreeing
   * on what the task staged.
   */
  private static long recordedLengthOf(final File stagedFile) {
    try {
      return new LoadTsFileProgress(stagedFile)
          .readAllRecordsRepairingTornTail().stream()
              .mapToLong(LoadTsFileProgress.ChunkRangeRecord::physicalEnd)
              .max()
              .orElse(-1L);
    } catch (final IOException e) {
      LOGGER.warn(
          StorageEngineMessages.LOG_LOAD_CONSENSUS_SNAPSHOT_PROGRESS_UNREADABLE_ARG_ARG_509E36FD,
          stagedFile.getAbsolutePath(),
          e.getMessage());
      return -1L;
    }
  }

  /**
   * Waits out the window in which a piece has appended the entry that describes its bytes but has
   * not reached the end of the staged file with the bytes themselves.
   */
  private static void waitForTheStagedFileToCatchUp() {
    try {
      Thread.sleep(STAGED_FILE_CATCH_UP_WAIT_MS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static boolean isProgressLog(final File file) {
    return file.getName().endsWith(PROGRESS_SUFFIX);
  }

  /**
   * Records the staging root of every task of the snapshot, before any of their files is copied.
   */
  private static void writeRootsManifest(final File loadSnapshotDir, final List<File> taskDirs)
      throws IOException {
    final StringBuilder manifest = new StringBuilder();
    for (final File taskDir : taskDirs) {
      manifest
          .append(taskDir.getName())
          .append(' ')
          .append(LoadStagingDirs.baseDirIndexOf(taskDir))
          .append(System.lineSeparator());
    }
    final File manifestFile = File.createTempFile(ROOTS_MANIFEST_NAME, TEMP_FILE_SUFFIX);
    try {
      Files.write(manifestFile.toPath(), manifest.toString().getBytes(StandardCharsets.UTF_8));
      copy(new File(loadSnapshotDir, ROOTS_MANIFEST_NAME), manifestFile);
    } finally {
      Files.deleteIfExists(manifestFile.toPath());
    }
  }

  /** Reads the recorded staging root of every task of the snapshot, empty when it holds none. */
  private static Map<String, Integer> readRootsManifest(final File loadSnapshotDir) {
    final File manifestFile = new File(loadSnapshotDir, ROOTS_MANIFEST_NAME);
    if (!manifestFile.isFile()) {
      return Collections.emptyMap();
    }
    final Map<String, Integer> taskRoots = new HashMap<>();
    try {
      for (final String line :
          new String(Files.readAllBytes(manifestFile.toPath()), StandardCharsets.UTF_8)
              .split(System.lineSeparator())) {
        final int separator = line.lastIndexOf(' ');
        if (separator <= 0) {
          continue;
        }
        try {
          taskRoots.put(
              line.substring(0, separator), Integer.parseInt(line.substring(separator + 1)));
        } catch (final NumberFormatException ignored) {
          // A line this node cannot read is a hint it does without, not a failure.
        }
      }
    } catch (final IOException e) {
      LOGGER.warn(StorageEngineMessages.CATCH_IO_EXCEPTION_CREATING_SNAPSHOT, e);
      return Collections.emptyMap();
    }
    return taskRoots;
  }

  /** The root a task of the snapshot is restored into, on a node that may hold fewer roots. */
  private static int rootOf(
      final Map<String, Integer> taskRoots, final String taskName, final int rootCount) {
    final Integer recorded = taskRoots.get(taskName);
    if (recorded == null || recorded < 0) {
      return 0;
    }
    return recorded < rootCount ? recorded : recorded % rootCount;
  }

  /**
   * Lists a directory of the staging area.
   *
   * <p>A directory that is not there holds no file, which is a state the staging area reaches on
   * its own: the directory of a task that finished while the snapshot was being taken is deleted,
   * and the snapshot then holds nothing for it. A directory that is there and still cannot be
   * listed is a failure instead - an unreadable task directory is not an empty one, and a snapshot
   * that silently skipped its files would be transferred as if the task held none of them, leaving
   * the replica that resumes from it without the pieces those files hold.
   */
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

  /**
   * Clears this region's LOAD staging directory before restoring a snapshot, so stale in-progress
   * tasks from before the snapshot cannot leak into the recovered region.
   */
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

  /**
   * Restores the staged files from {@code snapshotDir/load/} into this region's LOAD staging
   * directory. When a snapshot is spread across several receive folders this is called once per
   * folder, and each call merges its {@code load} folder into the same target directory.
   */
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
    final File[] loadIdDirs = loadSnapshotDir.listFiles();
    if (loadIdDirs == null) {
      return;
    }
    int taskCount = 0;
    int fileCount = 0;
    for (final File loadIdDir : loadIdDirs) {
      if (!loadIdDir.isDirectory()) {
        continue;
      }
      final File[] files = loadIdDir.listFiles();
      if (files == null) {
        continue;
      }
      // The task goes back to the root it was staged in when this node has it, and to the roots it
      // has otherwise: the recorded references are relative to a root, so they describe the files
      // wherever they land.
      final File targetDir =
          new File(
              LoadStagingDirs.regionLoadDir(
                  new File(
                      loadBaseDirs[rootOf(taskRoots, loadIdDir.getName(), loadBaseDirs.length)]),
                  databaseName,
                  dataRegionIdString),
              loadIdDir.getName());
      taskCount++;
      for (final File file : files) {
        if (!file.isFile()) {
          continue;
        }
        copy(new File(targetDir, file.getName()), file);
        fileCount++;
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

  /**
   * Collects every file under the dedicated {@code load} folder of a snapshot dir, regardless of
   * suffix, so the snapshot-transfer layer includes the {@code .progress} bitmaps together with the
   * staged TsFiles and their modification files.
   */
  public static List<File> collectSnapshotFiles(final File snapshotDir) throws IOException {
    final File loadSnapshotDir = new File(snapshotDir, SNAPSHOT_SUBDIR_NAME);
    if (!loadSnapshotDir.isDirectory()) {
      return Collections.emptyList();
    }
    final List<File> fileList = new LinkedList<>();
    Files.walkFileTree(
        loadSnapshotDir.toPath(),
        new FileVisitor<Path>() {
          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
              throws IOException {
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            if (!file.getFileName().toString().contains(TEMP_FILE_SUFFIX)) {
              // A file that is still being copied is not part of the snapshot: it is published
              // under its final name once its copy is complete.
              fileList.add(file.toFile());
            }
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
            // A staged file that cannot be enumerated would be missing from the snapshot while the
            // snapshot still reports success, and restoring it would resume a task with holes. The
            // failure has to abort the snapshot instead of shortening it silently.
            throw new IOException(
                String.format(
                    StorageEngineMessages
                        .EXCEPTION_FAILED_TO_ENUMERATE_THE_LOAD_SNAPSHOT_FILE_ARG_ARG_9105BFC5,
                    file,
                    exc.getMessage()),
                exc);
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            if (exc != null) {
              throw new IOException(
                  String.format(
                      StorageEngineMessages
                          .EXCEPTION_FAILED_TO_ENUMERATE_THE_LOAD_SNAPSHOT_DIRECTORY_ARG_ARG_E2890E70,
                      dir,
                      exc.getMessage()),
                  exc);
            }
            return FileVisitResult.CONTINUE;
          }
        });
    return fileList;
  }

  /**
   * Copies one file into the snapshot, publishing it under its final name only once it is complete.
   *
   * <p>The snapshot directory is read by the transfer layer while it is still being filled, so a
   * file that appears under its final name has to be complete: a partially copied staged TsFile
   * would otherwise be transferred as if it were whole.
   */
  private static void copy(final File target, final File source) throws IOException {
    if (!target.getParentFile().exists() && !target.getParentFile().mkdirs()) {
      throw new IOException(
          String.format(
              StorageEngineMessages.FAILED_TO_CREATE_DIR,
              target.getParentFile().getAbsolutePath()));
    }
    final File tempTarget =
        new File(target.getParentFile(), target.getName() + TEMP_FILE_SUFFIX + UUID.randomUUID());
    try {
      Files.copy(source.toPath(), tempTarget.toPath(), StandardCopyOption.REPLACE_EXISTING);
      try {
        Files.move(
            tempTarget.toPath(),
            target.toPath(),
            StandardCopyOption.REPLACE_EXISTING,
            StandardCopyOption.ATOMIC_MOVE);
      } catch (final AtomicMoveNotSupportedException e) {
        // The snapshot may live on a file system without atomic renames; the copy is complete at
        // this point, so a plain move publishes the same bytes.
        Files.move(tempTarget.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
    } catch (final IOException e) {
      Files.deleteIfExists(tempTarget.toPath());
      throw e;
    }
  }

  /**
   * Re-copies the first {@code length} bytes of a staged file of the snapshot over the copy that is
   * already published there, so that the copy covers the bytes its progress log describes.
   *
   * <p>Only the prefix is taken, and it is taken from the staged file of the region: the bytes of a
   * logged chunk are written before its entry is appended, so the prefix the log needs is on disk
   * whenever the log that names it was already copied.
   */
  private static void copyRange(final File target, final long length) throws IOException {
    final File tempTarget =
        new File(target.getParentFile(), target.getName() + TEMP_FILE_SUFFIX + UUID.randomUUID());
    try (final FileChannel source = FileChannel.open(target.toPath(), StandardOpenOption.READ);
        final FileChannel destination =
            FileChannel.open(
                tempTarget.toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
      long position = 0L;
      while (position < length) {
        final long copied = source.transferTo(position, length - position, destination);
        if (copied <= 0L) {
          // The staged file of the region is shorter than the log describes: there is nothing more
          // to copy, and the truncation below leaves the snapshot saying exactly that.
          break;
        }
        position += copied;
      }
    } catch (final IOException e) {
      Files.deleteIfExists(tempTarget.toPath());
      throw e;
    }
    try {
      Files.move(
          tempTarget.toPath(),
          target.toPath(),
          StandardCopyOption.REPLACE_EXISTING,
          StandardCopyOption.ATOMIC_MOVE);
    } catch (final AtomicMoveNotSupportedException e) {
      Files.move(tempTarget.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }
  }
}
