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
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;

import org.apache.tsfile.external.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

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

  private LoadTsFileSnapshot() {}

  /**
   * Copies the staged files of the in-progress LOAD tasks of {@code dataRegion} into {@code
   * snapshotDir/load/}. Returns {@code false} on an IO error so the caller can fail and clean up
   * the whole snapshot, mirroring the other snapshot steps.
   */
  public static boolean snapshot(final DataRegion dataRegion, final File snapshotDir) {
    final LoadTsFileManager manager = dataRegion.getLoadTsFileManagerIfPresent().orElse(null);
    if (manager == null) {
      return true;
    }
    final List<File> taskDirs = manager.getActiveTaskDirs();
    if (taskDirs.isEmpty()) {
      return true;
    }
    final File loadSnapshotDir = new File(snapshotDir, SNAPSHOT_SUBDIR_NAME);
    int taskCount = 0;
    int fileCount = 0;
    try {
      for (final File taskDir : taskDirs) {
        final File[] files = taskDir.listFiles();
        if (files == null) {
          continue;
        }
        final File targetDir = new File(loadSnapshotDir, taskDir.getName());
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
                StorageEngineMessages.LOG_LOAD_CONSENSUS_SNAPSHOT_TAKEN_09A7DD4C,
                taskCount,
                fileCount,
                dataRegion.getDatabaseName()
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + dataRegion.getDataRegionIdString(),
                snapshotDir.getAbsolutePath()));
      }
      return true;
    } catch (final IOException e) {
      LOGGER.warn(StorageEngineMessages.CATCH_IO_EXCEPTION_CREATING_SNAPSHOT, e);
      return false;
    }
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
    final File regionLoadDir =
        LoadStagingDirs.regionLoadDir(new File(loadBaseDirs[0]), databaseName, dataRegionIdString);
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
      final File targetDir = new File(regionLoadDir, loadIdDir.getName());
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
            fileList.add(file.toFile());
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            return FileVisitResult.CONTINUE;
          }
        });
    return fileList;
  }

  private static void copy(final File target, final File source) throws IOException {
    if (!target.getParentFile().exists() && !target.getParentFile().mkdirs()) {
      throw new IOException(
          String.format(
              StorageEngineMessages.FAILED_TO_CREATE_DIR,
              target.getParentFile().getAbsolutePath()));
    }
    Files.copy(source.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
  }
}
