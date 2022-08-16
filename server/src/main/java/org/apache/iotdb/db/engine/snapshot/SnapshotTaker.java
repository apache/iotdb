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
package org.apache.iotdb.db.engine.snapshot;

import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.snapshot.exception.DirectoryNotLegalException;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Objects;

/**
 * SnapshotTaker takes data snapshot for a DataRegion in one time. It does so by creating hard link
 * for files or copying them. SnapshotTaker supports two different ways of snapshot: Full Snapshot
 * and Incremental Snapshot. The former takes a snapshot for all files in an empty directory, and
 * the latter takes a snapshot based on the snapshot that took before.
 */
public class SnapshotTaker {
  private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotTaker.class);
  private final DataRegion dataRegion;
  private SnapshotLogger snapshotLogger;
  private List<TsFileResource> seqFiles;
  private List<TsFileResource> unseqFiles;

  public SnapshotTaker(DataRegion dataRegion) {
    this.dataRegion = dataRegion;
  }

  public boolean takeFullSnapshot(String snapshotDirPath, boolean flushBeforeSnapshot)
      throws DirectoryNotLegalException, IOException {
    File snapshotDir = new File(snapshotDirPath);
    if (snapshotDir.exists()
        && snapshotDir.listFiles() != null
        && Objects.requireNonNull(snapshotDir.listFiles()).length > 0) {
      // the directory should be empty or not exists
      throw new DirectoryNotLegalException(
          String.format("%s already exists and is not empty", snapshotDirPath));
    }

    if (!snapshotDir.exists() && !snapshotDir.mkdirs()) {
      throw new IOException(String.format("Failed to create directory %s", snapshotDir));
    }

    if (flushBeforeSnapshot) {
      dataRegion.syncCloseAllWorkingTsFileProcessors();
    }

    File snapshotLog = new File(snapshotDir, SnapshotLogger.SNAPSHOT_LOG_NAME);
    try {
      snapshotLogger = new SnapshotLogger(snapshotLog);
      boolean success = false;

      readLockTheFile();
      try {
        success = createSnapshot(true, seqFiles, snapshotDir.getName()) || success;
        success = createSnapshot(false, unseqFiles, snapshotDir.getName()) || success;
      } finally {
        readUnlockTheFile();
      }

      LOGGER.info(
          "Successfully take snapshot for {}-{}, snapshot directory is {}",
          dataRegion.getStorageGroupName(),
          dataRegion.getDataRegionId(),
          snapshotDirPath);

      return success;
    } catch (Exception e) {
      LOGGER.error(
          "Exception occurs when taking snapshot for {}-{}",
          dataRegion.getStorageGroupName(),
          dataRegion.getDataRegionId(),
          e);
      return false;
    } finally {
      try {
        snapshotLogger.close();
      } catch (Exception e) {
        LOGGER.error("Failed to close snapshot logger", e);
      }
    }
  }

  private void readLockTheFile() {
    TsFileManager manager = dataRegion.getTsFileManager();
    manager.readLock();
    try {
      seqFiles = manager.getTsFileList(true);
      unseqFiles = manager.getTsFileList(false);
      for (TsFileResource resource : seqFiles) {
        resource.readLock();
      }
      for (TsFileResource resource : unseqFiles) {
        resource.readLock();
      }
    } finally {
      manager.readUnlock();
    }
  }

  private void readUnlockTheFile() {
    for (TsFileResource resource : seqFiles) {
      resource.readUnlock();
    }
    for (TsFileResource resource : unseqFiles) {
      resource.readUnlock();
    }
  }

  private void cleanUpWhenFail(File snapshotDir) {
    File[] files = snapshotDir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (!file.delete()) {
          LOGGER.error("Failed to delete link file {} after failing to create snapshot", file);
        }
      }
    }
    try {
      snapshotLogger.cleanUpWhenFailed();
    } catch (IOException e) {
      LOGGER.error("Failed to clean up log file", e);
    }
  }

  private boolean createSnapshot(boolean seq, List<TsFileResource> resources, String snapshotId)
      throws IOException {
    for (TsFileResource resource : resources) {
      File tsFile = resource.getTsFile();
      File dir = getSnapshotDirDirForResource(tsFile, snapshotId);
      // create hard link for tsfile, resource, mods, compaction mods
      createHardLink(new File(dir, tsFile.getName()), tsFile);
      createHardLink(
          new File(dir, tsFile.getName() + TsFileResource.RESOURCE_SUFFIX),
          new File(tsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX));
      if (resource.getModFile().exists()) {
        createHardLink(
            new File(dir, tsFile.getName() + ModificationFile.FILE_SUFFIX),
            new File(tsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX));
      }
      if (resource.getCompactionModFile().exists()) {
        createHardLink(
            new File(dir, tsFile.getName() + ModificationFile.COMPACTION_FILE_SUFFIX),
            new File(tsFile.getAbsolutePath() + ModificationFile.COMPACTION_FILE_SUFFIX));
      }
    }
    return true;
  }

  private void createHardLink(File target, File source) throws IOException {
    Files.createLink(target.toPath(), source.toPath());
    snapshotLogger.logFile(source.getAbsolutePath(), target.getAbsolutePath());
  }

  private File getSnapshotDirDirForResource(File tsFile, String snapshotId) throws IOException {
    // ... data (un)sequence sgName dataRegionId timePartition tsFileName
    String[] splittedPath =
        tsFile.getAbsolutePath().split(File.separator.equals("\\") ? "\\\\" : File.separator);
    StringBuilder stringBuilder = new StringBuilder();
    int i = 0;
    for (; i < splittedPath.length - 5; ++i) {
      stringBuilder.append(splittedPath[i]);
      stringBuilder.append(File.separator);
    }
    stringBuilder.append("snapshot");
    stringBuilder.append(File.separator);
    stringBuilder.append(snapshotId);
    stringBuilder.append(File.separator);
    for (; i < splittedPath.length - 1; ++i) {
      stringBuilder.append(splittedPath[i]);
      stringBuilder.append(File.separator);
    }
    File dir = new File(stringBuilder.toString());
    if (!dir.exists() && !dir.mkdirs()) {
      throw new IOException("Cannot create directory " + dir.getAbsolutePath());
    }
    return dir;
  }
}
