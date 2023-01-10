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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
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
    String snapshotId = snapshotDir.getName();
    return takeFullSnapshot(snapshotDirPath, snapshotId, snapshotId, flushBeforeSnapshot);
  }

  public boolean takeFullSnapshot(
      String snapshotDirPath,
      String tempSnapshotId,
      String finalSnapshotId,
      boolean flushBeforeSnapshot)
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

    File snapshotLog = new File(snapshotDir, SnapshotLogger.SNAPSHOT_LOG_NAME);
    try {
      snapshotLogger = new SnapshotLogger(snapshotLog);
      boolean success;
      snapshotLogger.logSnapshotId(finalSnapshotId);

      try {
        readLockTheFile();
        if (flushBeforeSnapshot) {
          try {
            dataRegion.writeLock("snapshotTaker");
            dataRegion.syncCloseAllWorkingTsFileProcessors();
          } finally {
            dataRegion.writeUnlock();
          }
        }
        success = createSnapshot(seqFiles, tempSnapshotId);
        success = success && createSnapshot(unseqFiles, tempSnapshotId);
      } finally {
        readUnlockTheFile();
      }

      if (!success) {
        LOGGER.warn(
            "Failed to take snapshot for {}-{}, clean up",
            dataRegion.getDatabaseName(),
            dataRegion.getDataRegionId());
        cleanUpWhenFail(finalSnapshotId);
      } else {
        snapshotLogger.logEnd();
        LOGGER.info(
            "Successfully take snapshot for {}-{}, snapshot directory is {}",
            dataRegion.getDatabaseName(),
            dataRegion.getDataRegionId(),
            snapshotDir.getParentFile().getAbsolutePath() + File.separator + finalSnapshotId);
      }

      return success;
    } catch (Exception e) {
      LOGGER.error(
          "Exception occurs when taking snapshot for {}-{}",
          dataRegion.getDatabaseName(),
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

  private boolean createSnapshot(List<TsFileResource> resources, String snapshotId) {
    try {
      for (TsFileResource resource : resources) {
        if (!resource.isClosed()) {
          continue;
        }
        File tsFile = resource.getTsFile();
        if (!resource.isClosed()) {
          continue;
        }
        File snapshotTsFile = getSnapshotFilePathForTsFile(tsFile, snapshotId);
        // create hard link for tsfile, resource, mods
        createHardLink(snapshotTsFile, tsFile);
        createHardLink(
            new File(snapshotTsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX),
            new File(tsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX));
        if (resource.getModFile().exists()) {
          createHardLink(
              new File(snapshotTsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX),
              new File(tsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX));
        }
      }
      return true;
    } catch (IOException e) {
      LOGGER.error("Catch IOException when creating snapshot", e);
      return false;
    }
  }

  private void createHardLink(File target, File source) throws IOException {
    if (!target.getParentFile().exists()) {
      LOGGER.error("Hard link target dir {} doesn't exist", target.getParentFile());
    }
    if (!source.exists()) {
      LOGGER.error("Hard link source file {} doesn't exist", source);
    }
    Files.deleteIfExists(target.toPath());
    Files.createLink(target.toPath(), source.toPath());
    snapshotLogger.logFile(source);
  }

  /**
   * Construct the snapshot file path for a given tsfile, and will create the dir. Eg, given a
   * tsfile in /data/iotdb/data/sequence/root.testsg/1/0/1-1-0-0.tsfile, with snapshotId "sm123",
   * the snapshot location will be /data/iotdb/data/snapshot/sm123/root.testsg/1/0/1-1-0-0.tsfile
   *
   * @param tsFile tsfile to be taken a snapshot
   * @param snapshotId the id for current snapshot
   * @return the File object of the snapshot file, and its parent directory will be created
   * @throws IOException
   */
  public File getSnapshotFilePathForTsFile(File tsFile, String snapshotId) throws IOException {
    // ... data (un)sequence sgName dataRegionId timePartition tsFileName
    String[] splittedPath =
        tsFile.getAbsolutePath().split(File.separator.equals("\\") ? "\\\\" : File.separator);
    // snapshot dir will be like
    // ... data snapshot snapshotId (un)sequence sgName dataRegionId timePartition
    StringBuilder stringBuilder = new StringBuilder();
    int i = 0;
    // build the prefix part of data dir
    for (; i < splittedPath.length - 5; ++i) {
      stringBuilder.append(splittedPath[i]);
      stringBuilder.append(File.separator);
    }
    stringBuilder.append(IoTDBConstant.SNAPSHOT_FOLDER_NAME);
    stringBuilder.append(File.separator);
    stringBuilder.append(dataRegion.getDatabaseName());
    stringBuilder.append(IoTDBConstant.FILE_NAME_SEPARATOR);
    stringBuilder.append(dataRegion.getDataRegionId());
    stringBuilder.append(File.separator);
    stringBuilder.append(snapshotId);
    stringBuilder.append(File.separator);
    // the content in here will be
    // ... data snapshot snapshotId

    // build the rest part for the dir
    for (; i < splittedPath.length - 1; ++i) {
      stringBuilder.append(splittedPath[i]);
      stringBuilder.append(File.separator);
    }
    File dir = new File(stringBuilder.toString());
    if (!dir.exists() && !dir.mkdirs()) {
      throw new IOException("Cannot create directory " + dir.getAbsolutePath());
    }
    return new File(dir, tsFile.getName());
  }

  private void cleanUpWhenFail(String snapshotId) {
    LOGGER.info("Cleaning up snapshot dir for {}", snapshotId);
    for (String dataDir : IoTDBDescriptor.getInstance().getConfig().getDataDirs()) {
      File dataDirForThisSnapshot =
          new File(
              dataDir
                  + File.separator
                  + IoTDBConstant.SNAPSHOT_FOLDER_NAME
                  + File.separator
                  + snapshotId);
      if (dataDirForThisSnapshot.exists()) {
        try {
          FileUtils.recursiveDeleteFolder(dataDirForThisSnapshot.getAbsolutePath());
        } catch (IOException e) {
          LOGGER.error(
              "Failed to delete folder {} when cleaning up",
              dataDirForThisSnapshot.getAbsolutePath());
        }
      }
    }
  }
}
