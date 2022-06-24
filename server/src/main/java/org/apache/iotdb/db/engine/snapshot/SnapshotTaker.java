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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.log.CompactionLogger;
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
import java.util.LinkedList;
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
  public static String SNAPSHOT_FILE_INFO_SEP_STR = "_";
  private File seqBaseDir;
  private File unseqBaseDir;

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
    seqBaseDir =
        new File(
            snapshotDir,
            "sequence"
                + File.separator
                + dataRegion.getLogicalStorageGroupName()
                + File.separator
                + dataRegion.getDataRegionId());
    unseqBaseDir =
        new File(
            snapshotDir,
            "unsequence"
                + File.separator
                + dataRegion.getLogicalStorageGroupName()
                + File.separator
                + dataRegion.getDataRegionId());

    if (!snapshotDir.exists() && !snapshotDir.mkdirs()) {
      throw new IOException(String.format("Failed to create directory %s", snapshotDir));
    }

    if (flushBeforeSnapshot) {
      dataRegion.syncCloseAllWorkingTsFileProcessors();
    }

    List<Long> timePartitions = dataRegion.getTimePartitions();
    TsFileManager manager = dataRegion.getTsFileManager();
    manager.readLock();
    try {
      for (Long timePartition : timePartitions) {
        List<String> seqDataDirs = getAllDataDirOfOnePartition(true, timePartition);

        try {
          createFileSnapshot(seqDataDirs, true, timePartition);
        } catch (IOException e) {
          LOGGER.error("Fail to create snapshot", e);
          cleanUpWhenFail(snapshotDir);
          return false;
        }

        List<String> unseqDataDirs = getAllDataDirOfOnePartition(false, timePartition);

        try {
          createFileSnapshot(unseqDataDirs, false, timePartition);
        } catch (IOException e) {
          LOGGER.error("Fail to create snapshot", e);
          cleanUpWhenFail(snapshotDir);
          return false;
        }
      }
    } finally {
      manager.readUnlock();
    }

    LOGGER.info(
        "Successfully take snapshot for {}-{}, snapshot directory is {}",
        dataRegion.getLogicalStorageGroupName(),
        dataRegion.getDataRegionId(),
        snapshotDirPath);

    return true;
  }

  private List<String> getAllDataDirOfOnePartition(boolean sequence, long timePartition) {
    String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
    List<String> resultDirs = new LinkedList<>();

    for (String dataDir : dataDirs) {
      resultDirs.add(
          dataDir
              + File.separator
              + (sequence
                  ? IoTDBConstant.SEQUENCE_FLODER_NAME
                  : IoTDBConstant.UNSEQUENCE_FLODER_NAME)
              + File.separator
              + dataRegion.getLogicalStorageGroupName()
              + File.separator
              + dataRegion.getDataRegionId()
              + File.separator
              + timePartition
              + File.separator);
    }
    return resultDirs;
  }

  private void createFileSnapshot(List<String> sourceDirPaths, boolean sequence, long timePartition)
      throws IOException {
    File timePartitionDir =
        new File(sequence ? seqBaseDir : unseqBaseDir, String.valueOf(timePartition));
    if (!timePartitionDir.exists() && !timePartitionDir.mkdirs()) {
      throw new IOException(
          String.format("%s not exists and cannot create it", timePartitionDir.getAbsolutePath()));
    }

    for (String sourceDirPath : sourceDirPaths) {
      File sourceDir = new File(sourceDirPath);
      if (!sourceDir.exists()) {
        continue;
      }
      // Collect TsFile, TsFileResource, Mods, CompactionMods
      File[] files =
          sourceDir.listFiles(
              (dir, name) ->
                  name.endsWith(".tsfile")
                      || name.endsWith(TsFileResource.RESOURCE_SUFFIX)
                      || name.endsWith(ModificationFile.FILE_SUFFIX)
                      || name.endsWith(ModificationFile.COMPACTION_FILE_SUFFIX)
                      || name.endsWith(CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX)
                      || name.endsWith(CompactionLogger.CROSS_COMPACTION_LOG_NAME_SUFFIX)
                      || name.endsWith(IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX)
                      || name.endsWith(IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX));
      if (files == null || files.length == 0) {
        continue;
      }

      for (File file : files) {
        File linkFile = new File(timePartitionDir, file.getName());
        Files.createLink(linkFile.toPath(), file.toPath());
      }
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
  }
}
