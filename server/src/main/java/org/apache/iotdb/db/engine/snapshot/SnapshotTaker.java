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
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.snapshot.exception.DirectoryNotLegalException;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.List;

/**
 * SnapshotTaker takes data snapshot for a DataRegion in one time. It does so by creating hard link
 * for files or copying them. SnapshotTaker supports two different ways of snapshot: Full Snapshot
 * and Incremental Snapshot. The former takes a snapshot for all files in an empty directory, and
 * the latter takes a snapshot based on the snapshot that took before.
 */
public class SnapshotTaker {
  private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotTaker.class);
  private final DataRegion dataRegion;

  public SnapshotTaker(DataRegion dataRegion) {
    this.dataRegion = dataRegion;
  }

  public boolean takeFullSnapshot(String snapshotDirPath, boolean flushBeforeSnapshot)
      throws DirectoryNotLegalException {
    File seqSnapshotDir =
        new File(
            snapshotDirPath
                + File.separator
                + IoTDBConstant.SEQUENCE_FLODER_NAME
                + File.separator
                + dataRegion.getLogicalStorageGroupName()
                + File.separator
                + dataRegion.getDataRegionId());
    File unseqSnapshotDir =
        new File(
            snapshotDirPath
                + File.separator
                + IoTDBConstant.UNSEQUENCE_FLODER_NAME
                + File.separator
                + dataRegion.getLogicalStorageGroupName()
                + File.separator
                + dataRegion.getDataRegionId());
    if ((seqSnapshotDir.exists() && seqSnapshotDir.listFiles() != null)
        || (unseqSnapshotDir.exists() && unseqSnapshotDir.listFiles() != null)) {
      // the directory should be empty or not exists
      throw new DirectoryNotLegalException(
          String.format("%s already exists and is not empty", snapshotDirPath));
    }

    if (flushBeforeSnapshot) {
      dataRegion.syncCloseAllWorkingTsFileProcessors();
    }

    List<Long> timePartitions = dataRegion.getTimePartitions();
    for (Long timePartition : timePartitions) {
      List<String> seqDataDirs = getAllDataDirOfOnePartition(true, timePartition);
      File seqTargetDir =
          new File(
              snapshotDirPath
                  + File.separator
                  + IoTDBConstant.SEQUENCE_FLODER_NAME
                  + File.separator
                  + dataRegion.getLogicalStorageGroupName()
                  + File.separator
                  + dataRegion.getDataRegionId()
                  + File.separator
                  + timePartition);
      if (!seqTargetDir.mkdirs()) {
        LOGGER.error("Failed to create target directory {}", seqTargetDir);
        return false;
      }

      try {
        createFileSnapshot(seqDataDirs, seqTargetDir);
      } catch (IOException e) {
        LOGGER.error("Fail to create snapshot", e);
        return false;
      }

      List<String> unseqDataDirs = getAllDataDirOfOnePartition(false, timePartition);
      File unseqTargetDir =
          new File(
              snapshotDirPath
                  + File.separator
                  + IoTDBConstant.UNSEQUENCE_FLODER_NAME
                  + File.separator
                  + dataRegion.getLogicalStorageGroupName()
                  + File.separator
                  + dataRegion.getDataRegionId()
                  + File.separator
                  + timePartition);
      if (!unseqTargetDir.mkdirs()) {
        LOGGER.error("Failed to create target directory {}", seqTargetDir);
        return false;
      }

      try {
        createFileSnapshot(unseqDataDirs, unseqTargetDir);
      } catch (IOException e) {
        LOGGER.error("Fail to create snapshot", e);
        return false;
      }
    }

    return true;
  }

  public boolean takeIncrementalSnapshot(long maxWalSizeBeforeSnapshot, String snapshotDirPath) {
    return false;
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

  private void createFileSnapshot(List<String> sourceDirPaths, File targetDir) throws IOException {
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
                      || name.endsWith(ModificationFile.COMPACTION_FILE_SUFFIX));
      if (files == null || files.length == 0) {
        continue;
      }

      for (File file : files) {
        File linkFile = new File(targetDir, file.getName());
        Files.createLink(linkFile.toPath(), file.toPath());
      }
    }
  }
}
