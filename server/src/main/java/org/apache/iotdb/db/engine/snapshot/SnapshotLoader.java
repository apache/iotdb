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
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SnapshotLoader {
  private Logger LOGGER = LoggerFactory.getLogger(SnapshotLoader.class);
  private String storageGroupName;
  private String snapshotPath;
  private String dataRegionId;

  public SnapshotLoader(String snapshotPath, String storageGroupName, String dataRegionId) {
    this.snapshotPath = snapshotPath;
    this.storageGroupName = storageGroupName;
    this.dataRegionId = dataRegionId;
  }

  private DataRegion loadSnapshot() {
    try {
      return new DataRegion(
          IoTDBDescriptor.getInstance().getConfig().getSystemDir()
              + File.separator
              + "storage_groups"
              + File.separator
              + storageGroupName,
          dataRegionId,
          StorageEngineV2.getInstance().getFileFlushPolicy(),
          storageGroupName);
    } catch (Exception e) {
      LOGGER.error("Exception occurs while load snapshot from {}", snapshotPath, e);
      return null;
    }
  }

  /**
   * 1. Clear origin data 2. Move snapshot data to data dir 3. Load data region
   *
   * @return
   */
  public DataRegion loadSnapshotForStateMachine() {
    try {
      deleteAllFilesInDataDirs();
    } catch (IOException e) {
      return null;
    }

    // move the snapshot data to data dir
    String seqBaseDir =
        IoTDBConstant.SEQUENCE_FLODER_NAME
            + File.separator
            + storageGroupName
            + File.separator
            + dataRegionId;
    String unseqBaseDir =
        IoTDBConstant.UNSEQUENCE_FLODER_NAME
            + File.separator
            + storageGroupName
            + File.separator
            + dataRegionId;
    File sourceDataDir = new File(snapshotPath);
    if (sourceDataDir.exists()) {
      try {
        createLinksFromSnapshotDirToDataDir(sourceDataDir, seqBaseDir, unseqBaseDir);
      } catch (IOException | DiskSpaceInsufficientException e) {
        LOGGER.error(
            "Exception occurs when creating links from snapshot directory to data directory", e);
        return null;
      }
    }

    return loadSnapshot();
  }

  private void deleteAllFilesInDataDirs() throws IOException {
    String[] dataDirPaths = IoTDBDescriptor.getInstance().getConfig().getDataDirs();

    // delete
    List<File> timePartitions = new ArrayList<>();
    for (String dataDirPath : dataDirPaths) {
      File seqDataDirForThisRegion =
          new File(
              dataDirPath
                  + File.separator
                  + IoTDBConstant.SEQUENCE_FLODER_NAME
                  + File.separator
                  + storageGroupName
                  + File.separator
                  + dataRegionId);
      if (seqDataDirForThisRegion.exists()) {
        File[] files = seqDataDirForThisRegion.listFiles();
        if (files != null) {
          timePartitions.addAll(Arrays.asList(files));
        }
      }

      File unseqDataDirForThisRegion =
          new File(
              dataDirPath
                  + File.separator
                  + IoTDBConstant.UNSEQUENCE_FLODER_NAME
                  + File.separator
                  + storageGroupName
                  + File.separator
                  + dataRegionId);

      if (unseqDataDirForThisRegion.exists()) {
        File[] files = unseqDataDirForThisRegion.listFiles();
        if (files != null) {
          timePartitions.addAll(Arrays.asList(files));
        }
      }
    }

    try {
      for (File timePartition : timePartitions) {
        FileUtils.forceDelete(timePartition);
      }
    } catch (IOException e) {
      LOGGER.error(
          "Exception occurs when deleting time partition directory for {}-{}",
          storageGroupName,
          dataRegionId,
          e);
      throw e;
    }
  }

  private void createLinksFromSnapshotDirToDataDir(
      File sourceDir, String seqBaseDir, String unseqBaseDir)
      throws IOException, DiskSpaceInsufficientException {
    File[] files = sourceDir.listFiles();
    if (files == null) {
      return;
    }
    for (File sourceFile : files) {
      String[] fileInfo = sourceFile.getName().split(SnapshotTaker.SNAPSHOT_FILE_INFO_SEP_STR);
      if (fileInfo.length != 5) {
        continue;
      }
      boolean seq = fileInfo[0].equals("seq");
      String timePartition = fileInfo[3];
      String fileName = fileInfo[4];
      String nextDataDir =
          seq
              ? DirectoryManager.getInstance().getNextFolderForSequenceFile()
              : DirectoryManager.getInstance().getNextFolderForUnSequenceFile();
      File baseDir = new File(nextDataDir, seq ? seqBaseDir : unseqBaseDir);
      File targetDirForThisTimePartition = new File(baseDir, timePartition);
      if (!targetDirForThisTimePartition.exists() && !targetDirForThisTimePartition.mkdirs()) {
        throw new IOException(
            String.format("Failed to make directory %s", targetDirForThisTimePartition));
      }

      File targetFile = new File(targetDirForThisTimePartition, fileName);
      try {
        Files.createLink(targetFile.toPath(), sourceFile.toPath());
      } catch (IOException e) {
        throw new IOException(
            String.format("Failed to create hard link from %s to %s", sourceFile, targetFile), e);
      }
    }
  }
}
