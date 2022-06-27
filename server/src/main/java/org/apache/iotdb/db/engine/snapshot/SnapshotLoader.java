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
import java.util.Comparator;
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
    LOGGER.info(
        "Loading snapshot for {}-{}, source directory is {}",
        storageGroupName,
        dataRegionId,
        snapshotPath);
    try {
      deleteAllFilesInDataDirs();
      LOGGER.info("Remove all data files in original data dir");
    } catch (IOException e) {
      return null;
    }

    // move the snapshot data to data dir
    File sourceDataDir = new File(snapshotPath);
    if (sourceDataDir.exists()) {
      try {
        createLinksFromSnapshotDirToDataDir(sourceDataDir);
        LOGGER.info("Move data files from snapshot to data directory");
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

  private void createLinksFromSnapshotDirToDataDir(File sourceDir)
      throws IOException, DiskSpaceInsufficientException {
    File seqFileDir = new File(sourceDir, "sequence" + File.separator + storageGroupName);
    File unseqFileDir = new File(sourceDir, "unsequence" + File.separator + storageGroupName);
    if (!seqFileDir.exists() && !unseqFileDir.exists()) {
      throw new IOException(
          String.format(
              "Cannot find %s or %s",
              seqFileDir.getAbsolutePath(), unseqFileDir.getAbsolutePath()));
    }

    File[] seqRegionDirs = seqFileDir.listFiles();
    if (seqRegionDirs != null && seqRegionDirs.length > 0) {
      for (File seqRegionDir : seqRegionDirs) {
        if (!seqRegionDir.isDirectory()) {
          LOGGER.info("Skip {}, because it is not a directory", seqRegionDir);
          continue;
        }
        File[] seqPartitionDirs = seqRegionDir.listFiles();
        if (seqPartitionDirs != null && seqPartitionDirs.length > 0) {
          for (File seqPartitionDir : seqPartitionDirs) {
            String[] splitPath =
                seqPartitionDir
                    .getAbsolutePath()
                    .split(File.separator.equals("\\") ? "\\\\" : File.separator);
            long timePartition = Long.parseLong(splitPath[splitPath.length - 1]);
            File[] files = seqPartitionDir.listFiles();
            if (files != null && files.length > 0) {
              Arrays.sort(files, Comparator.comparing(File::getName));
              String currDir = DirectoryManager.getInstance().getNextFolderForSequenceFile();
              for (File file : files) {
                if (file.getName().endsWith(".tsfile")) {
                  currDir = DirectoryManager.getInstance().getNextFolderForSequenceFile();
                }
                File targetFile =
                    new File(
                        currDir,
                        storageGroupName
                            + File.separator
                            + dataRegionId
                            + File.separator
                            + timePartition
                            + File.separator
                            + file.getName());
                if (!targetFile.getParentFile().exists() && !targetFile.getParentFile().mkdirs()) {
                  throw new IOException(
                      String.format("Failed to create dir %s", targetFile.getParent()));
                }
                Files.createLink(targetFile.toPath(), file.toPath());
              }
            }
          }
        }
      }
    }

    File[] unseqRegionDirs = unseqFileDir.listFiles();
    if (unseqRegionDirs != null && unseqRegionDirs.length > 0) {
      for (File unseqRegionDir : unseqRegionDirs) {
        if (!unseqRegionDir.isDirectory()) {
          LOGGER.info("Skip {}, because it is not a directory", unseqRegionDir);
          continue;
        }
        File[] unseqPartitionDirs = unseqRegionDir.listFiles();
        if (unseqPartitionDirs != null && unseqPartitionDirs.length > 0) {
          for (File unseqPartitionDir : unseqPartitionDirs) {
            String[] splitPath =
                unseqPartitionDir
                    .getAbsolutePath()
                    .split(File.separator.equals("\\") ? "\\\\" : File.separator);
            long timePartition = Long.parseLong(splitPath[splitPath.length - 1]);
            File[] files = unseqPartitionDir.listFiles();
            if (files != null && files.length > 0) {
              Arrays.sort(files, Comparator.comparing(File::getName));
              String currDir = DirectoryManager.getInstance().getNextFolderForUnSequenceFile();
              for (File file : files) {
                if (file.getName().endsWith(".tsfile")) {
                  currDir = DirectoryManager.getInstance().getNextFolderForUnSequenceFile();
                }
                File targetFile =
                    new File(
                        currDir,
                        storageGroupName
                            + File.separator
                            + dataRegionId
                            + File.separator
                            + timePartition
                            + File.separator
                            + file.getName());
                if (!targetFile.getParentFile().exists() && !targetFile.getParentFile().mkdirs()) {
                  throw new IOException(
                      String.format("Failed to create dir %s", targetFile.getParent()));
                }
                Files.createLink(targetFile.toPath(), file.toPath());
              }
            }
          }
        }
      }
    }
  }
}
