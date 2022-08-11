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
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import org.apache.ratis.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystemException;
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
  private SnapshotLogger snapshotLogger;

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

    boolean inSameFs = isSnapshotDirAndDataDirOnSameFs(snapshotDir);

    if (flushBeforeSnapshot) {
      dataRegion.syncCloseAllWorkingTsFileProcessors();
    }

    File snapshotLog = new File(snapshotDir, SnapshotLogger.SNAPSHOT_LOG_NAME);
    try {
      snapshotLogger = new SnapshotLogger(snapshotLog);
      boolean success = false;
      if (inSameFs) {
        success = createSnapshotInLocalFs(snapshotDir);
      } else {
        success = createSnapshotInAnotherFs(snapshotDir);
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
              + dataRegion.getStorageGroupName()
              + File.separator
              + dataRegion.getDataRegionId()
              + File.separator
              + timePartition
              + File.separator);
    }
    return resultDirs;
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

  private boolean createSnapshotInLocalFs(File snapshotDir) {
    seqBaseDir =
        new File(
            snapshotDir,
            "sequence"
                + File.separator
                + dataRegion.getStorageGroupName()
                + File.separator
                + dataRegion.getDataRegionId());
    unseqBaseDir =
        new File(
            snapshotDir,
            "unsequence"
                + File.separator
                + dataRegion.getStorageGroupName()
                + File.separator
                + dataRegion.getDataRegionId());

    List<Long> timePartitions = dataRegion.getTimePartitions();
    TsFileManager manager = dataRegion.getTsFileManager();
    manager.readLock();
    try {
      try {
        snapshotLogger.logSnapshotType(SnapshotLogger.SnapshotType.LOCAL_FS);
      } catch (IOException e) {
        LOGGER.error("Fail to create snapshot", e);
        cleanUpWhenFail(snapshotDir);
        return false;
      }
      for (Long timePartition : timePartitions) {
        List<String> seqDataDirs = getAllDataDirOfOnePartition(true, timePartition);

        try {
          for (String seqDataDir : seqDataDirs) {
            createFileSnapshotToTargetOne(
                new File(seqDataDir), new File(seqBaseDir, String.valueOf(timePartition)));
          }
        } catch (IOException e) {
          LOGGER.error("Fail to create snapshot", e);
          cleanUpWhenFail(snapshotDir);
          return false;
        }

        List<String> unseqDataDirs = getAllDataDirOfOnePartition(false, timePartition);
        try {
          for (String unseqDataDir : unseqDataDirs) {
            createFileSnapshotToTargetOne(
                new File(unseqDataDir), new File(seqBaseDir, String.valueOf(timePartition)));
          }
        } catch (IOException e) {
          LOGGER.error("Fail to create snapshot", e);
          cleanUpWhenFail(snapshotDir);
          return false;
        }
      }
    } finally {
      manager.readUnlock();
    }
    return true;
  }

  private boolean createSnapshotInAnotherFs(File snapshotDir) {
    try {
      snapshotLogger.logSnapshotType(SnapshotLogger.SnapshotType.REMOTE_FS);
    } catch (IOException e) {
      LOGGER.error("Fail to create snapshot", e);
      cleanUpWhenFail(snapshotDir);
      return false;
    }
    for (String dataDir : IoTDBDescriptor.getInstance().getConfig().getDataDirs()) {
      File seqDir =
          new File(
              dataDir,
              "sequence"
                  + File.separator
                  + dataRegion.getStorageGroupName()
                  + File.separator
                  + dataRegion.getDataRegionId());
      File unseqDir =
          new File(
              dataDir,
              "unsequence"
                  + File.separator
                  + dataRegion.getStorageGroupName()
                  + File.separator
                  + dataRegion.getDataRegionId());
      File localSeqSnapshotDir =
          new File(
              dataDir,
              "snapshot"
                  + File.separator
                  + "sequence"
                  + File.separator
                  + snapshotDir.getName()
                  + File.separator
                  + dataRegion.getStorageGroupName()
                  + File.separator
                  + dataRegion.getDataRegionId());
      File localUnseqSnapshotDir =
          new File(
              dataDir,
              "snapshot"
                  + File.separator
                  + "unsequence"
                  + File.separator
                  + snapshotDir.getName()
                  + File.separator
                  + dataRegion.getStorageGroupName()
                  + File.separator
                  + dataRegion.getDataRegionId());
      if (!localSeqSnapshotDir.mkdirs()) {
        LOGGER.warn("Failed to create local snapshot dir {}", localSeqSnapshotDir);
        return false;
      }
      List<Long> timePartitions = dataRegion.getTimePartitions();
      TsFileManager manager = dataRegion.getTsFileManager();
      manager.readLock();
      for (long timePartition : timePartitions) {
        createSnapshotForTimePartition(snapshotDir, seqDir, localSeqSnapshotDir, timePartition);
        createSnapshotForTimePartition(snapshotDir, unseqDir, localUnseqSnapshotDir, timePartition);
      }
    }

    return true;
  }

  private void createSnapshotForTimePartition(
      File snapshotDir, File dataDir, File localSeqSnapshotDir, long timePartition) {
    File seqTimePartitionDir = new File(dataDir, String.valueOf(timePartition));
    File seqTimePartitionSnapshotDir = new File(localSeqSnapshotDir, String.valueOf(timePartition));

    try {
      createFileSnapshotToTargetOne(seqTimePartitionDir, seqTimePartitionSnapshotDir);
    } catch (IOException e) {
      LOGGER.error(
          "Failed to create snapshot for {}-{}",
          dataRegion.getStorageGroupName(),
          dataRegion.getDataRegionId(),
          e);
      cleanUpWhenFail(snapshotDir);
    }
  }

  private void createFileSnapshotToTargetOne(File sourceDir, File targetDir) throws IOException {
    File[] files =
        sourceDir.listFiles(
            (dir, name) ->
                name.endsWith(TsFileConstant.TSFILE_SUFFIX)
                    || name.endsWith(TsFileResource.RESOURCE_SUFFIX)
                    || name.endsWith(ModificationFile.FILE_SUFFIX)
                    || name.endsWith(ModificationFile.COMPACTION_FILE_SUFFIX)
                    || name.endsWith(CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX)
                    || name.endsWith(CompactionLogger.CROSS_COMPACTION_LOG_NAME_SUFFIX)
                    || name.endsWith(IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX)
                    || name.endsWith(IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX));
    if (files == null) {
      return;
    }
    if (!targetDir.exists() && !targetDir.mkdirs()) {
      throw new IOException("Failed to create dir " + targetDir.getAbsolutePath());
    }
    for (File file : files) {
      File targetFile = new File(targetDir, file.getName());
      Files.createLink(targetFile.toPath(), file.toPath());
      snapshotLogger.logFile(file.getAbsolutePath(), targetFile.getAbsolutePath());
    }
  }

  /**
   * Test whether all data dirs is under the same fs with snapshot dir.
   *
   * @throws IOException
   */
  private boolean isSnapshotDirAndDataDirOnSameFs(File snapshotDir) throws IOException {
    String testFileName = "test.txt";
    File testFile = new File(snapshotDir, testFileName);
    if (!testFile.createNewFile() || !createTestFile(testFile)) {
      throw new IOException(
          "Failed to test whether the data dir and snapshot dir is on the same file system");
    }
    String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
    try {
      for (String dataDir : dataDirs) {
        File dirFile = new File(dataDir);
        File testSnapshotFile = new File(dirFile, testFileName);
        try {
          Files.createLink(testSnapshotFile.toPath(), testFile.toPath());
        } catch (FileSystemException e) {
          // if the fs rejects to create the link, it means they are in the different fs
          return false;
        }
        testSnapshotFile.delete();
      }
      return true;
    } finally {
      FileUtils.delete(testFile.toPath());
    }
  }

  private boolean createTestFile(File testFile) {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(testFile))) {
      writer.write("test");
      writer.flush();
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
