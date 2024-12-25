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

package org.apache.iotdb.db.storageengine.dataregion.snapshot;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.rescon.disk.FolderManager;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.DirectoryStrategyType;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SnapshotLoader {
  private Logger LOGGER = LoggerFactory.getLogger(SnapshotLoader.class);
  private String storageGroupName;
  private String snapshotPath;
  private String dataRegionId;
  private SnapshotLogAnalyzer logAnalyzer;

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
              + "databases"
              + File.separator
              + storageGroupName,
          dataRegionId,
          StorageEngine.getInstance().getFileFlushPolicy(),
          storageGroupName);
    } catch (Exception e) {
      LOGGER.error("Exception occurs while load snapshot from {}", snapshotPath, e);
      return null;
    }
  }

  private File getSnapshotLogFile() {
    File sourceDataDir = new File(snapshotPath);

    if (sourceDataDir.exists()) {
      File[] files =
          sourceDataDir.listFiles((dir, name) -> name.equals(SnapshotLogger.SNAPSHOT_LOG_NAME));
      if (files != null && files.length == 1) {
        LOGGER.info("Reading snapshot log file {}", files[0]);
        return files[0];
      }
    }
    return null;
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

    File snapshotLogFile = getSnapshotLogFile();

    if (snapshotLogFile == null) {
      return loadSnapshotWithoutLog();
    } else {
      return loadSnapshotWithLog(snapshotLogFile);
    }
  }

  private DataRegion loadSnapshotWithoutLog() {
    try {
      try {
        deleteAllFilesInDataDirs();
        LOGGER.info("Remove all data files in original data dir");
      } catch (IOException e) {
        LOGGER.error("Failed to remove origin data files", e);
        return null;
      }
      LOGGER.info("Moving snapshot file to data dirs");
      createLinksFromSnapshotDirToDataDirWithoutLog(new File(snapshotPath));
      return loadSnapshot();
    } catch (IOException | DiskSpaceInsufficientException e) {
      LOGGER.error(
          "Exception occurs when loading snapshot for {}-{}", storageGroupName, dataRegionId, e);
      return null;
    }
  }

  private DataRegion loadSnapshotWithLog(File logFile) {
    boolean snapshotComplete = false;
    try {
      logAnalyzer = new SnapshotLogAnalyzer(logFile);
      snapshotComplete = logAnalyzer.isSnapshotComplete();
    } catch (Exception e) {
      LOGGER.error("Exception occurs when reading snapshot file", e);
      return null;
    }

    if (!snapshotComplete) {
      // Do not load this snapshot because it's not complete.
      LOGGER.error("This snapshot is not complete, cannot load it");
      return null;
    }

    try {
      try {
        deleteAllFilesInDataDirs();
        LOGGER.info("Remove all data files in original data dir");
        createLinksFromSnapshotDirToDataDirWithLog();
        return loadSnapshot();
      } catch (IOException e) {
        LOGGER.error("Failed to remove origin data files", e);
        return null;
      }
    } finally {
      logAnalyzer.close();
    }
  }

  private void deleteAllFilesInDataDirs() throws IOException {
    String[] dataDirPaths = IoTDBDescriptor.getInstance().getConfig().getLocalDataDirs();

    // delete
    List<File> timePartitions = new ArrayList<>();
    for (String dataDirPath : dataDirPaths) {
      File seqDataDirForThisRegion =
          new File(
              dataDirPath
                  + File.separator
                  + IoTDBConstant.SEQUENCE_FOLDER_NAME
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
                  + IoTDBConstant.UNSEQUENCE_FOLDER_NAME
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

  private void createLinksFromSnapshotDirToDataDirWithoutLog(File sourceDir)
      throws IOException, DiskSpaceInsufficientException {
    File seqFileDir =
        new File(
            sourceDir,
            IoTDBConstant.SEQUENCE_FOLDER_NAME
                + File.separator
                + storageGroupName
                + File.separator
                + dataRegionId);
    File unseqFileDir =
        new File(
            sourceDir,
            IoTDBConstant.UNSEQUENCE_FOLDER_NAME
                + File.separator
                + storageGroupName
                + File.separator
                + dataRegionId);
    if (!seqFileDir.exists() && !unseqFileDir.exists()) {
      throw new IOException(
          String.format(
              "Cannot find %s or %s",
              seqFileDir.getAbsolutePath(), unseqFileDir.getAbsolutePath()));
    }
    FolderManager folderManager =
        new FolderManager(
            Arrays.asList(IoTDBDescriptor.getInstance().getConfig().getLocalDataDirs()),
            DirectoryStrategyType.SEQUENCE_STRATEGY);
    File[] timePartitionFolders = seqFileDir.listFiles();
    if (timePartitionFolders != null) {
      for (File timePartitionFolder : timePartitionFolders) {
        File[] files = timePartitionFolder.listFiles();
        if (files == null || files.length == 0) {
          continue;
        }
        String targetSuffix =
            IoTDBConstant.SEQUENCE_FOLDER_NAME
                + File.separator
                + storageGroupName
                + File.separator
                + dataRegionId
                + File.separator
                + timePartitionFolder.getName();
        createLinksFromSnapshotToSourceDir(targetSuffix, files, folderManager);
      }
    }

    timePartitionFolders = unseqFileDir.listFiles();
    if (timePartitionFolders != null) {
      for (File timePartitionFolder : timePartitionFolders) {
        File[] files = timePartitionFolder.listFiles();
        if (files == null || files.length == 0) {
          continue;
        }
        String targetSuffix =
            IoTDBConstant.UNSEQUENCE_FOLDER_NAME
                + File.separator
                + storageGroupName
                + File.separator
                + dataRegionId
                + File.separator
                + timePartitionFolder.getName();
        createLinksFromSnapshotToSourceDir(targetSuffix, files, folderManager);
      }
    }
  }

  private void createLinksFromSnapshotToSourceDir(
      String targetSuffix, File[] files, FolderManager folderManager)
      throws DiskSpaceInsufficientException, IOException {
    Map<String, String> fileTarget = new HashMap<>();
    for (File file : files) {
      String fileKey = file.getName().split("\\.")[0];
      String dataDir;
      if (fileTarget.containsKey(fileKey)) {
        dataDir = fileTarget.get(fileKey);
      } else {
        dataDir = folderManager.getNextFolder();
        fileTarget.put(fileKey, dataDir);
      }
      File targetFile =
          new File(dataDir + File.separator + targetSuffix + File.separator + file.getName());
      if (!targetFile.getParentFile().exists() && !targetFile.getParentFile().mkdirs()) {
        throw new IOException(
            String.format(
                "Cannot create directory %s", targetFile.getParentFile().getAbsolutePath()));
      }
      try {
        Files.createLink(targetFile.toPath(), file.toPath());
        continue;
      } catch (IOException e) {
        LOGGER.info("Cannot create link from {} to {}, try to copy it", file, targetFile);
      }

      Files.copy(file.toPath(), targetFile.toPath());
    }
  }

  private void createLinksFromSnapshotDirToDataDirWithLog() throws IOException {
    String snapshotId = logAnalyzer.getSnapshotId();
    int loggedFileNum = logAnalyzer.getTotalFileCountInSnapshot();
    Set<String> fileInfoSet = logAnalyzer.getFileInfoSet();
    String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getLocalDataDirs();
    int fileCnt = 0;
    for (String dataDir : dataDirs) {
      String snapshotDir =
          dataDir
              + File.separator
              + IoTDBConstant.SNAPSHOT_FOLDER_NAME
              + File.separator
              + storageGroupName
              + "-"
              + dataRegionId
              + File.separator
              + snapshotId;
      fileCnt += takeHardLinksFromSnapshotToDataDir(dataDir, new File(snapshotDir), fileInfoSet);
    }
    if (fileCnt != loggedFileNum) {
      throw new IOException(
          String.format(
              "The file num in log is %d, while file num in disk is %d", loggedFileNum, fileCnt));
    }
  }

  private int takeHardLinksFromSnapshotToDataDir(
      String dataDir, File snapshotFolder, Set<String> fileInfoSet) throws IOException {
    int cnt = 0;
    File sequenceTimePartitionFolders =
        new File(
            snapshotFolder.getAbsolutePath()
                + File.separator
                + IoTDBConstant.SEQUENCE_FOLDER_NAME
                + File.separator
                + storageGroupName
                + File.separator
                + dataRegionId);
    File[] timePartitionFolders = sequenceTimePartitionFolders.listFiles();
    if (timePartitionFolders != null) {
      for (File timePartitionFolder : timePartitionFolders) {
        String timePartition = timePartitionFolder.getName();
        File[] sourceFiles = timePartitionFolder.listFiles();
        if (sourceFiles == null) {
          continue;
        }
        File targetDir =
            new File(
                dataDir
                    + File.separator
                    + IoTDBConstant.SEQUENCE_FOLDER_NAME
                    + File.separator
                    + storageGroupName
                    + File.separator
                    + dataRegionId
                    + File.separator
                    + timePartition);
        createLinksFromSourceToTarget(targetDir, sourceFiles, fileInfoSet);
        cnt += sourceFiles.length;
      }
    }

    File unsequenceTimePartitionFolders =
        new File(
            snapshotFolder.getAbsolutePath()
                + File.separator
                + IoTDBConstant.UNSEQUENCE_FOLDER_NAME
                + File.separator
                + storageGroupName
                + File.separator
                + dataRegionId);
    timePartitionFolders = unsequenceTimePartitionFolders.listFiles();
    if (timePartitionFolders != null) {
      for (File timePartitionFolder : timePartitionFolders) {
        String timePartition = timePartitionFolder.getName();
        File[] sourceFiles = timePartitionFolder.listFiles();
        if (sourceFiles == null) {
          continue;
        }
        File targetDir =
            new File(
                dataDir
                    + File.separator
                    + IoTDBConstant.UNSEQUENCE_FOLDER_NAME
                    + File.separator
                    + storageGroupName
                    + File.separator
                    + dataRegionId
                    + File.separator
                    + timePartition);
        createLinksFromSourceToTarget(targetDir, sourceFiles, fileInfoSet);
        cnt += sourceFiles.length;
      }
    }

    return cnt;
  }

  private void createLinksFromSourceToTarget(File targetDir, File[] files, Set<String> fileInfoSet)
      throws IOException {
    for (File file : files) {
      String infoStr = getFileInfoString(file);
      if (!fileInfoSet.contains(infoStr)) {
        throw new IOException(
            String.format("File %s is not in the log file list", file.getAbsolutePath()));
      }
      File targetFile = new File(targetDir, file.getName());
      if (!targetFile.getParentFile().exists() && !targetFile.getParentFile().mkdirs()) {
        throw new IOException(
            String.format(
                "Cannot create directory %s", targetFile.getParentFile().getAbsolutePath()));
      }
      Files.createLink(targetFile.toPath(), file.toPath());
    }
  }

  private String getFileInfoString(File file) {
    String[] splittedStr = file.getAbsolutePath().split(File.separator.equals("\\") ? "\\\\" : "/");
    int length = splittedStr.length;
    return splittedStr[length - SnapshotLogger.FILE_NAME_OFFSET]
        + SnapshotLogger.SPLIT_CHAR
        + splittedStr[length - SnapshotLogger.TIME_PARTITION_OFFSET]
        + SnapshotLogger.SPLIT_CHAR
        + splittedStr[length - SnapshotLogger.SEQUENCE_OFFSET];
  }

  public List<File> getSnapshotFileInfo() throws IOException {
    File snapshotLogFile = getSnapshotLogFile();

    if (snapshotLogFile == null) {
      return searchDataFilesRecursively(snapshotPath);
    } else {
      return getSnapshotFileWithLog(snapshotLogFile);
    }
  }

  private List<File> getSnapshotFileWithLog(File logFile) throws IOException {
    SnapshotLogAnalyzer analyzer = new SnapshotLogAnalyzer(logFile);
    try {
      String snapshotId = analyzer.getSnapshotId();
      String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getLocalDataDirs();
      List<File> fileList = new LinkedList<>();
      for (String dataDir : dataDirs) {
        String snapshotDir =
            dataDir
                + File.separator
                + IoTDBConstant.SNAPSHOT_FOLDER_NAME
                + File.separator
                + storageGroupName
                + "-"
                + dataRegionId
                + File.separator
                + snapshotId;
        fileList.addAll(searchDataFilesRecursively(snapshotDir));
      }
      return fileList;
    } finally {
      analyzer.close();
    }
  }

  /**
   * Search all data files in one directory recursively.
   *
   * @return
   */
  private List<File> searchDataFilesRecursively(String dir) throws IOException {
    LinkedList<File> fileList = new LinkedList<>();
    Files.walkFileTree(
        new File(dir).toPath(),
        new FileVisitor<Path>() {
          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
              throws IOException {
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            if (SnapshotFileSet.isDataFile(file.toFile())) {
              fileList.add(file.toFile());
            }
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
}
