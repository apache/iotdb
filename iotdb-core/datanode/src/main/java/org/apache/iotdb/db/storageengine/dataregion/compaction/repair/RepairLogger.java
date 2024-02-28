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

package org.apache.iotdb.db.storageengine.dataregion.compaction.repair;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileRepairStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

public class RepairLogger implements Closeable {

  static final String repairTaskStartTimeLogPrefix = "TASK_START_TIME";
  static final String repairTimePartitionStartLogPrefix = "START_TIME_PARTITION";
  static final String cannotRepairFileLogPrefix = "TSFILE";
  static final String repairTimePartitionEndLogPrefix = "END_TIME_PARTITION";
  public static final String repairProgressFileName = "repair-data.progress";
  public static final String repairProgressStoppedFileName = "repair-data.stopped";
  public static final String repairLogDir = "repair";
  private File logFile;
  private final File logFileDir;
  private FileOutputStream logStream;
  private boolean hasPreviousTask = false;
  private boolean isPreviousTaskStopped = false;
  private boolean needRecoverFromLogFile = false;

  public RepairLogger(File logFileDir, boolean recover) throws IOException {
    this.logFileDir = logFileDir;
    checkPreviousRepairTaskStatus();
    if (recover) {
      recoverPreviousTask();
    } else {
      startRepairTask();
    }
  }

  private void checkPreviousRepairTaskStatus() {
    File[] files = logFileDir.listFiles();
    if (files == null) {
      return;
    }
    for (File file : files) {
      String fileName = file.getName();
      if (repairProgressFileName.equals(fileName)) {
        hasPreviousTask = true;
        this.logFile = file;
      } else if (repairProgressStoppedFileName.equals(fileName)) {
        hasPreviousTask = true;
        isPreviousTaskStopped = true;
        this.logFile = file;
      }
    }
  }

  /**
   * Used for start a repair task. May restart a stopped repair task or create a new repair task.
   */
  private void startRepairTask() throws IOException {
    if (hasPreviousTask && isPreviousTaskStopped) {
      // Restart the stopped task
      // 1. rename file to unmark stopped
      unmarkStopped();
      // 2. recover from previous log file
      recoverPreviousTask();
      return;
    }
    // Start a new repair task
    deletePreviousLogFileIfExists();
    createNewLogFile();
  }

  private void unmarkStopped() throws IOException {
    File progressFile =
        new File(logFileDir.getPath() + File.separator + RepairLogger.repairProgressFileName);
    File stoppedFile =
        new File(
            logFileDir.getPath() + File.separator + RepairLogger.repairProgressStoppedFileName);
    if (stoppedFile.exists()) {
      Files.move(stoppedFile.toPath(), progressFile.toPath());
      logFile = progressFile;
    }
    this.isPreviousTaskStopped = false;
  }

  private void deletePreviousLogFileIfExists() throws IOException {
    File[] files = logFileDir.listFiles();
    if (files == null) {
      return;
    }
    for (File file : files) {
      if (repairProgressFileName.equals(file.getName())) {
        Files.deleteIfExists(file.toPath());
        return;
      }
    }
  }

  private void createNewLogFile() throws IOException {
    this.logFile = new File(logFileDir.getPath() + File.separator + repairProgressFileName);
    Path logFilePath = logFile.toPath();
    if (!Files.exists(logFilePath)) {
      Files.createFile(logFilePath);
    }
    this.logStream = new FileOutputStream(logFile, true);
  }

  /** Used for recovering StorageEngine or recovering stopped repair task */
  private void recoverPreviousTask() throws FileNotFoundException {
    if (!hasPreviousTask) {
      return;
    }
    this.needRecoverFromLogFile = true;
    if (!isPreviousTaskStopped) {
      this.logStream = new FileOutputStream(logFile, true);
    }
  }

  public boolean isNeedRecoverFromLogFile() {
    return needRecoverFromLogFile;
  }

  public boolean isPreviousTaskStopped() {
    return isPreviousTaskStopped;
  }

  public File getLogFile() {
    return logFile;
  }

  public void recordRepairTaskStartTimeIfLogFileEmpty(long repairTaskStartTime) throws IOException {
    if (logFile.length() != 0) {
      return;
    }
    String repairTaskStartTimeLog =
        String.format("%s %s\n", repairTaskStartTimeLogPrefix, repairTaskStartTime);
    logStream.write(repairTaskStartTimeLog.getBytes());
  }

  public void recordRepairedTimePartition(RepairTimePartition timePartition) throws IOException {
    markStartOfRepairedTimePartition(timePartition);
    recordCannotRepairFiles(timePartition);
    markEndOfRepairedTimePartition(timePartition);
  }

  public void recordCannotRepairFiles(RepairTimePartition timePartition) throws IOException {
    List<TsFileResource> resources = timePartition.getAllFileSnapshot();
    List<TsFileResource> cannotRepairFiles =
        resources.stream()
            .filter(
                resource -> resource.getTsFileRepairStatus() == TsFileRepairStatus.CAN_NOT_REPAIR)
            .collect(Collectors.toList());
    for (TsFileResource cannotRepairFile : cannotRepairFiles) {
      recordOneFile(cannotRepairFile);
    }
  }

  public void markStartOfRepairedTimePartition(RepairTimePartition timePartition)
      throws IOException {
    String startTimePartitionLog =
        String.format(
            "%s %s %s %s\n",
            repairTimePartitionStartLogPrefix,
            timePartition.getDatabaseName(),
            timePartition.getDataRegionId(),
            timePartition.getTimePartitionId());
    logStream.write(startTimePartitionLog.getBytes());
  }

  public void markEndOfRepairedTimePartition(RepairTimePartition timePartition) throws IOException {
    String endTimePartitionLog = String.format("%s\n", repairTimePartitionEndLogPrefix);
    logStream.write(endTimePartitionLog.getBytes());
    logStream.flush();
  }

  public void recordOneFile(TsFileResource resource) throws IOException {
    String fileLog =
        String.format("%s %s\n", cannotRepairFileLogPrefix, resource.getTsFile().getAbsolutePath());
    logStream.write(fileLog.getBytes());
  }

  public String getRepairLogFilePath() {
    return logFile.getAbsolutePath();
  }

  @Override
  public void close() throws IOException {
    if (logStream != null) {
      logStream.getFD().sync();
      logStream.close();
    }
  }
}
