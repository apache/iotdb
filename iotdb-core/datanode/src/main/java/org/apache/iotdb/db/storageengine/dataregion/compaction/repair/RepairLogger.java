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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
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

  static final String repairTimePartitionStartLogPrefix = "START_TIME_PARTITION";
  static final String cannotRepairFileLogPrefix = "TSFILE";
  static final String repairTimePartitionEndLogPrefix = "END_TIME_PARTITION";
  public static final String repairLogSuffix = ".repair-data.log";
  public static final String repairLogDir = "repair";
  private final File logFile;
  private final long repairTaskStartTime;
  private final FileOutputStream logStream;

  public RepairLogger() throws IOException {
    this.repairTaskStartTime = System.currentTimeMillis();
    File logFileDir =
        new File(
            IoTDBDescriptor.getInstance().getConfig().getSystemDir()
                + File.separator
                + repairLogDir);
    if (!logFileDir.exists()) {
      logFileDir.mkdirs();
    }
    File[] files = logFileDir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.getName().endsWith(repairLogSuffix)) {
          Files.delete(file.toPath());
        }
      }
    }
    String logFileName = String.format("%s%s", repairTaskStartTime, repairLogSuffix);
    this.logFile = new File(logFileDir.getPath() + File.separator + logFileName);
    Path logFilePath = logFile.toPath();
    if (!Files.exists(logFilePath)) {
      Files.createFile(logFilePath);
    }
    this.logStream = new FileOutputStream(logFile);
  }

  public RepairLogger(File logFile) throws FileNotFoundException {
    this.logFile = logFile;
    String logFileName = logFile.getName();
    this.repairTaskStartTime = Long.parseLong(logFileName.replace(repairLogSuffix, ""));
    this.logStream = new FileOutputStream(logFile, true);
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
        String.format("%s %s\n", cannotRepairFileLogPrefix, resource.getTsFile().getName());
    logStream.write(fileLog.getBytes());
  }

  public String getRepairLogFilePath() {
    return logFile.getAbsolutePath();
  }

  public long getRepairTaskStartTime() {
    return repairTaskStartTime;
  }

  @Override
  public void close() throws IOException {
    logStream.getFD().sync();
    logStream.close();
  }
}
