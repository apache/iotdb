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

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileRepairStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RepairLogger implements Closeable {

  private static final String repairTimePartitionStartLogPrefix = "START_TIME_PARTITION";
  private static final String cannotRepairFileLogPrefix = "TSFILE";
  private static final String repairTimePartitionEndLogPrefix = "END_TIME_PARTITION";
  private static final String repairLogSuffix = ".repair-data.log";
  private final File logFile;
  private final long repairTaskStartTime;
  private final FileOutputStream logStream;

  public RepairLogger() throws FileNotFoundException {
    this.repairTaskStartTime = System.currentTimeMillis();
    this.logFile = new File(String.format("%s%s", repairTaskStartTime, repairLogSuffix));
    this.logStream = new FileOutputStream(logFile, true);
  }

  void recordRepairedTimePartition(TimePartitionFiles timePartition) throws IOException {
    markStartOfRepairedTimePartition(timePartition);
    recordCannotRepairFiles(timePartition);
    markEndOfRepairedTimePartition(timePartition);
  }

  void recordCannotRepairFiles(TimePartitionFiles timePartition) {
    TsFileManager tsFileManager = timePartition.getTsFileManager();
    List<TsFileResource> seqResources =
        tsFileManager.getTsFileListSnapshot(timePartition.getTimePartition(), true);
    List<TsFileResource> unseqResources =
        tsFileManager.getTsFileListSnapshot(timePartition.getTimePartition(), false);
    List<TsFileResource> cannotRepairFiles =
        Stream.concat(seqResources.stream(), unseqResources.stream())
            .filter(
                resource -> resource.getTsFileRepairStatus() == TsFileRepairStatus.CAN_NOT_REPAIR)
            .collect(Collectors.toList());
    for (TsFileResource cannotRepairFile : cannotRepairFiles) {
      try {
        recordOneFile(cannotRepairFile);
      } catch (Exception e) {

      }
    }
  }

  private void markStartOfRepairedTimePartition(TimePartitionFiles timePartition)
      throws IOException {
    String startTimePartitionLog =
        String.format(
            "%s %s %s %s\n",
            repairTimePartitionStartLogPrefix,
            timePartition.getDatabaseName(),
            timePartition.getDataRegionId(),
            timePartition.getTimePartition());
    logStream.write(startTimePartitionLog.getBytes());
  }

  private void markEndOfRepairedTimePartition(TimePartitionFiles timePartition) throws IOException {
    String endTimePartitionLog = String.format("%s\n", repairTimePartitionEndLogPrefix);
    logStream.write(endTimePartitionLog.getBytes());
    logStream.flush();
    logStream.getFD().sync();
  }

  private void recordOneFile(TsFileResource resource) throws IOException {
    String fileLog =
        String.format("%s %s\n", cannotRepairFileLogPrefix, resource.getTsFile().getName());
    logStream.write(fileLog.getBytes());
  }

  public String getRepairLogFilePath() {
    return logFile.getAbsolutePath();
  }

  @Override
  public void close() throws IOException {}
}
