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
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RepairLogger implements Closeable {

  private static final String repairLogSuffix = ".repair_log";
  private final File logFile;
  private final long repairTaskStartTime;

  public RepairLogger() {
    this.repairTaskStartTime = System.currentTimeMillis();
    this.logFile = new File(String.format("%s%s", repairTaskStartTime, repairLogSuffix));
  }

  void recordRepairedTimePartition(TimePartitionFiles timePartition) {
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
      recordOneFile(cannotRepairFile);
    }
  }

  private void markStartOfRepairedTimePartition(TimePartitionFiles timePartition) {}

  private void markEndOfRepairedTimePartition(TimePartitionFiles timePartition) {}

  private void recordOneFile(TsFileResource resource) {}

  public String getRepairLogFilePath() {
    return logFile.getAbsolutePath();
  }

  @Override
  public void close() throws IOException {}
}
