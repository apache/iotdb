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

import org.apache.iotdb.db.storageengine.dataregion.DataRegion;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RepairTaskRecoveryPerformer {

  private File logFile;
  private Map<TimePartitionFiles, List<String>> repairedTimePartitionsWithCannotRepairFiles;
  private TimePartitionFiles currentTimePartition;
  private List<String> currentTimePartitionCannotRepairFiles;

  public RepairTaskRecoveryPerformer(List<DataRegion> dataRegions) {}

  void perform() throws IOException {
    findLogFile();
    parseLogFile();
  }

  private void findLogFile() {}

  private void parseLogFile() throws IOException {
    try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
      String curLine;
      while ((curLine = reader.readLine()) != null) {
        if (curLine.startsWith(RepairLogger.repairTimePartitionStartLogPrefix)) {
          parseStartTimePartitionLog(curLine);
        } else if (curLine.startsWith(RepairLogger.repairTimePartitionEndLogPrefix)) {
          parseEndTimePartitionLog(curLine);
        } else if (curLine.startsWith(RepairLogger.cannotRepairFileLogPrefix)) {
          parseFileLog(curLine);
        } else {
          throw new IllegalArgumentException("Unknown format of repair log");
        }
      }
    }
  }

  private void parseStartTimePartitionLog(String line) {
    if (currentTimePartition != null) {
      // TODO: previous time partition log is not complete
    }
    String[] values = line.split(" ");
    if (values.length != 4) {
      throw new RuntimeException(String.format("String '%s' is not a legal repair log", line));
    }
    currentTimePartition = new TimePartitionFiles(values[1], values[2], Long.parseLong(values[3]));
    currentTimePartitionCannotRepairFiles = new ArrayList<>();
  }

  private void parseFileLog(String line) {
    currentTimePartitionCannotRepairFiles.add(line);
  }

  private void parseEndTimePartitionLog(String line) {
    repairedTimePartitionsWithCannotRepairFiles.put(
        currentTimePartition, currentTimePartitionCannotRepairFiles);
    currentTimePartition = null;
    currentTimePartitionCannotRepairFiles = null;
  }

  List<TimePartitionFiles> getRepairedTimePartitions() {
    List<TimePartitionFiles> repairedTimePartitions = new ArrayList<>();

    return repairedTimePartitions;
  }

  private void markResourcesThatCannotRepair(Set<TimePartitionFiles> timePartitions) {}

  public String getRepairLogFilePath() {
    return logFile.getAbsolutePath();
  }
}
