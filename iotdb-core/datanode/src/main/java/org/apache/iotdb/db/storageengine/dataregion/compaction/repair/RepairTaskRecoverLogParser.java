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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RepairTaskRecoverLogParser {

  private final File logFile;
  private static final Logger LOGGER = LoggerFactory.getLogger(RepairTaskRecoverLogParser.class);
  private final Map<RepairTimePartition, Set<String>> repairedTimePartitionsWithCannotRepairFiles =
      new HashMap<>();
  private long repairTaskStartTime = Long.MIN_VALUE;
  private RepairTimePartition currentTimePartition;
  private Set<String> currentTimePartitionCannotRepairFiles;

  public RepairTaskRecoverLogParser(File logFile) {
    this.logFile = logFile;
  }

  void parse() throws IOException {
    parseLogFile();
  }

  private void parseLogFile() throws IOException {
    try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
      String curLine;
      while ((curLine = reader.readLine()) != null) {
        if (curLine.startsWith(RepairLogger.repairTaskStartTimeLogPrefix)) {
          parseTaskStartTimeLog(curLine);
        } else if (curLine.startsWith(RepairLogger.repairTimePartitionStartLogPrefix)) {
          parseStartTimePartitionLog(curLine.trim());
        } else if (curLine.startsWith(RepairLogger.repairTimePartitionEndLogPrefix)) {
          parseEndTimePartitionLog(curLine.trim());
        } else if (curLine.startsWith(RepairLogger.cannotRepairFileLogPrefix)) {
          parseFileLog(curLine.trim());
        } else {
          throw new IllegalArgumentException("Unknown format of repair log");
        }
      }
      if (currentTimePartitionCannotRepairFiles != null
          && !currentTimePartitionCannotRepairFiles.isEmpty()) {
        handleIncompleteRepairedTimePartition();
      }
    }
  }

  private void parseTaskStartTimeLog(String line) {
    if (repairTaskStartTime != Long.MIN_VALUE) {
      return;
    }
    String[] values = line.split(" ");
    if (values.length != 2) {
      throw new RuntimeException(String.format("String '%s' is not a legal repair log", line));
    }
    repairTaskStartTime = Long.parseLong(values[1]);
  }

  private void parseStartTimePartitionLog(String line) {
    if (currentTimePartition != null) {
      handleIncompleteRepairedTimePartition();
    }
    String[] values = line.split(" ");
    if (values.length != 4) {
      throw new RuntimeException(String.format("String '%s' is not a legal repair log", line));
    }
    currentTimePartition = new RepairTimePartition(values[1], values[2], Long.parseLong(values[3]));
    currentTimePartitionCannotRepairFiles = new HashSet<>();
  }

  private void parseFileLog(String line) {
    if (line.length() <= RepairLogger.cannotRepairFileLogPrefix.length()) {
      throw new RuntimeException(String.format("String '%s' is not a legal repair log", line));
    }
    String filePath = line.substring(RepairLogger.cannotRepairFileLogPrefix.length());
    File f = new File(filePath.trim());
    currentTimePartitionCannotRepairFiles.add(f.getName());
  }

  private void parseEndTimePartitionLog(String line) {
    repairedTimePartitionsWithCannotRepairFiles.put(
        currentTimePartition, currentTimePartitionCannotRepairFiles);
    currentTimePartition = null;
    currentTimePartitionCannotRepairFiles = null;
  }

  private void handleIncompleteRepairedTimePartition() {
    LOGGER.error(
        "[{}][{}]Repair data log is not complete, time partition is {}.",
        currentTimePartition.getDatabaseName(),
        currentTimePartition.getDataRegionId(),
        currentTimePartition.getTimePartitionId());
    repairedTimePartitionsWithCannotRepairFiles.put(
        currentTimePartition, currentTimePartitionCannotRepairFiles);
  }

  long getRepairDataTaskStartTime() {
    return repairTaskStartTime < 0 ? System.currentTimeMillis() : repairTaskStartTime;
  }

  Map<RepairTimePartition, Set<String>> getRepairedTimePartitionsWithCannotRepairFiles() {
    return repairedTimePartitionsWithCannotRepairFiles;
  }

  String getRepairLogFilePath() {
    return logFile.getAbsolutePath();
  }
}
