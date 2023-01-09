/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.compaction.execute.utils.log;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.engine.compaction.execute.utils.log.CompactionLogger.SEQUENCE_NAME_FROM_OLD;
import static org.apache.iotdb.db.engine.compaction.execute.utils.log.CompactionLogger.STR_DELETED_TARGET_FILES;
import static org.apache.iotdb.db.engine.compaction.execute.utils.log.CompactionLogger.STR_MERGE_START_FROM_OLD;
import static org.apache.iotdb.db.engine.compaction.execute.utils.log.CompactionLogger.STR_SEQ_FILES_FROM_OLD;
import static org.apache.iotdb.db.engine.compaction.execute.utils.log.CompactionLogger.STR_SOURCE_FILES;
import static org.apache.iotdb.db.engine.compaction.execute.utils.log.CompactionLogger.STR_SOURCE_FILES_FROM_OLD;
import static org.apache.iotdb.db.engine.compaction.execute.utils.log.CompactionLogger.STR_TARGET_FILES;
import static org.apache.iotdb.db.engine.compaction.execute.utils.log.CompactionLogger.STR_TARGET_FILES_FROM_OLD;
import static org.apache.iotdb.db.engine.compaction.execute.utils.log.CompactionLogger.STR_UNSEQ_FILES_FROM_OLD;
import static org.apache.iotdb.db.engine.compaction.execute.utils.log.CompactionLogger.UNSEQUENCE_NAME_FROM_OLD;

public class CompactionLogAnalyzer {

  private final File logFile;
  private final List<TsFileIdentifier> sourceFileInfos = new ArrayList<>();
  private final List<TsFileIdentifier> targetFileInfos = new ArrayList<>();
  private final List<TsFileIdentifier> deletedTargetFileInfos = new ArrayList<>();
  private boolean isLogFromOld = false;

  public CompactionLogAnalyzer(File logFile) {
    this.logFile = logFile;
  }

  /** @return analyze (source files, target files, deleted target files) */
  public void analyze() throws IOException {
    String currLine;
    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(logFile))) {
      while ((currLine = bufferedReader.readLine()) != null) {
        String fileInfo;
        if (currLine.startsWith(STR_SOURCE_FILES)) {
          fileInfo = currLine.replace(STR_SOURCE_FILES + TsFileIdentifier.INFO_SEPARATOR, "");
          sourceFileInfos.add(TsFileIdentifier.getFileIdentifierFromInfoString(fileInfo));
        } else if (currLine.startsWith(STR_TARGET_FILES)) {
          fileInfo = currLine.replace(STR_TARGET_FILES + TsFileIdentifier.INFO_SEPARATOR, "");
          targetFileInfos.add(TsFileIdentifier.getFileIdentifierFromInfoString(fileInfo));
        } else {
          fileInfo =
              currLine.replace(STR_DELETED_TARGET_FILES + TsFileIdentifier.INFO_SEPARATOR, "");
          deletedTargetFileInfos.add(TsFileIdentifier.getFileIdentifierFromInfoString(fileInfo));
        }
      }
    }
  }

  /** Analyze inner space compaction log of previous version (<0.13). */
  public void analyzeOldInnerCompactionLog() throws IOException {
    isLogFromOld = true;
    String currLine;
    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(logFile))) {
      while ((currLine = bufferedReader.readLine()) != null) {
        switch (currLine) {
          case STR_SOURCE_FILES_FROM_OLD:
            currLine = bufferedReader.readLine();
            sourceFileInfos.add(TsFileIdentifier.getFileIdentifierFromOldInfoString(currLine));
            break;
          case STR_TARGET_FILES_FROM_OLD:
            currLine = bufferedReader.readLine();
            targetFileInfos.add(TsFileIdentifier.getFileIdentifierFromOldInfoString(currLine));
            break;
          case STR_SOURCE_FILES:
            currLine = bufferedReader.readLine();
            sourceFileInfos.add(TsFileIdentifier.getFileIdentifierFromFilePath(currLine));
            break;
          case STR_TARGET_FILES:
            currLine = bufferedReader.readLine();
            targetFileInfos.add(TsFileIdentifier.getFileIdentifierFromFilePath(currLine));
            break;
          case SEQUENCE_NAME_FROM_OLD:
          case UNSEQUENCE_NAME_FROM_OLD:
            break;
          default:
            break;
        }
      }
    }
  }

  /** Analyze cross space compaction log of previous version (<0.13). */
  public void analyzeOldCrossCompactionLog() throws IOException {
    isLogFromOld = true;
    String currLine;
    boolean isSeqSource = true;
    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(logFile))) {
      while ((currLine = bufferedReader.readLine()) != null) {
        if (currLine.equals(STR_UNSEQ_FILES_FROM_OLD)) {
          isSeqSource = false;
          continue;
        } else if (currLine.equals(STR_SEQ_FILES_FROM_OLD)) {
          isSeqSource = true;
          continue;
        } else if (currLine.equals(STR_MERGE_START_FROM_OLD)) {
          break;
        }
        analyzeOldFilePath(isSeqSource, currLine);
      }
    }
  }

  private void analyzeOldFilePath(boolean isSeqSource, String oldFilePath) {
    if (oldFilePath.startsWith("root")) {
      sourceFileInfos.add(TsFileIdentifier.getFileIdentifierFromOldInfoString(oldFilePath));
    } else {
      sourceFileInfos.add(TsFileIdentifier.getFileIdentifierFromFilePath(oldFilePath));
    }
    if (isSeqSource) {
      String targetFilePath =
          oldFilePath.replace(
              TsFileConstant.TSFILE_SUFFIX,
              TsFileConstant.TSFILE_SUFFIX
                  + IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX_FROM_OLD);
      if (oldFilePath.startsWith("root")) {
        targetFileInfos.add(TsFileIdentifier.getFileIdentifierFromOldInfoString(targetFilePath));
      } else {
        targetFileInfos.add(TsFileIdentifier.getFileIdentifierFromFilePath(targetFilePath));
      }
    }
  }

  public List<TsFileIdentifier> getSourceFileInfos() {
    return sourceFileInfos;
  }

  public List<TsFileIdentifier> getTargetFileInfos() {
    return targetFileInfos;
  }

  public List<TsFileIdentifier> getDeletedTargetFileInfos() {
    return deletedTargetFileInfos;
  }

  public boolean isLogFromOld() {
    return isLogFromOld;
  }
}
