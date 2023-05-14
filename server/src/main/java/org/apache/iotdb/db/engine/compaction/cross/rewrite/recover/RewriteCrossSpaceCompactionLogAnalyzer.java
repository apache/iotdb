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
package org.apache.iotdb.db.engine.compaction.cross.rewrite.recover;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.compaction.TsFileIdentifier;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.InvalidPropertiesFormatException;
import java.util.List;

import static org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger.MAGIC_STRING;
import static org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger.STR_SEQ_FILES;
import static org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger.STR_TARGET_FILES;
import static org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger.STR_UNSEQ_FILES;

public class RewriteCrossSpaceCompactionLogAnalyzer {

  private final File logFile;
  private final List<TsFileIdentifier> sourceFileInfos = new ArrayList<>();
  private final List<TsFileIdentifier> targetFileInfos = new ArrayList<>();
  private boolean isFirstMagicStringExisted = false;
  private boolean isEndMagicStringExisted = false;
  private boolean isLogFromOld = false;

  public RewriteCrossSpaceCompactionLogAnalyzer(File logFile) {
    this.logFile = logFile;
  }

  /** @return analyze (source file list, target file) */
  public void analyze() throws IOException {
    String currLine;
    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(logFile))) {
      if ((currLine = bufferedReader.readLine()) != null) {
        switch (currLine) {
          case MAGIC_STRING:
            // compaction log of version 0.13
            isFirstMagicStringExisted = true;
            analyzeLog(bufferedReader);
            break;
          case STR_SEQ_FILES:
            // compaction log of version < 0.13
            isLogFromOld = true;
            analyzeOldLog(bufferedReader);
            break;
          default:
            throw new InvalidPropertiesFormatException(
                "Unsupported string in cross space log :" + logFile.getAbsolutePath());
        }
      }
    }
  }

  /** Analyze cross space compaction log of version 0.13. */
  private void analyzeLog(BufferedReader bufferedReader) throws IOException {
    String currLine;
    boolean isTargetFile = false;
    while ((currLine = bufferedReader.readLine()) != null) {
      switch (currLine) {
        case MAGIC_STRING:
          isEndMagicStringExisted = true;
          break;
        case STR_TARGET_FILES:
          isTargetFile = true;
          break;
        case STR_SEQ_FILES:
        case STR_UNSEQ_FILES:
          isTargetFile = false;
          break;
        default:
          analyzeFilePath(isTargetFile, currLine);
          break;
      }
    }
  }

  /** Analyze cross space compaction log of previous version (<0.13). */
  private void analyzeOldLog(BufferedReader bufferedReader) throws IOException {
    String currLine;
    boolean isSeqSource = true;
    while ((currLine = bufferedReader.readLine()) != null) {
      if (currLine.equals(STR_UNSEQ_FILES)) {
        isSeqSource = false;
        continue;
      }
      analyzeOldFilePath(isSeqSource, currLine);
    }
  }

  private void analyzeFilePath(boolean isTargetFile, String filePath) {
    if (isTargetFile) {
      targetFileInfos.add(TsFileIdentifier.getFileIdentifierFromInfoString(filePath));
    } else {
      sourceFileInfos.add(TsFileIdentifier.getFileIdentifierFromInfoString(filePath));
    }
  }

  private void analyzeOldFilePath(boolean isSeqSource, String oldFilePath) {
    sourceFileInfos.add(TsFileIdentifier.getFileIdentifierFromOldInfoString(oldFilePath));
    if (isSeqSource) {
      String targetFilePath =
          oldFilePath.replace(
              TsFileConstant.TSFILE_SUFFIX,
              TsFileConstant.TSFILE_SUFFIX
                  + IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX_FROM_OLD);
      targetFileInfos.add(TsFileIdentifier.getFileIdentifierFromOldInfoString(targetFilePath));
    }
  }

  public List<TsFileIdentifier> getSourceFileInfos() {
    return sourceFileInfos;
  }

  public List<TsFileIdentifier> getTargetFileInfos() {
    return targetFileInfos;
  }

  public boolean isEndMagicStringExisted() {
    return isEndMagicStringExisted;
  }

  public boolean isFirstMagicStringExisted() {
    return isFirstMagicStringExisted;
  }

  public boolean isLogFromOld() {
    return isLogFromOld;
  }
}
