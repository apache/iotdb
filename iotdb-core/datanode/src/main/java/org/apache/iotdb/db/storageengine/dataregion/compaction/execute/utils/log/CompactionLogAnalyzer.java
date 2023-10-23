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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger.STR_DELETED_TARGET_FILES;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger.STR_SOURCE_FILES;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger.STR_TARGET_FILES;

public class CompactionLogAnalyzer {

  private final File logFile;
  private final List<TsFileIdentifier> sourceFileInfos = new ArrayList<>();
  private final List<TsFileIdentifier> targetFileInfos = new ArrayList<>();
  private final List<TsFileIdentifier> deletedTargetFileInfos = new ArrayList<>();
  private CompactionTaskStage taskStage;

  public CompactionLogAnalyzer(File logFile) {
    this.logFile = logFile;
  }

  /** analyze (source files, target files, deleted target files). */
  public void analyze() throws IOException, IllegalArgumentException {
    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(logFile))) {
      String currLine;
      while ((currLine = bufferedReader.readLine()) != null) {
        final String lineValue = currLine;
        String fileInfo;
        if (currLine.startsWith(STR_SOURCE_FILES)) {
          fileInfo = currLine.replaceFirst(STR_SOURCE_FILES + TsFileIdentifier.INFO_SEPARATOR, "");
          sourceFileInfos.add(TsFileIdentifier.getFileIdentifierFromInfoString(fileInfo));
        } else if (currLine.startsWith(STR_TARGET_FILES)) {
          fileInfo = currLine.replaceFirst(STR_TARGET_FILES + TsFileIdentifier.INFO_SEPARATOR, "");
          targetFileInfos.add(TsFileIdentifier.getFileIdentifierFromInfoString(fileInfo));
        } else if (currLine.startsWith(STR_DELETED_TARGET_FILES)) {
          fileInfo =
              currLine.replaceFirst(STR_DELETED_TARGET_FILES + TsFileIdentifier.INFO_SEPARATOR, "");
          deletedTargetFileInfos.add(TsFileIdentifier.getFileIdentifierFromInfoString(fileInfo));
        } else if (Stream.of(CompactionTaskStage.values())
            .anyMatch(stage -> lineValue.startsWith(stage.name()))) {
          taskStage = CompactionTaskStage.valueOf(currLine);
        } else {
          throw new IllegalArgumentException(
              String.format("unknown compaction log line: %s", currLine));
        }
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

  public CompactionTaskStage getTaskStage() {
    return taskStage;
  }
}
