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

package org.apache.iotdb.db.engine.compaction.utils;

import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.FULL_MERGE;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.MERGE_FINISHED;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.SEQUENCE_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.SOURCE_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.TARGET_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.UNSEQUENCE_NAME;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CompactionLogAnalyzer {

  public static final String STR_DEVICE_OFFSET_SEPERATOR = " ";

  private File logFile;
  private boolean isMergeFinished = false;
  private Set<String> deviceSet = new HashSet<>();
  private long offset = 0;
  private List<File> sourceFiles = new ArrayList<>();
  private File targetFile = null;
  private boolean isSeq = false;
  private boolean fullMerge = false;

  public CompactionLogAnalyzer(File logFile) {
    this.logFile = logFile;
  }

  /**
   * @return analyze (written device set, last offset, source file list, target file , is contains
   * merge finished)
   */
  public void analyze() throws IOException {
    String currLine;
    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(logFile))) {
      while ((currLine = bufferedReader.readLine()) != null) {
        switch (currLine) {
          case SOURCE_NAME:
            currLine = bufferedReader.readLine();
            sourceFiles.add(new File(currLine));
            break;
          case TARGET_NAME:
            currLine = bufferedReader.readLine();
            targetFile = new File(currLine);
            break;
          case MERGE_FINISHED:
            isMergeFinished = true;
            break;
          case FULL_MERGE:
            fullMerge = true;
            break;
          case SEQUENCE_NAME:
            isSeq = true;
            break;
          case UNSEQUENCE_NAME:
            isSeq = false;
            break;
          default:
            if (currLine.contains(STR_DEVICE_OFFSET_SEPERATOR)) {
              String[] resultList = currLine.split(STR_DEVICE_OFFSET_SEPERATOR);
              deviceSet.add(resultList[0]);
              offset = Long.parseLong(resultList[1]);
            }
            break;
        }
      }
    }
  }

  public boolean isMergeFinished() {
    return isMergeFinished;
  }

  public Set<String> getDeviceSet() {
    return deviceSet;
  }

  public long getOffset() {
    return offset;
  }

  public List<File> getSourceFiles() {
    return sourceFiles;
  }

  public File getTargetFile() {
    return targetFile;
  }

  public boolean isSeq() {
    return isSeq;
  }

  public boolean isFullMerge() {
    return fullMerge;
  }
}
