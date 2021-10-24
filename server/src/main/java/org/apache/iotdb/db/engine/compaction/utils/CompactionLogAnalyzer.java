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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.FULL_MERGE;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.SEQUENCE_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.SOURCE_INFO;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.SOURCE_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.TARGET_INFO;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.TARGET_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.UNSEQUENCE_NAME;

public class CompactionLogAnalyzer {

  public static final String STR_DEVICE_OFFSET_SEPARATOR = " ";

  private File logFile;
  private List<String> deviceList = new ArrayList<>();
  private List<Long> offsets = new ArrayList<>();
  private List<String> sourceFiles = new ArrayList<>();
  private List<CompactionFileInfo> sourceFileInfo = new ArrayList<>();
  private CompactionFileInfo targetFileInfo = null;
  private String targetFile = null;
  private boolean isSeq = false;
  private boolean fullMerge = false;

  public CompactionLogAnalyzer(File logFile) {
    this.logFile = logFile;
  }

  /**
   * @return analyze (written device set, last offset, source file list, target file , is contains
   *     merge finished)
   */
  public void analyze() throws IOException {
    String currLine;
    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(logFile))) {
      while ((currLine = bufferedReader.readLine()) != null) {
        switch (currLine) {
          case SOURCE_NAME:
            currLine = bufferedReader.readLine();
            sourceFileInfo.add(CompactionFileInfo.parseCompactionFileInfoFromPath(currLine));
            break;
          case SOURCE_INFO:
            currLine = bufferedReader.readLine();
            sourceFileInfo.add(CompactionFileInfo.parseCompactionFileInfo(currLine));
            break;
          case TARGET_NAME:
            currLine = bufferedReader.readLine();
            targetFileInfo = CompactionFileInfo.parseCompactionFileInfoFromPath(currLine);
            break;
          case TARGET_INFO:
            currLine = bufferedReader.readLine();
            targetFileInfo = CompactionFileInfo.parseCompactionFileInfo(currLine);
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
            int separatorIndex = currLine.lastIndexOf(STR_DEVICE_OFFSET_SEPARATOR);
            deviceList.add(currLine.substring(0, separatorIndex));
            offsets.add(Long.parseLong(currLine.substring(separatorIndex + 1)));
            break;
        }
      }
    }
  }

  public Set<String> getDeviceSet() {
    if (offsets.size() < 2) {
      return new HashSet<>();
    } else {
      return new HashSet<>(deviceList.subList(0, deviceList.size() - 1));
    }
  }

  public long getOffset() {
    if (offsets.size() > 2) {
      return offsets.get(offsets.size() - 2);
    } else {
      return 0;
    }
  }

  public List<String> getSourceFiles() {
    return sourceFiles;
  }

  public String getTargetFile() {
    return targetFile;
  }

  public boolean isSeq() {
    return isSeq;
  }

  public boolean isFullMerge() {
    return fullMerge;
  }

  public List<CompactionFileInfo> getSourceFileInfo() {
    return sourceFileInfo;
  }

  public CompactionFileInfo getTargetFileInfo() {
    return targetFileInfo;
  }

  public static class CompactionFileInfo {
    String logicalStorageGroup;
    String virtualStorageGroupId;
    long timePartition;
    String filename;
    boolean sequence;

    private CompactionFileInfo(
        String logicalStorageGroup,
        String virtualStorageGroupId,
        long timePartition,
        String filename,
        boolean sequence) {
      this.logicalStorageGroup = logicalStorageGroup;
      this.virtualStorageGroupId = virtualStorageGroupId;
      this.timePartition = timePartition;
      this.filename = filename;
      this.sequence = sequence;
    }

    public static CompactionFileInfo parseCompactionFileInfo(String infoString) throws IOException {
      String[] info = infoString.split(" ");
      try {
        return new CompactionFileInfo(
            info[0], info[1], Long.parseLong(info[2]), info[3], info[4].equals("sequence"));
      } catch (Exception e) {
        throw new IOException("invalid compaction log line: " + infoString);
      }
    }

    public static CompactionFileInfo parseCompactionFileInfoFromPath(String filePath)
        throws IOException {
      String separator = File.separator;
      if (separator.equals("\\")) {
        separator += "\\";
      }
      String[] splitFilePath = filePath.split(separator);
      int pathLength = splitFilePath.length;
      if (pathLength < 4) {
        throw new IOException("invalid compaction file path: " + filePath);
      }
      try {
        return new CompactionFileInfo(
            splitFilePath[pathLength - 4],
            splitFilePath[pathLength - 3],
            Long.parseLong(splitFilePath[pathLength - 2]),
            splitFilePath[pathLength - 1],
            splitFilePath[pathLength - 5].equals("sequence") || splitFilePath[pathLength - 5].equals("target"));
      } catch (Exception e) {
        throw new IOException("invalid compaction log line: " + filePath);
      }
    }

    public File getFile(String dataDir) {
      return new File(
          dataDir
              + File.separator
              + (sequence ? "sequence" : "unsequence")
              + File.separator
              + logicalStorageGroup
              + File.separator
              + virtualStorageGroupId
              + File.separator
              + timePartition
              + File.separator
              + filename);
    }

    public String getFilename() {
      return filename;
    }
  }
}
