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

package org.apache.iotdb.db.engine.flush;

import static org.apache.iotdb.db.engine.flush.VmLogger.MERGE_FINISHED;
import static org.apache.iotdb.db.engine.flush.VmLogger.SOURCE_NAME;
import static org.apache.iotdb.db.engine.flush.VmLogger.TARGET_NAME;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class VmLogAnalyzer {

  static final String STR_DEVICE_OFFSET_SEPERATOR = " ";

  private File logFile;
  private boolean isMergeFinished = false;
  private Set<String> deviceSet = new HashSet<>();
  private long offset = 0;
  private List<File> sourceFiles = new ArrayList<>();
  private File targetFile = null;

  public VmLogAnalyzer(File logFile) {
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
          default:
            String[] resultList = currLine.split(STR_DEVICE_OFFSET_SEPERATOR);
            deviceSet.add(resultList[0]);
            offset = Long.parseLong(resultList[1]);
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
}
