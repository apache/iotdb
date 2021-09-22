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

package org.apache.iotdb.db.engine.compaction.inner.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger.*;

public class SizeTieredCompactionLogAnalyzer {

  public static final String STR_DEVICE_OFFSET_SEPARATOR = " ";

  private File logFile;
  private List<String> sourceFiles = new ArrayList<>();
  private String targetFile = null;
  private boolean isSeq = false;

  public SizeTieredCompactionLogAnalyzer(File logFile) {
    this.logFile = logFile;
  }

  /** @return analyze (source file list, target file) */
  public void analyze() throws IOException {
    String currLine;
    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(logFile))) {
      while ((currLine = bufferedReader.readLine()) != null) {
        switch (currLine) {
          case SOURCE_NAME:
            currLine = bufferedReader.readLine();
            sourceFiles.add(currLine);
            break;
          case TARGET_NAME:
            currLine = bufferedReader.readLine();
            targetFile = currLine;
            break;
          case SEQUENCE_NAME:
            isSeq = true;
            break;
          case UNSEQUENCE_NAME:
            isSeq = false;
            break;
          default:
            break;
        }
      }
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
}
