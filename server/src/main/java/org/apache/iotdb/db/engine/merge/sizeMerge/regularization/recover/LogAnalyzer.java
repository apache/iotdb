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

package org.apache.iotdb.db.engine.merge.sizeMerge.regularization.recover;


import static org.apache.iotdb.db.engine.merge.sizeMerge.regularization.recover.RegularizationMergeLogger.STR_ALL_TS_END;
import static org.apache.iotdb.db.engine.merge.sizeMerge.regularization.recover.RegularizationMergeLogger.STR_MERGE_START;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LogAnalyzer scans the "merge.log" file and recovers information such as files of last merge,
 * whether the new file is generated. An example of merging 1 seqFile and 1 unseqFile is: seqFiles
 * server/0seq.tsfile unseqFiles server/0unseq.tsfile merge start all ts end
 * server/0seq.tsfile.merge merge end
 */
public class LogAnalyzer {

  private static final Logger logger = LoggerFactory.getLogger(
      LogAnalyzer.class);

  private String taskName;
  private File logFile;

  private String currLine;

  private TsFileResource newResource;
  private Status status;

  public LogAnalyzer(String taskName, File logFile) {
    this.taskName = taskName;
    this.logFile = logFile;
  }

  /**
   * Scan through the logs to find out where the last merge has stopped and store the information
   * about the progress in the fields.
   *
   * @return a Status indicating the completed stage of the last merge.
   */
  public Status analyze() throws IOException {
    status = Status.NONE;
    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(logFile))) {
      currLine = bufferedReader.readLine();
      if (currLine != null) {
        analyzeSeqFiles(bufferedReader);
      }
    }
    return status;
  }

  private void analyzeSeqFiles(BufferedReader bufferedReader) throws IOException {
    if (!STR_MERGE_START.equals(currLine)) {
      return;
    }
    status = Status.MERGE_START;
    long startTime = System.currentTimeMillis();
    while ((currLine = bufferedReader.readLine()) != null) {
      if (STR_ALL_TS_END.equals(currLine)) {
        status = Status.ALL_TS_MERGED;
        return;
      }
      newResource = new TsFileResource(new File(currLine));
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{} found {} size merge file after {}ms", taskName, newResource.getFileSize(),
          (System.currentTimeMillis() - startTime));
    }
  }

  public TsFileResource getNewResource() {
    return newResource;
  }

  public enum Status {
    // almost nothing has been done
    NONE,
    // at least the files and timeseries to be merged are known
    MERGE_START,
    // all the timeseries have been merged(new file is generated)
    ALL_TS_MERGED
  }
}
