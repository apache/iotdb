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

package org.apache.iotdb.db.engine.merge.squeeze.recover;


import static org.apache.iotdb.db.engine.merge.squeeze.recover.MergeLogger.STR_ALL_TS_END;
import static org.apache.iotdb.db.engine.merge.squeeze.recover.MergeLogger.STR_SEQ_FILES;
import static org.apache.iotdb.db.engine.merge.squeeze.recover.MergeLogger.STR_UNSEQ_FILES;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LogAnalyzer scans the "merge.log" file and recovers information such as files of last merge,
 * whether the new file is generated.
 * An example of merging 1 seqFile and 1 unseqFile is:
 * seqFiles
 * server/0seq.tsfile
 * unseqFiles
 * server/0unseq.tsfile
 * merge start
 * all ts end
 * server/0seq.tsfile.merge
 * merge end
 */
public class LogAnalyzer {

  private static final Logger logger = LoggerFactory.getLogger(
      LogAnalyzer.class);

  private MergeResource resource;
  private String taskName;
  private File logFile;

  private String currLine;

  private TsFileResource newResource;
  private Status status;

  public LogAnalyzer(MergeResource resource, String taskName, File logFile) {
    this.resource = resource;
    this.taskName = taskName;
    this.logFile = logFile;
  }

  /**
   * Scan through the logs to find out where the last merge has stopped and store the information
   * about the progress in the fields.
   * @return a Status indicating the completed stage of the last merge.
   * @throws IOException
   */
  public Status analyze() throws IOException {
    status = Status.NONE;
    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(logFile))) {
      currLine = bufferedReader.readLine();
      if (currLine != null) {
        analyzeSeqFiles(bufferedReader);

        analyzeUnseqFiles(bufferedReader);

        analyzeMergedFile(bufferedReader);
      }
    }
    return status;
  }

  private void analyzeSeqFiles(BufferedReader bufferedReader) throws IOException {
    if (!STR_SEQ_FILES.equals(currLine)) {
      return;
    }
    long startTime = System.currentTimeMillis();
    List<TsFileResource> mergeSeqFiles = new ArrayList<>();
    while ((currLine = bufferedReader.readLine()) != null) {
      if (STR_UNSEQ_FILES.equals(currLine)) {
        break;
      }
      Iterator<TsFileResource> iterator = resource.getSeqFiles().iterator();
      while (iterator.hasNext()) {
        TsFileResource seqFile = iterator.next();
        if (seqFile.getFile().getAbsolutePath().equals(currLine)) {
          mergeSeqFiles.add(seqFile);
          // remove to speed-up next iteration
          iterator.remove();
          break;
        }
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{} found {} seq files after {}ms", taskName, mergeSeqFiles.size(),
          (System.currentTimeMillis() - startTime));
    }
    resource.setSeqFiles(mergeSeqFiles);
  }

  private void analyzeUnseqFiles(BufferedReader bufferedReader) throws IOException {
    if (!STR_UNSEQ_FILES.equals(currLine)) {
      return;
    }
    long startTime = System.currentTimeMillis();
    List<TsFileResource> mergeUnseqFiles = new ArrayList<>();
    while ((currLine = bufferedReader.readLine()) != null) {
      if (currLine.equals(STR_ALL_TS_END)) {
        break;
      }
      Iterator<TsFileResource> iterator = resource.getUnseqFiles().iterator();
      while (iterator.hasNext()) {
        TsFileResource unseqFile = iterator.next();
        if (unseqFile.getFile().getAbsolutePath().equals(currLine)) {
          mergeUnseqFiles.add(unseqFile);
          // remove to speed-up next iteration
          iterator.remove();
          break;
        }
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{} found {} unseq files after {}ms", taskName, mergeUnseqFiles.size(),
          (System.currentTimeMillis() - startTime));
    }
    resource.setUnseqFiles(mergeUnseqFiles);
  }


  private void analyzeMergedFile(BufferedReader bufferedReader) throws IOException {
    if (!STR_ALL_TS_END.equals(currLine)) {
      return;
    }

    currLine = bufferedReader.readLine();
    if (currLine != null) {
      status = Status.ALL_TS_MERGED;
      File newFile = FSFactoryProducer.getFSFactory().getFile(currLine);
      newResource = new TsFileResource(newFile);
      newResource.deSerialize();

      if (logger.isDebugEnabled()) {
        logger.debug("{} found files have already been merged into {}", taskName,
            newFile.getPath());
      }
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
