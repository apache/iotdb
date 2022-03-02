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

package org.apache.iotdb.db.engine.merge.recover;

import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.iotdb.db.engine.merge.recover.MergeLogger.STR_SEQ_FILES;
import static org.apache.iotdb.db.engine.merge.recover.MergeLogger.STR_UNSEQ_FILES;

/**
 * LogAnalyzer scans the "merge.log" file and recovers information such as files of last merge, the
 * last available positions of each file and how many timeseries and files have been merged. An
 * example of merging 1 seqFile and 1 unseqFile containing 3 series is: seqFiles server/0seq.tsfile
 * unseqFiles server/0unseq.tsfile merge start start root.mergeTest.device0.sensor0
 * server/0seq.tsfile.merge 338 end start root.mergeTest.device0.sensor1 server/0seq.tsfile.merge
 * 664 end all ts end server/0seq.tsfile 145462 end merge end
 */
public class MergeLogAnalyzer {

  private static final Logger logger = LoggerFactory.getLogger(MergeLogAnalyzer.class);

  private MergeResource resource;
  private String taskName;
  private File logFile;
  private String currLine;

  private Status status;

  public MergeLogAnalyzer(
      MergeResource resource, String taskName, File logFile, String storageGroupName) {
    this.resource = resource;
    this.taskName = taskName;
    this.logFile = logFile;
  }

  /**
   * Scan through the logs to find out where the last merge has stopped and store the information
   * about the progress in the fields.
   *
   * @return a Status indicating the completed stage of the last merge.
   * @throws IOException
   */
  public Status analyze() throws IOException, MetadataException {
    status = Status.NONE;
    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(logFile))) {
      currLine = bufferedReader.readLine();
      if (currLine != null) {
        status = Status.All_SOURCE_FILES_EXIST;
        analyzeSeqFiles(bufferedReader);
        analyzeUnseqFiles(bufferedReader);
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
    boolean allSourceFileExists = true;
    while ((currLine = bufferedReader.readLine()) != null) {
      if (STR_UNSEQ_FILES.equals(currLine)) {
        break;
      }
      Iterator<TsFileResource> iterator = resource.getSeqFiles().iterator();
      MergeFileInfo toMatchedInfo = MergeFileInfo.getFileInfoFromString(currLine);
      boolean currentFileFound = false;
      while (iterator.hasNext()) {
        TsFileResource seqFile = iterator.next();
        if (MergeFileInfo.getFileInfoFromFile(seqFile.getTsFile()).equals(toMatchedInfo)) {
          mergeSeqFiles.add(seqFile);
          // remove to speed-up next iteration
          iterator.remove();
          if (seqFile.getTsFile().exists()) {
            currentFileFound = true;
          }
          break;
        }
      }
      if (!currentFileFound) {
        mergeSeqFiles.add(
            new TsFileResource(
                new File(
                    resource.getSeqFiles().get(0).getTsFile().getParent(),
                    MergeFileInfo.getFileInfoFromString(currLine).filename)));
        allSourceFileExists = false;
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{} found {} seq files after {}ms",
          taskName,
          mergeSeqFiles.size(),
          (System.currentTimeMillis() - startTime));
    }
    if (!allSourceFileExists) {
      status = Status.SOME_SOURCE_FILES_LOST;
    }
    resource.setSeqFiles(mergeSeqFiles);
  }

  private void analyzeUnseqFiles(BufferedReader bufferedReader) throws IOException {
    if (!STR_UNSEQ_FILES.equals(currLine)) {
      return;
    }
    long startTime = System.currentTimeMillis();
    List<TsFileResource> mergeUnseqFiles = new ArrayList<>();
    boolean allSourceFileExists = true;
    while ((currLine = bufferedReader.readLine()) != null) {
      Iterator<TsFileResource> iterator = resource.getUnseqFiles().iterator();
      MergeFileInfo toMatchInfo = MergeFileInfo.getFileInfoFromString(currLine);
      boolean currentFileFound = false;
      while (iterator.hasNext()) {
        TsFileResource unseqFile = iterator.next();
        if (MergeFileInfo.getFileInfoFromFile(unseqFile.getTsFile()).equals(toMatchInfo)) {
          mergeUnseqFiles.add(unseqFile);
          // remove to speed-up next iteration
          iterator.remove();
          if (unseqFile.getTsFile().exists()) {
            currentFileFound = true;
          }
          break;
        }
      }
      if (!currentFileFound) {
        mergeUnseqFiles.add(
            new TsFileResource(
                new File(
                    resource
                        .getSeqFiles()
                        .get(0)
                        .getTsFile()
                        .getParent()
                        .replace("sequence", "unsequence"),
                    MergeFileInfo.getFileInfoFromString(currLine).filename)));
        allSourceFileExists = false;
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{} found {} unseq files after {}ms",
          taskName,
          mergeUnseqFiles.size(),
          (System.currentTimeMillis() - startTime));
    }
    if (!allSourceFileExists) {
      status = Status.SOME_SOURCE_FILES_LOST;
    }
    resource.setUnseqFiles(mergeUnseqFiles);
  }

  public enum Status {
    // almost nothing has been done
    NONE,
    // all source files exist
    All_SOURCE_FILES_EXIST,
    // some source files lost
    SOME_SOURCE_FILES_LOST
  }
}
