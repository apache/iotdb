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

package org.apache.iotdb.db.engine.compaction.cross.inplace.task;

import org.apache.iotdb.db.engine.compaction.cross.inplace.recover.InplaceCompactionLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static org.apache.iotdb.db.engine.compaction.cross.inplace.recover.InplaceCompactionLogger.MAGIC_STRING;
import static org.apache.iotdb.db.engine.compaction.cross.inplace.recover.InplaceCompactionLogger.STR_SEQ_FILES;
import static org.apache.iotdb.db.engine.compaction.cross.inplace.recover.InplaceCompactionLogger.STR_TARGET_FILES;
import static org.apache.iotdb.db.engine.compaction.cross.inplace.recover.InplaceCompactionLogger.STR_UNSEQ_FILES;

/**
 * CrossSpaceMergeTask merges given seqFiles and unseqFiles into new ones, which basically consists
 * of three steps: 1. rewrite overflowed, modified or small-sized chunks into temp merge files 2.
 * move the merged chunks in the temp files back to the seqFiles or move the unmerged chunks in the
 * seqFiles into temp files and replace the seqFiles with the temp files. 3. remove unseqFiles
 */
public class CrossSpaceMergeTask implements Callable<Void> {

  public static final String MERGE_SUFFIX = ".merge";
  private static final Logger logger = LoggerFactory.getLogger(CrossSpaceMergeTask.class);

  //  CrossSpaceMergeResource resource;
  List<TsFileResource> sequenceTsFileResourceList;
  List<TsFileResource> unsequenceTsFileResourceList;
  String storageGroupSysDir;
  String storageGroupName;
  InplaceCompactionLogger inplaceCompactionLogger;
  int concurrentMergeSeriesNum;
  String taskName;
  boolean fullMerge;
  States states = States.START;

  CrossSpaceMergeTask(
      List<TsFileResource> seqFiles,
      List<TsFileResource> unseqFiles,
      String storageGroupSysDir,
      String taskName,
      boolean fullMerge,
      String storageGroupName) {
    this.sequenceTsFileResourceList = seqFiles;
    this.unsequenceTsFileResourceList = unseqFiles;
    this.storageGroupSysDir = storageGroupSysDir;
    this.taskName = taskName;
    this.fullMerge = fullMerge;
    this.concurrentMergeSeriesNum = 1;
    this.storageGroupName = storageGroupName;
  }

  public CrossSpaceMergeTask(
      List<TsFileResource> seqFiles,
      List<TsFileResource> unseqFiles,
      String storageGroupSysDir,
      String taskName,
      boolean fullMerge,
      int concurrentMergeSeriesNum,
      String storageGroupName) {
    this.sequenceTsFileResourceList = seqFiles;
    this.unsequenceTsFileResourceList = unseqFiles;
    this.storageGroupSysDir = storageGroupSysDir;
    this.taskName = taskName;
    this.fullMerge = fullMerge;
    this.concurrentMergeSeriesNum = concurrentMergeSeriesNum;
    this.storageGroupName = storageGroupName;
  }

  @Override
  public Void call() throws Exception {
    try {
      doMerge();
    } catch (Exception e) {
      logger.error("Runtime exception in merge {}", taskName, e);
      abort();
    }
    return null;
  }

  private void abort() throws IOException {
    states = States.ABORTED;
    cleanUp();
  }

  void compact(
      List<TsFileResource> sequenceTsFileResourceList,
      List<TsFileResource> unsequenceTsFileResourceList,
      String storageGroupName,
      List<TsFileResource> targetTsFileResource) {
  }

  private void doMerge() throws IOException {
    long startTime = System.currentTimeMillis();

    List<TsFileResource> targetTsfileResourceList = new ArrayList<>();
    for (TsFileResource tsFileResource : sequenceTsFileResourceList) {
      targetTsfileResourceList.add(
          new TsFileResource(TsFileNameGenerator.increaseCrossCompactionCnt(tsFileResource)));
    }
    logger.info("{}-crossSpaceCompactionTask start.", storageGroupName);
    inplaceCompactionLogger = new InplaceCompactionLogger(storageGroupSysDir);
    inplaceCompactionLogger.logFiles(targetTsfileResourceList, STR_TARGET_FILES);
    inplaceCompactionLogger.logFiles(sequenceTsFileResourceList, STR_SEQ_FILES);
    inplaceCompactionLogger.logFiles(unsequenceTsFileResourceList, STR_UNSEQ_FILES);
    compact(
        sequenceTsFileResourceList,
        unsequenceTsFileResourceList,
        storageGroupName,
        targetTsfileResourceList);
    inplaceCompactionLogger.logStringInfo(MAGIC_STRING);

    for (TsFileResource tsFileResource : targetTsfileResourceList) {
      moveToTargetFile(tsFileResource);
    }
    List<String> unDeletedFiles =
        deleteOldFiles(sequenceTsFileResourceList, unsequenceTsFileResourceList);
    if (unDeletedFiles != null && !unDeletedFiles.isEmpty()) {
      logger.warn(
          "Failed to delete old files in the process of crossSpaceCompaction:" + unDeletedFiles);
      throw new IOException("Delete old files failed.");
    }
    states = States.CLEAN_UP;
    cleanUp();
    logger.info(
        "{}-crossSpaceCompactionTask Costs {} s",
        storageGroupName,
        (System.currentTimeMillis() - startTime) / 1000);
  }

  List<String> deleteOldFiles(
      List<TsFileResource> sequenceTsFileResourceList,
      List<TsFileResource> unsequenceTsFileResourceList) {
    List<String> failedToDeleteFileList = new ArrayList<>();
    for (TsFileResource tsFileResource : sequenceTsFileResourceList) {
      if (!tsFileResource.getTsFile().delete()) {
        failedToDeleteFileList.add(tsFileResource.getTsFile().getAbsolutePath());
      }
    }
    for (TsFileResource tsFileResource : unsequenceTsFileResourceList) {
      if (!tsFileResource.getTsFile().delete()) {
        failedToDeleteFileList.add(tsFileResource.getTsFile().getAbsolutePath());
      }
    }
    return failedToDeleteFileList;
  }

  void moveToTargetFile(TsFileResource targetFileResource) throws IOException {
    File targetFile = targetFileResource.getTsFile();
    File newFile =
        new File(
            targetFile
                .getAbsolutePath()
                .substring(0, targetFile.getAbsolutePath().lastIndexOf(MERGE_SUFFIX)));
    FSFactoryProducer.getFSFactory().moveFile(targetFile, newFile);
    targetFileResource.setFile(newFile);
    targetFileResource.serialize();
    targetFileResource.close();
  }

  void cleanUp() throws IOException {
    logger.info("{} is cleaning up", taskName);

    if (inplaceCompactionLogger != null) {
      inplaceCompactionLogger.close();
    }

    for (TsFileResource seqFile : sequenceTsFileResourceList) {
      File mergeFile = new File(seqFile.getTsFilePath() + MERGE_SUFFIX);
      mergeFile.delete();
      seqFile.setMerging(false);
    }
    for (TsFileResource unseqFile : unsequenceTsFileResourceList) {
      unseqFile.setMerging(false);
    }

    File logFile = new File(storageGroupSysDir, InplaceCompactionLogger.MERGE_LOG_NAME);
    logFile.delete();
  }

  public String getStorageGroupName() {
    return storageGroupName;
  }

  public String getProgress() {
    switch (states) {
      case ABORTED:
        return "Aborted";
      case CLEAN_UP:
        return "Cleaning up";
      case MERGE_FILES:
      case MERGE_CHUNKS:
        return "Merging ...";
      case START:
      default:
        return "Just started";
    }
  }

  public String getTaskName() {
    return taskName;
  }

  enum States {
    START,
    MERGE_CHUNKS,
    MERGE_FILES,
    CLEAN_UP,
    ABORTED
  }
}
