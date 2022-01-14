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

import org.apache.iotdb.db.engine.compaction.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.cross.inplace.recover.InplaceCompactionLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.query.control.FileReaderManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import static org.apache.iotdb.db.engine.compaction.cross.inplace.recover.InplaceCompactionLogger.MAGIC_STRING;
import static org.apache.iotdb.db.engine.compaction.cross.inplace.recover.InplaceCompactionLogger.STR_SEQ_FILES;
import static org.apache.iotdb.db.engine.compaction.cross.inplace.recover.InplaceCompactionLogger.STR_TARGET_FILES;
import static org.apache.iotdb.db.engine.compaction.cross.inplace.recover.InplaceCompactionLogger.STR_UNSEQ_FILES;

/**
 * CrossSpaceCompactionTask merges given seqFiles and unseqFiles into new ones, which basically
 * consists of three steps: 1. rewrite overflowed, modified or small-sized chunks into temp merge
 * files 2. move the merged chunks in the temp files back to the seqFiles. 3. remove unseqFiles and
 * seqFiles
 */
public class CrossSpaceCompactionTask implements Callable<Void> {

  public static final String MERGE_SUFFIX = ".merge";
  private static final Logger logger = LoggerFactory.getLogger(CrossSpaceCompactionTask.class);
  private List<TsFileResource> targetTsfileResourceList;
  private List<TsFileResource> holdReadLockList = new ArrayList<>();
  private List<TsFileResource> holdWriteLockList = new ArrayList<>();

  List<TsFileResource> sequenceTsFileResourceList;
  List<TsFileResource> unsequenceTsFileResourceList;
  String storageGroupSysDir;
  String storageGroupName;
  InplaceCompactionLogger inplaceCompactionLogger;
  String taskName;
  States states = States.START;

  public CrossSpaceCompactionTask(
      List<TsFileResource> seqFiles,
      List<TsFileResource> unseqFiles,
      String storageGroupSysDir,
      String taskName,
      String storageGroupName) {
    this.sequenceTsFileResourceList = seqFiles;
    this.unsequenceTsFileResourceList = unseqFiles;
    this.storageGroupSysDir = storageGroupSysDir;
    this.taskName = taskName;
    this.storageGroupName = storageGroupName;
  }

  @Override
  public Void call() throws Exception {
    try {
      doCompaction();
    } catch (Exception e) {
      logger.error("Runtime exception in merge {}", taskName, e);
      abort();
    } finally {
      releaseAllLock();
    }
    return null;
  }

  private void abort() throws IOException {
    states = States.ABORTED;
    cleanUp();
  }

  private void doCompaction() throws IOException, StorageEngineException, MetadataException {
    long startTime = System.currentTimeMillis();
    targetTsfileResourceList = new ArrayList<>();
    List<File> targetFiles =
        TsFileNameGenerator.getCrossCompactionTargetFile(sequenceTsFileResourceList);
    for (File targetFile : targetFiles) {
      targetTsfileResourceList.add(new TsFileResource(targetFile));
    }
    if (targetTsfileResourceList.isEmpty()
        && sequenceTsFileResourceList.isEmpty()
        && unsequenceTsFileResourceList.isEmpty()) {
      return;
    }
    addReadLock(sequenceTsFileResourceList);
    addReadLock(unsequenceTsFileResourceList);
    logger.info("{}-crossSpaceCompactionTask start.", storageGroupName);
    inplaceCompactionLogger = new InplaceCompactionLogger(storageGroupSysDir);
    // print the path of the temporary file first for priority check during recovery
    inplaceCompactionLogger.logFiles(targetTsfileResourceList, STR_TARGET_FILES);
    inplaceCompactionLogger.logFiles(sequenceTsFileResourceList, STR_SEQ_FILES);
    inplaceCompactionLogger.logFiles(unsequenceTsFileResourceList, STR_UNSEQ_FILES);
    states = States.COMPACTION;
    CompactionUtils.compact(
        sequenceTsFileResourceList,
        unsequenceTsFileResourceList,
        targetTsfileResourceList,
        storageGroupName);
    // indicates that the merge is complete and needs to be cleared
    // the result can be reused during a restart recovery
    inplaceCompactionLogger.logStringInfo(MAGIC_STRING);

    states = States.CLEAN_UP;
    CompactionUtils.moveToTargetFile(targetTsfileResourceList, false, storageGroupName);

    releaseReadAndLockWrite(sequenceTsFileResourceList);
    releaseReadAndLockWrite(unsequenceTsFileResourceList);

    deleteOldFiles(sequenceTsFileResourceList);
    deleteOldFiles(unsequenceTsFileResourceList);

    cleanUp();
    logger.info(
        "{}-crossSpaceCompactionTask Costs {} s",
        storageGroupName,
        (System.currentTimeMillis() - startTime) / 1000);
  }

  private void addReadLock(List<TsFileResource> tsFileResourceList) {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      tsFileResource.readLock();
      holdReadLockList.add(tsFileResource);
    }
  }

  private void releaseReadAndLockWrite(List<TsFileResource> tsFileResourceList) {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      tsFileResource.readUnlock();
      holdReadLockList.remove(tsFileResource);
      tsFileResource.writeLock();
      holdWriteLockList.add(tsFileResource);
    }
  }

  private void releaseAllLock() {
    for (TsFileResource tsFileResource : holdReadLockList) {
      tsFileResource.readUnlock();
    }
    holdReadLockList.clear();
    for (TsFileResource tsFileResource : holdWriteLockList) {
      tsFileResource.writeUnlock();
    }
    holdWriteLockList.clear();
  }

  void deleteOldFiles(List<TsFileResource> tsFileResourceList) throws IOException {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFilePath());
      tsFileResource.setDeleted(true);
      tsFileResource.delete();
      logger.info(
          "[CrossSpaceCompaction] Delete TsFile :{}.",
          tsFileResource.getTsFile().getAbsolutePath());
    }
  }

  void cleanUp() throws IOException {
    logger.info("{} is cleaning up", taskName);
    for (TsFileResource seqFile : sequenceTsFileResourceList) {
      for (File file :
          TsFileNameGenerator.getCrossCompactionTargetFile(Collections.singletonList(seqFile))) {
        // this file does not necessarily exist, so deletion results need not be processed
        file.delete();
        seqFile.setMerging(false);
      }
    }
    for (TsFileResource unseqFile : unsequenceTsFileResourceList) {
      unseqFile.setMerging(false);
    }
    if (inplaceCompactionLogger != null) {
      inplaceCompactionLogger.close();
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
      case COMPACTION:
        return "Compaction";
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
    COMPACTION,
    CLEAN_UP,
    ABORTED
  }
}
