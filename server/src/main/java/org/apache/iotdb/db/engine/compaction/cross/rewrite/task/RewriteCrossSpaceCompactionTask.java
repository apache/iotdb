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
package org.apache.iotdb.db.engine.compaction.cross.rewrite.task;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.compaction.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.cross.AbstractCrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.cross.CrossSpaceCompactionExceptionHandler;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.rescon.TsFileResourceManager;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger.MAGIC_STRING;
import static org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger.STR_SEQ_FILES;
import static org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger.STR_TARGET_FILES;
import static org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger.STR_UNSEQ_FILES;

public class RewriteCrossSpaceCompactionTask extends AbstractCrossSpaceCompactionTask {

  private static final Logger logger =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  protected List<TsFileResource> selectedSeqTsFileResourceList;
  protected List<TsFileResource> selectedUnSeqTsFileResourceList;
  protected TsFileManager tsFileManager;
  private File logFile;

  private List<TsFileResource> targetTsfileResourceList;
  private List<TsFileResource> holdReadLockList = new ArrayList<>();
  private List<TsFileResource> holdWriteLockList = new ArrayList<>();

  public RewriteCrossSpaceCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartitionId,
      TsFileManager tsFileManager,
      List<TsFileResource> selectedSeqTsFileResourceList,
      List<TsFileResource> selectedUnSeqTsFileResourceList,
      AtomicInteger currentTaskNum) {
    super(
        logicalStorageGroupName + "-" + virtualStorageGroupName,
        timePartitionId,
        currentTaskNum,
        selectedSeqTsFileResourceList,
        selectedUnSeqTsFileResourceList);
    this.selectedSeqTsFileResourceList = selectedSeqTsFileResourceList;
    this.selectedUnSeqTsFileResourceList = selectedUnSeqTsFileResourceList;
    this.tsFileManager = tsFileManager;
  }

  @Override
  protected void doCompaction() throws Exception {
    try {
      executeCompaction();
    } catch (Throwable throwable) {
      // catch throwable instead of exception to handle OOM errors
      logger.error("Meet errors in cross space compaction, {}", throwable.getMessage());
      CrossSpaceCompactionExceptionHandler.handleException(
          fullStorageGroupName,
          logFile,
          targetTsfileResourceList,
          selectedSeqTsFileResourceList,
          selectedUnSeqTsFileResourceList,
          tsFileManager);
      throw throwable;
    } finally {
      releaseAllLock();
    }
  }

  private void executeCompaction()
      throws IOException, StorageEngineException, MetadataException, InterruptedException,
          WriteProcessException {
    if (!tsFileManager.isAllowCompaction()) {
      return;
    }
    long startTime = System.currentTimeMillis();
    targetTsfileResourceList =
        TsFileNameGenerator.getCrossCompactionTargetFileResources(selectedSeqTsFileResourceList);

    if (targetTsfileResourceList.isEmpty()
        || selectedSeqTsFileResourceList.isEmpty()
        || selectedUnSeqTsFileResourceList.isEmpty()) {
      logger.info(
          "{} [Compaction] Cross space compaction file list is empty, end it",
          fullStorageGroupName);
      return;
    }

    logger.info(
        "{} [Compaction] CrossSpaceCompactionTask start. Sequence files : {}, unsequence files : {}",
        fullStorageGroupName,
        selectedSeqTsFileResourceList,
        selectedUnSeqTsFileResourceList);
    logFile =
        new File(
            selectedSeqTsFileResourceList.get(0).getTsFile().getParent()
                + File.separator
                + targetTsfileResourceList.get(0).getTsFile().getName()
                + PATH_SEPARATOR
                + RewriteCrossSpaceCompactionLogger.COMPACTION_LOG_NAME);

    try (RewriteCrossSpaceCompactionLogger compactionLogger =
        new RewriteCrossSpaceCompactionLogger(logFile)) {
      // print the path of the temporary file first for priority check during recovery
      compactionLogger.logFiles(targetTsfileResourceList, STR_TARGET_FILES);
      compactionLogger.logFiles(selectedSeqTsFileResourceList, STR_SEQ_FILES);
      compactionLogger.logFiles(selectedUnSeqTsFileResourceList, STR_UNSEQ_FILES);
      CompactionUtils.compact(
          selectedSeqTsFileResourceList, selectedUnSeqTsFileResourceList, targetTsfileResourceList);

      CompactionUtils.moveTargetFile(targetTsfileResourceList, false, fullStorageGroupName);

      // indicates that the cross compaction is complete and the result can be reused during a
      // restart recovery
      compactionLogger.logStringInfo(MAGIC_STRING);
      compactionLogger.close();

      releaseReadAndLockWrite(selectedSeqTsFileResourceList);
      releaseReadAndLockWrite(selectedUnSeqTsFileResourceList);

      CompactionUtils.combineModsInCompaction(
          selectedSeqTsFileResourceList, selectedUnSeqTsFileResourceList, targetTsfileResourceList);

      deleteOldFiles(selectedSeqTsFileResourceList);
      deleteOldFiles(selectedUnSeqTsFileResourceList);
      removeCompactionModification();

      updateTsFileResource();
      if (logFile.exists()) {
        FileUtils.delete(logFile);
      }
      logger.info(
          "{} [Compaction] CrossSpaceCompactionTask Costs {} s",
          fullStorageGroupName,
          (System.currentTimeMillis() - startTime) / 1000);
    }
  }

  private void updateTsFileResource() throws IOException {
    for (TsFileResource resource : selectedSeqTsFileResourceList) {
      TsFileResourceManager.getInstance().removeTsFileResource(resource);
      tsFileManager.remove(resource, true);
    }
    for (TsFileResource resource : selectedUnSeqTsFileResourceList) {
      TsFileResourceManager.getInstance().removeTsFileResource(resource);
      tsFileManager.remove(resource, false);
    }
    for (TsFileResource resource : targetTsfileResourceList) {
      tsFileManager.getSequenceListByTimePartition(timePartition).keepOrderInsert(resource);
      TsFileResourceManager.getInstance().registerSealedTsFileResource(resource);
    }
  }

  private boolean addReadLock(List<TsFileResource> tsFileResourceList) {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      if (tsFileResource.isCompacting()
          || !tsFileResource.isClosed()
          || !tsFileResource.getTsFile().exists()
          || tsFileResource.isDeleted()) {
        releaseAllLock();
        return false;
      }
      tsFileResource.readLock();
      holdReadLockList.add(tsFileResource);
      tsFileResource.setCompacting(true);
    }
    return true;
  }

  @Override
  public boolean checkValidAndSetMerging() {
    return addReadLock(selectedSeqTsFileResourceList)
        && addReadLock(selectedUnSeqTsFileResourceList);
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
      tsFileResource.setCompacting(false);
    }
    for (TsFileResource tsFileResource : holdWriteLockList) {
      tsFileResource.writeUnlock();
      tsFileResource.setCompacting(false);
    }
    holdReadLockList.clear();
    holdWriteLockList.clear();
  }

  void deleteOldFiles(List<TsFileResource> tsFileResourceList) throws IOException {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFilePath());
      tsFileResource.setDeleted(true);
      tsFileResource.remove();
      logger.info(
          "[CrossSpaceCompaction] Delete TsFile :{}.",
          tsFileResource.getTsFile().getAbsolutePath());
    }
  }

  public String getStorageGroupName() {
    return fullStorageGroupName;
  }

  private void removeCompactionModification() {
    try {
      for (TsFileResource seqFile : selectedSeqTsFileResourceList) {
        ModificationFile.getCompactionMods(seqFile).remove();
      }
      for (TsFileResource unseqFile : selectedUnSeqTsFileResourceList) {
        ModificationFile.getCompactionMods(unseqFile).remove();
      }
    } catch (IOException e) {
      logger.error("{} cannot remove merging modification ", fullStorageGroupName, e);
    }
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask other) {
    if (other instanceof RewriteCrossSpaceCompactionTask) {
      RewriteCrossSpaceCompactionTask otherTask = (RewriteCrossSpaceCompactionTask) other;
      return otherTask.selectedSeqTsFileResourceList.equals(selectedSeqTsFileResourceList)
          && otherTask.selectedUnSeqTsFileResourceList.equals(selectedUnSeqTsFileResourceList);
    }
    return false;
  }
}
