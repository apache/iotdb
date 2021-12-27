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
package org.apache.iotdb.db.engine.compaction.inner.sizetiered;

import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.inner.InnerSpaceCompactionExceptionHandler;
import org.apache.iotdb.db.engine.compaction.inner.utils.InnerSpaceCompactionUtils;
import org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.exception.WriteLockFailedException;
import org.apache.iotdb.db.rescon.TsFileResourceManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger.SOURCE_INFO;
import static org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger.TARGET_INFO;

/**
 * SizeTiredCompactionTask compact several inner space files selected by {@link
 * SizeTieredCompactionSelector} into one file.
 */
public class SizeTieredCompactionTask extends AbstractInnerSpaceCompactionTask {
  private static final Logger LOGGER = LoggerFactory.getLogger("COMPACTION");
  protected TsFileResourceList tsFileResourceList;
  protected TsFileManager tsFileManager;
  protected boolean[] isHoldingReadLock;
  protected boolean[] isHoldingWriteLock;

  public SizeTieredCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartition,
      TsFileManager tsFileManager,
      TsFileResourceList tsFileResourceList,
      List<TsFileResource> selectedTsFileResourceList,
      boolean sequence,
      AtomicInteger currentTaskNum) {
    super(
        logicalStorageGroupName + "-" + virtualStorageGroupName,
        timePartition,
        currentTaskNum,
        sequence,
        selectedTsFileResourceList);
    this.tsFileResourceList = tsFileResourceList;
    this.tsFileManager = tsFileManager;
    isHoldingReadLock = new boolean[selectedTsFileResourceList.size()];
    isHoldingWriteLock = new boolean[selectedTsFileResourceList.size()];
    for (int i = 0; i < selectedTsFileResourceList.size(); ++i) {
      isHoldingWriteLock[i] = false;
      isHoldingReadLock[i] = false;
    }
  }

  @Override
  protected void doCompaction() throws Exception {
    long startTime = System.currentTimeMillis();
    // get resource of target file
    String dataDirectory = selectedTsFileResourceList.get(0).getTsFile().getParent();
    // Here is tmpTargetFile, which is xxx.target
    String targetFileName =
        TsFileNameGenerator.getInnerCompactionFileName(selectedTsFileResourceList, sequence)
            .getName();
    TsFileResource targetTsFileResource =
        new TsFileResource(new File(dataDirectory + File.separator + targetFileName));
    LOGGER.info(
        "{} [Compaction] starting compaction task with {} files",
        fullStorageGroupName,
        selectedTsFileResourceList.size());
    File logFile = null;
    SizeTieredCompactionLogger sizeTieredCompactionLogger = null;

    try {
      logFile =
          new File(
              dataDirectory
                  + File.separator
                  + targetFileName
                  + SizeTieredCompactionLogger.COMPACTION_LOG_NAME);
      sizeTieredCompactionLogger = new SizeTieredCompactionLogger(logFile.getPath());

      for (TsFileResource resource : selectedTsFileResourceList) {
        sizeTieredCompactionLogger.logFileInfo(SOURCE_INFO, resource.getTsFile());
      }
      sizeTieredCompactionLogger.logSequence(sequence);
      sizeTieredCompactionLogger.logFileInfo(TARGET_INFO, targetTsFileResource.getTsFile());
      LOGGER.info(
          "{} [Compaction] compaction with {}", fullStorageGroupName, selectedTsFileResourceList);

      // carry out the compaction
      InnerSpaceCompactionUtils.compact(
          targetTsFileResource, selectedTsFileResourceList, fullStorageGroupName, true);
      InnerSpaceCompactionUtils.moveTargetFile(targetTsFileResource, fullStorageGroupName);
      LOGGER.info(
          "{} [SizeTiredCompactionTask] compact finish, close the logger", fullStorageGroupName);
      sizeTieredCompactionLogger.close();

      LOGGER.info(
          "{} [Compaction] compaction finish, start to delete old files", fullStorageGroupName);
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException(
            String.format("%s [Compaction] abort", fullStorageGroupName));
      }
      LOGGER.info(
          "{} [Compaction] Compacted target files, try to get the write lock of source files",
          fullStorageGroupName);

      // release the read lock of all source files, and get the write lock of them to delete them
      for (int i = 0; i < selectedTsFileResourceList.size(); ++i) {
        selectedTsFileResourceList.get(i).readUnlock();
        isHoldingReadLock[i] = false;
        selectedTsFileResourceList.get(i).writeLock();
        isHoldingWriteLock[i] = true;
      }

      LOGGER.info(
          "{} [SizeTiredCompactionTask] old file deleted, start to rename mods file",
          fullStorageGroupName);
      InnerSpaceCompactionUtils.combineModsInCompaction(
          selectedTsFileResourceList, targetTsFileResource);

      LOGGER.info(
          "{} [Compaction] Get the write lock of files, try to get the write lock of TsFileResourceList",
          fullStorageGroupName);
      // get write lock for TsFileResource list with timeout
      try {
        tsFileManager.writeLockWithTimeout("size-tired compaction", 60_000);
      } catch (WriteLockFailedException e) {
        // if current compaction thread couldn't get write lock
        // a WriteLockFailException will be thrown, then terminate the thread itself
        LOGGER.warn(
            "{} [SizeTiredCompactionTask] failed to get write lock, abort the task and delete the target file {}",
            fullStorageGroupName,
            targetTsFileResource.getTsFile(),
            e);
        throw new InterruptedException(
            String.format(
                "%s [Compaction] compaction abort because cannot acquire write lock",
                fullStorageGroupName));
      }

      // delete the old files
      InnerSpaceCompactionUtils.deleteTsFilesInDisk(
          selectedTsFileResourceList, fullStorageGroupName);
      InnerSpaceCompactionUtils.deleteModificationForSourceFile(
          selectedTsFileResourceList, fullStorageGroupName);

      // replace the old files with new file, the new is in same position as the old
      for (TsFileResource resource : selectedTsFileResourceList) {
        TsFileResourceManager.getInstance().removeTsFileResource(resource);
      }
      tsFileResourceList.insertBefore(selectedTsFileResourceList.get(0), targetTsFileResource);
      TsFileResourceManager.getInstance().registerSealedTsFileResource(targetTsFileResource);
      for (TsFileResource resource : selectedTsFileResourceList) {
        tsFileResourceList.remove(resource);
      }

      long costTime = System.currentTimeMillis() - startTime;
      LOGGER.info(
          "{} [SizeTiredCompactionTask] all compaction task finish, target file is {},"
              + "time cost is {} s",
          fullStorageGroupName,
          targetFileName,
          costTime / 1000);
      if (logFile.exists()) {
        logFile.delete();
      }
    } catch (Throwable throwable) {
      LOGGER.error(
          "{} [Compaction] Throwable is caught during execution of SizeTieredCompaction, {}",
          fullStorageGroupName,
          throwable);
      LOGGER.warn("{} [Compaction] Start to handle exception", fullStorageGroupName);
      if (sizeTieredCompactionLogger != null) {
        sizeTieredCompactionLogger.close();
      }
      InnerSpaceCompactionExceptionHandler.handleException(
          fullStorageGroupName,
          logFile,
          targetTsFileResource,
          selectedTsFileResourceList,
          tsFileManager,
          tsFileResourceList);
    } finally {
      releaseFileLocksAndResetMergingStatus(true);
      tsFileManager.writeUnlock();
    }
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask other) {
    if (other instanceof SizeTieredCompactionTask) {
      SizeTieredCompactionTask otherSizeTieredTask = (SizeTieredCompactionTask) other;
      if (!selectedTsFileResourceList.equals(otherSizeTieredTask.selectedTsFileResourceList)) {
        return false;
      }
      return true;
    }
    return false;
  }

  @Override
  public boolean checkValidAndSetMerging() {
    tsFileResourceList.readLock();
    try {
      for (int i = 0; i < selectedTsFileResourceList.size(); ++i) {
        TsFileResource resource = selectedTsFileResourceList.get(i);
        resource.readLock();
        isHoldingReadLock[i] = true;
        if (resource.isMerging() | !resource.isClosed()
            || !resource.getTsFile().exists()
            || resource.isDeleted()) {
          // this source file cannot be compacted
          // release the lock of locked files, and return
          releaseFileLocksAndResetMergingStatus(false);
          return false;
        }
      }

      for (TsFileResource resource : selectedTsFileResourceList) {
        resource.setMerging(true);
      }
      return true;
    } finally {
      tsFileResourceList.readUnlock();
    }
  }

  /**
   * release the read lock and write lock of files if it is held, and set the merging status of
   * selected files to false
   */
  private void releaseFileLocksAndResetMergingStatus(boolean resetCompactingStatus) {
    for (int i = 0; i < selectedTsFileResourceList.size(); ++i) {
      if (isHoldingReadLock[i]) {
        selectedTsFileResourceList.get(i).readUnlock();
      }
      if (isHoldingWriteLock[i]) {
        selectedTsFileResourceList.get(i).writeUnlock();
      }
      if (resetCompactingStatus) {
        selectedTsFileResourceList.get(i).setMerging(false);
      }
    }
  }
}
