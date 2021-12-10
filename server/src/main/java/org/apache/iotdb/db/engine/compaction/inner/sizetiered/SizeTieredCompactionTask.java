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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.inner.utils.InnerSpaceCompactionUtils;
import org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.exception.WriteLockFailedException;
import org.apache.iotdb.db.rescon.TsFileResourceManager;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
  }

  @Override
  protected void doCompaction() throws Exception {
    long startTime = System.currentTimeMillis();
    // get resource of target file
    String dataDirectory = selectedTsFileResourceList.get(0).getTsFile().getParent();
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
    // to mark if we got the write lock or read lock of the selected tsfile
    boolean[] isHoldingReadLock = new boolean[selectedTsFileResourceList.size()];
    boolean[] isHoldingWriteLock = new boolean[selectedTsFileResourceList.size()];
    for (int i = 0; i < selectedTsFileResourceList.size(); ++i) {
      isHoldingReadLock[i] = false;
      isHoldingWriteLock[i] = false;
    }
    LOGGER.info(
        "{} [Compaction] Try to get the read lock of all selected files", fullStorageGroupName);
    for (int i = 0; i < selectedTsFileResourceList.size(); ++i) {
      selectedTsFileResourceList.get(i).readLock();
      isHoldingReadLock[i] = true;
    }

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

      try {
        // replace the old files with new file, the new is in same position as the old
        for (TsFileResource resource : selectedTsFileResourceList) {
          TsFileResourceManager.getInstance().removeTsFileResource(resource);
        }
        tsFileResourceList.insertBefore(selectedTsFileResourceList.get(0), targetTsFileResource);
        TsFileResourceManager.getInstance().registerSealedTsFileResource(targetTsFileResource);
        for (TsFileResource resource : selectedTsFileResourceList) {
          tsFileResourceList.remove(resource);
        }
      } finally {
        tsFileManager.writeUnlock();
      }

      InnerSpaceCompactionUtils.combineModsInCompaction(
          selectedTsFileResourceList, targetTsFileResource);

      // delete the old files
      InnerSpaceCompactionUtils.deleteTsFilesInDisk(
          selectedTsFileResourceList, fullStorageGroupName);
      LOGGER.info(
          "{} [SizeTiredCompactionTask] old file deleted, start to rename mods file",
          fullStorageGroupName);

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
      LOGGER.info("{} [Compaction] Start to handle exception", fullStorageGroupName);
      if (sizeTieredCompactionLogger != null) {
        sizeTieredCompactionLogger.close();
      }
      handleException(logFile, targetTsFileResource);
    } finally {
      for (int i = 0; i < selectedTsFileResourceList.size(); ++i) {
        if (isHoldingReadLock[i]) {
          selectedTsFileResourceList.get(i).readUnlock();
        }
        if (isHoldingWriteLock[i]) {
          selectedTsFileResourceList.get(i).writeUnlock();
        }
        selectedTsFileResourceList.get(i).setMerging(false);
      }
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
    long minVersionNum = Long.MAX_VALUE;
    try {
      for (TsFileResource resource : selectedTsFileResourceList) {
        if (resource.isMerging() | !resource.isClosed() || !resource.getTsFile().exists()) {
          return false;
        }
        TsFileNameGenerator.TsFileName tsFileName =
            TsFileNameGenerator.getTsFileName(resource.getTsFile().getName());
        if (tsFileName.getVersion() < minVersionNum) {
          minVersionNum = tsFileName.getVersion();
        }
      }
    } catch (IOException e) {
      LOGGER.error("CompactionTask exists while check valid", e);
    }
    for (TsFileResource resource : selectedTsFileResourceList) {
      resource.setMerging(true);
    }
    return true;
  }

  public void handleException(File logFile, TsFileResource targetTsFile) {
    boolean handleSuccess = true;
    if (logFile.exists()) {
      // if the compaction log file does not exist
      // it means the compaction process didn't start yet
      // we need not to handle it
      boolean allSourceFileExist = true;
      List<TsFileResource> lostSourceFiles = new ArrayList<>();
      for (TsFileResource sourceTsFile : selectedTsFileResourceList) {
        if (!sourceTsFile.getTsFile().exists()) {
          allSourceFileExist = false;
          lostSourceFiles.add(sourceTsFile);
        }
      }

      if (allSourceFileExist) {
        // all source file exists, delete the target file
        LOGGER.info(
            "{} [Compaction][ExceptionHandler] all source files {} exists, delete target file {}",
            fullStorageGroupName,
            selectedTsFileResourceList,
            targetTsFile);
        if (targetTsFile.remove()) {
          for (TsFileResource tsFileResource : selectedTsFileResourceList) {
            if (!tsFileResourceList.contains(tsFileResource)) {
              tsFileResourceList.add(tsFileResource);
            }
          }
          tsFileResourceList.remove(targetTsFile);
        } else {
          // failed to remove target tsfile
          LOGGER.warn(
              "{} [Compaction][ExceptionHandler] failed to remove target file {}, set allowCompaction to false",
              fullStorageGroupName,
              targetTsFile);
          tsFileManager.setAllowCompaction(false);
          handleSuccess = false;
        }
      } else {
        // some source file does not exists
        // it means we start to delete source file
        LOGGER.info(
            "{} [Compaction][ExceptionHandler] some source files {} is lost",
            fullStorageGroupName,
            lostSourceFiles);
        if (!targetTsFile.getTsFile().exists()) {
          // some source files are missed, and target file not exists
          // some data is lost, set the system to read-only
          LOGGER.warn(
              "{} [Compaction][ExceptionHandler] target file {} does not exist either, do nothing. Set system to read-only",
              fullStorageGroupName,
              targetTsFile);
          IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
          tsFileManager.setAllowCompaction(false);
          handleSuccess = false;
        } else {
          try {
            RestorableTsFileIOWriter writer =
                new RestorableTsFileIOWriter(targetTsFile.getTsFile());
            writer.close();
            if (!writer.hasCrashed()) {
              // target file is complete, delete source files
              LOGGER.info(
                  "{} [Compaction][ExceptionHandler] target file {} is complete, delete remaining source files",
                  fullStorageGroupName,
                  targetTsFile);
              for (TsFileResource sourceFile : selectedTsFileResourceList) {
                if (!sourceFile.remove()) {
                  LOGGER.warn(
                      "{} [Compaction][ExceptionHandler] failed to remove source file {}, set allowCompaction to false",
                      fullStorageGroupName,
                      sourceFile);
                  tsFileManager.setAllowCompaction(false);
                  handleSuccess = false;
                } else {
                  tsFileResourceList.remove(sourceFile);
                }
              }
              if (!tsFileResourceList.contains(targetTsFile)) {
                tsFileResourceList.add(targetTsFile);
              }
            } else {
              // target file is not complete, and some source file is lost
              // some data is lost
              LOGGER.warn(
                  "{} [Compaction][ExceptionHandler] target file {} is not complete, and some source files {} is lost, do nothing. Set allowCompaction to false",
                  fullStorageGroupName,
                  targetTsFile,
                  lostSourceFiles);
              IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
              tsFileManager.setAllowCompaction(false);
              handleSuccess = false;
            }
          } catch (Throwable e) {
            LOGGER.error(
                "{} [Compaction][ExceptionHandler] Another exception occurs during handling exception, set allowCompaction to false",
                fullStorageGroupName,
                e);
            tsFileManager.setAllowCompaction(false);
            handleSuccess = false;
          }
        }
      }

      if (handleSuccess) {
        LOGGER.info(
            "{} [Compaction][ExceptionHandler] Handle exception successfully, delete log file {}",
            fullStorageGroupName,
            logFile);
        try {
          FileUtils.delete(logFile);
        } catch (IOException e) {
          LOGGER.error(
              "{} [Compaction][ExceptionHandler] Exception occurs while deleting log file {}, set allowCompaction to false",
              fullStorageGroupName,
              logFile,
              e);
          tsFileManager.setAllowCompaction(false);
        }
      }
    }
  }
}
