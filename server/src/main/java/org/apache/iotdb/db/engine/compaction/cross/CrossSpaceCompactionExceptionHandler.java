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

package org.apache.iotdb.db.engine.compaction.cross;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.compaction.CompactionUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.rescon.TsFileResourceManager;
import org.apache.iotdb.tsfile.utils.TsFileUtils;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CrossSpaceCompactionExceptionHandler {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  public static void handleException(
      String storageGroup,
      File logFile,
      List<TsFileResource> targetResourceList,
      List<TsFileResource> seqResourceList,
      List<TsFileResource> unseqResourceList,
      TsFileManager tsFileManager,
      long timePartition) {
    try {
      if (logFile == null || !logFile.exists()) {
        // the log file is null or the log file does not exists
        // it means that compaction has not started yet
        // we don't need to handle it
        return;
      }
      LOGGER.info(
          "[Compaction][ExceptionHandler] Cross space compaction start handling exception, source seqFiles is "
              + seqResourceList
              + ", source unseqFiles is "
              + unseqResourceList);

      boolean handleSuccess = true;

      List<TsFileResource> lostSourceFiles = new ArrayList<>();

      boolean allSeqFilesExist = checkAllSourceFileExists(seqResourceList, lostSourceFiles);
      boolean allUnseqFilesExist = checkAllSourceFileExists(unseqResourceList, lostSourceFiles);

      if (allSeqFilesExist && allUnseqFilesExist) {
        // all source files exists, remove target file and recover memory
        handleSuccess =
            handleWhenAllSourceFilesExist(
                storageGroup,
                targetResourceList,
                seqResourceList,
                unseqResourceList,
                tsFileManager,
                timePartition);
      } else {
        handleSuccess =
            handleWhenSomeSourceFilesLost(
                storageGroup,
                seqResourceList,
                unseqResourceList,
                targetResourceList,
                lostSourceFiles);
      }

      if (!handleSuccess) {
        LOGGER.error(
            "[Compaction][ExceptionHandler] failed to handle exception, set allowCompaction to false in {}",
            storageGroup);
        tsFileManager.setAllowCompaction(false);
      } else {
        FileUtils.delete(logFile);
      }
    } catch (Throwable throwable) {
      // catch throwable when handling exception
      // set the allowCompaction to false
      LOGGER.error(
          "[Compaction][ExceptionHandler] exception occurs when handling exception in cross space compaction."
              + " Set allowCompaction to false in {}",
          storageGroup,
          throwable);
      tsFileManager.setAllowCompaction(false);
    }
  }

  private static boolean checkAllSourceFileExists(
      List<TsFileResource> tsFileResources, List<TsFileResource> lostFiles) {
    for (TsFileResource tsFileResource : tsFileResources) {
      if (!tsFileResource.getTsFile().exists() || !tsFileResource.resourceFileExists()) {
        lostFiles.add(tsFileResource);
      }
    }
    return lostFiles.size() == 0;
  }

  /**
   * When all source files exists: (1) delete compaction mods files (2) delete target files, tmp
   * target files and its corresponding files (3) recover memory. To avoid triggering OOM again
   * under OOM errors, we do not check whether the target files are complete.
   */
  private static boolean handleWhenAllSourceFilesExist(
      String storageGroup,
      List<TsFileResource> targetTsFiles,
      List<TsFileResource> seqFileList,
      List<TsFileResource> unseqFileList,
      TsFileManager tsFileManager,
      long timePartition)
      throws IOException {
    TsFileResourceList unseqTsFileResourceList =
        tsFileManager.getUnsequenceListByTimePartition(timePartition);
    TsFileResourceList seqTsFileResourceList =
        tsFileManager.getSequenceListByTimePartition(timePartition);

    // delete compaction mods files
    CompactionUtils.deleteCompactionModsFile(seqFileList, unseqFileList);

    boolean removeAllTargetFile = true;
    tsFileManager.writeLock("CrossSpaceCompactionExceptionHandler");
    try {
      for (TsFileResource targetTsFile : targetTsFiles) {
        // delete target files
        targetTsFile.writeLock();
        if (!targetTsFile.remove()) {
          LOGGER.error(
              "{} [Compaction][Exception] failed to delete target tsfile {} when handling exception",
              storageGroup,
              targetTsFile);
          removeAllTargetFile = false;
        }
        targetTsFile.writeUnlock();

        // remove target tsfile resource in memory
        if (targetTsFile.isFileInList()) {
          seqTsFileResourceList.remove(targetTsFile);
          TsFileResourceManager.getInstance().removeTsFileResource(targetTsFile);
        }
      }

      // recover source tsfile resource in memory
      for (TsFileResource tsFileResource : seqFileList) {
        if (!tsFileResource.isFileInList()) {
          seqTsFileResourceList.keepOrderInsert(tsFileResource);
          TsFileResourceManager.getInstance().registerSealedTsFileResource(tsFileResource);
        }
      }
      for (TsFileResource tsFileResource : unseqFileList) {
        if (!tsFileResource.isFileInList()) {
          unseqTsFileResourceList.keepOrderInsert(tsFileResource);
          TsFileResourceManager.getInstance().registerSealedTsFileResource(tsFileResource);
        }
      }
    } finally {
      tsFileManager.writeUnlock();
    }
    return removeAllTargetFile;
  }

  /**
   * Some source files are lost, check if the compaction has finished. If the compaction has
   * finished, delete the remaining source files and compaction mods files. If the compaction has
   * not finished, set the allowCompaction in tsFileManager to false and print some error logs.
   */
  public static boolean handleWhenSomeSourceFilesLost(
      String storageGroup,
      List<TsFileResource> seqFileList,
      List<TsFileResource> unseqFileList,
      List<TsFileResource> targetFileList,
      List<TsFileResource> lostSourceFiles)
      throws IOException {
    if (!checkIsTargetFilesComplete(targetFileList, lostSourceFiles, storageGroup)) {
      return false;
    }

    // delete source files
    for (TsFileResource unseqFile : unseqFileList) {
      unseqFile.remove();
      unseqFile.setDeleted(true);
    }
    for (TsFileResource seqFile : seqFileList) {
      seqFile.remove();
      seqFile.setDeleted(true);
    }

    // delete compaction mods files
    CompactionUtils.deleteCompactionModsFile(seqFileList, unseqFileList);

    return true;
  }

  public static boolean checkIsTargetFilesComplete(
      List<TsFileResource> targetResources,
      List<TsFileResource> lostSourceResources,
      String fullStorageGroupName)
      throws IOException {
    for (TsFileResource targetResource : targetResources) {
      if (!TsFileUtils.isTsFileComplete(targetResource.getTsFile())) {
        LOGGER.error(
            "{} [Compaction][ExceptionHandler] target file {} is not complete, and some source files {} is lost, do nothing. Set allowCompaction to false",
            fullStorageGroupName,
            targetResource,
            lostSourceResources);
        return false;
      }
    }
    return true;
  }
}
