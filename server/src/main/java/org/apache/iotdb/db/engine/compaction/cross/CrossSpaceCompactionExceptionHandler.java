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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.compaction.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogAnalyzer;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
      TsFileManager tsFileManager) {
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

      List<TsFileResource> lostSeqFiles = new ArrayList<>();
      List<TsFileResource> lostUnseqFiles = new ArrayList<>();

      boolean allSeqFilesExist = checkAllSourceFileExists(seqResourceList, lostSeqFiles);
      boolean allUnseqFilesExist = checkAllSourceFileExists(unseqResourceList, lostUnseqFiles);

      if (allSeqFilesExist && allUnseqFilesExist) {
        // all source files exists, remove target file and recover memory
        handleSuccess =
            handleWhenAllSourceFilesExist(
                storageGroup,
                targetResourceList,
                seqResourceList,
                unseqResourceList,
                tsFileManager);
      } else {
        handleSuccess =
            handleWhenSomeSourceFilesLost(
                storageGroup, seqResourceList, unseqResourceList, logFile);
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
   * When all source files exists, convert compaction modification to normal modification, delete
   * target files and recover memory. To avoid triggering OOM again under OOM errors, we do not
   * check whether the target files are complete.
   */
  private static boolean handleWhenAllSourceFilesExist(
      String storageGroup,
      List<TsFileResource> targetTsFiles,
      List<TsFileResource> seqFileList,
      List<TsFileResource> unseqFileList,
      TsFileManager tsFileManager)
      throws IOException {
    TsFileResourceList unseqTsFileResourceList =
        tsFileManager.getUnsequenceListByTimePartition(unseqFileList.get(0).getTimePartition());
    TsFileResourceList seqTsFileResourceList =
        tsFileManager.getSequenceListByTimePartition(seqFileList.get(0).getTimePartition());

    // append new modifications to old mods files
    CompactionUtils.appendNewModificationsToOldModsFile(seqFileList);
    CompactionUtils.appendNewModificationsToOldModsFile(unseqFileList);

    boolean removeAllTargetFile = true;
    try {
      seqTsFileResourceList.writeLock();
      unseqTsFileResourceList.writeLock();
      for (TsFileResource targetTsFile : targetTsFiles) {
        // delete target files and tmp target files
        TsFileResource tmpTargetTsFile;
        if (targetTsFile.getTsFilePath().endsWith(IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX)) {
          tmpTargetTsFile = targetTsFile;
          targetTsFile =
              new TsFileResource(
                  new File(
                      tmpTargetTsFile
                          .getTsFilePath()
                          .replace(
                              IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX,
                              TsFileConstant.TSFILE_SUFFIX)));
        } else {
          tmpTargetTsFile =
              new TsFileResource(
                  new File(
                      targetTsFile
                          .getTsFilePath()
                          .replace(
                              TsFileConstant.TSFILE_SUFFIX,
                              IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX)));
        }
        if (!tmpTargetTsFile.remove()) {
          LOGGER.error(
              "{} [Compaction][Exception] failed to delete tmp target tsfile {} when handling exception",
              storageGroup,
              tmpTargetTsFile);
          removeAllTargetFile = false;
        }
        if (!targetTsFile.remove()) {
          LOGGER.error(
              "{} [Compaction][Exception] failed to delete target tsfile {} when handling exception",
              storageGroup,
              targetTsFile);
          removeAllTargetFile = false;
        }

        // remove target tsfile resource in memory
        if (targetTsFile.isFileInList()) {
          seqTsFileResourceList.remove(targetTsFile);
        }
      }

      // recover source tsfile resource in memory
      for (TsFileResource tsFileResource : seqTsFileResourceList) {
        if (!tsFileResource.isFileInList()) {
          seqTsFileResourceList.keepOrderInsert(tsFileResource);
        }
      }
      for (TsFileResource tsFileResource : unseqTsFileResourceList) {
        if (!tsFileResource.isFileInList()) {
          unseqTsFileResourceList.keepOrderInsert(tsFileResource);
        }
      }
    } finally {
      seqTsFileResourceList.writeUnlock();
      unseqTsFileResourceList.writeUnlock();
    }
    return removeAllTargetFile;
  }

  /**
   * Some source files are lost, check if the compaction has finished. If the compaction has
   * finished, try to rename the target files and delete source files. If the compaction has not
   * finished, set the allowCompaction in tsFileManager to false and print some error logs.
   */
  public static boolean handleWhenSomeSourceFilesLost(
      String storageGroup,
      List<TsFileResource> seqFileList,
      List<TsFileResource> unseqFileList,
      File logFile)
      throws IOException {
    long magicStringLength =
        RewriteCrossSpaceCompactionLogger.MAGIC_STRING.getBytes(StandardCharsets.UTF_8).length;
    long fileLength = logFile.length();

    if (fileLength < 2 * magicStringLength) {
      // the log length is less than twice the Magic String length
      // it means the compaction has not finished yet
      LOGGER.error(
          "{} [Compaction][ExceptionHandler] the compaction log length is less than twice "
              + "the MagicString length",
          storageGroup);
      return false;
    }

    // read head magic string in compaction log
    RewriteCrossSpaceCompactionLogAnalyzer logAnalyzer =
        new RewriteCrossSpaceCompactionLogAnalyzer(logFile);
    logAnalyzer.analyze();
    if (!logAnalyzer.isFirstMagicStringExisted()) {
      LOGGER.error(
          "{} [Compaction][ExceptionHandler] the head magic string in compaction log is incorrect,"
              + " failed to handle exception",
          storageGroup);
      return false;
    }

    // read tail string in compaction log
    if (!logAnalyzer.isEndMagicStringExisted()) {
      LOGGER.error(
          "{} [Compaction][ExceptionHandler] the tail magic string in compaction log is incorrect,"
              + " failed to handle exception",
          storageGroup);
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
    CompactionUtils.removeCompactionModification(seqFileList, unseqFileList, storageGroup);

    return true;
  }
}
