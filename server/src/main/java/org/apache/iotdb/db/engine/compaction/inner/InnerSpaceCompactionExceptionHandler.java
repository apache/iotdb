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
package org.apache.iotdb.db.engine.compaction.inner;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.inner.utils.InnerSpaceCompactionUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.utils.TsFileUtils;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class is used to handle exception (including OOM error) occurred during compaction. The
 * <i>allowCompaction</i> flag in {@link org.apache.iotdb.db.engine.storagegroup.TsFileManager} may
 * be set to false if exception cannot be handled correctly(such as OOM during handling exception),
 * after which the subsequent compaction in this storage group will not be carried out. Under some
 * serious circumstances(such as data lost), the system will be set to read-only.
 */
public class InnerSpaceCompactionExceptionHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger("COMPACTION");

  public static void handleException(
      String fullStorageGroupName,
      File logFile,
      TsFileResource targetTsFile,
      List<TsFileResource> selectedTsFileResourceList,
      TsFileManager tsFileManager,
      TsFileResourceList tsFileResourceList) {

    if (!logFile.exists()) {
      // log file does not exist
      // it means the compaction has not started yet
      // we need not to handle it
      return;
    }

    boolean handleSuccess = true;

    List<TsFileResource> lostSourceFiles = new ArrayList<>();
    boolean allSourceFileExist =
        checkAllSourceFilesExist(selectedTsFileResourceList, lostSourceFiles);

    if (allSourceFileExist) {
      handleSuccess =
          handleWhenAllSourceFilesExist(
              fullStorageGroupName, targetTsFile, selectedTsFileResourceList);
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
        handleSuccess = false;
      } else {
        handleSuccess =
            handleWhenSomeSourceFilesLost(
                fullStorageGroupName,
                targetTsFile,
                selectedTsFileResourceList,
                tsFileResourceList,
                lostSourceFiles);
      }
    }

    if (!handleSuccess) {
      LOGGER.error(
          "{} [Compaction][ExceptionHandler] Failed to handle exception, set allowCompaction to false",
          fullStorageGroupName);
      tsFileManager.setAllowCompaction(false);
    } else {
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

  private static boolean checkAllSourceFilesExist(
      List<TsFileResource> sourceFiles, List<TsFileResource> lostSourceFiles) {
    boolean allSourceFileExist = true;
    for (TsFileResource sourceTsFile : sourceFiles) {
      if (!sourceTsFile.getTsFile().exists()) {
        allSourceFileExist = false;
        lostSourceFiles.add(sourceTsFile);
      }
    }
    return allSourceFileExist;
  }

  public static boolean handleWhenAllSourceFilesExist(
      String fullStorageGroupName,
      TsFileResource targetTsFile,
      List<TsFileResource> selectedTsFileResourceList) {
    // all source file exists, delete the target file
    LOGGER.info(
        "{} [Compaction][ExceptionHandler] all source files {} exists, delete target file {}",
        fullStorageGroupName,
        selectedTsFileResourceList,
        targetTsFile);
    TsFileResource tmpTargetTsFile;
    if (targetTsFile.getTsFilePath().endsWith(IoTDBConstant.COMPACTION_TMP_FILE_SUFFIX)) {
      tmpTargetTsFile = targetTsFile;
      targetTsFile =
          new TsFileResource(
              new File(
                  tmpTargetTsFile
                      .getTsFilePath()
                      .replace(
                          IoTDBConstant.COMPACTION_TMP_FILE_SUFFIX, TsFileConstant.TSFILE_SUFFIX)));
    } else {
      tmpTargetTsFile =
          new TsFileResource(
              new File(
                  targetTsFile
                      .getTsFilePath()
                      .replace(
                          TsFileConstant.TSFILE_SUFFIX, IoTDBConstant.COMPACTION_TMP_FILE_SUFFIX)));
    }
    if (!tmpTargetTsFile.remove()) {
      // failed to remove tmp target tsfile
      // system should not carry out the subsequent compaction in case of data redundant
      LOGGER.warn(
          "{} [Compaction][ExceptionHandler] failed to remove target file {}",
          fullStorageGroupName,
          tmpTargetTsFile);
      return false;
    }
    if (!targetTsFile.remove()) {
      // failed to remove target tsfile
      // system should not carry out the subsequent compaction in case of data redundant
      LOGGER.warn(
          "{} [Compaction][ExceptionHandler] failed to remove target file {}",
          fullStorageGroupName,
          targetTsFile);
      return false;
    }
    // deal with compaction modification
    try {
      InnerSpaceCompactionUtils.appendNewModificationsToOldModsFile(selectedTsFileResourceList);
    } catch (Throwable e) {
      LOGGER.error(
          "{} Exception occurs while handling exception, set allowCompaction to false",
          fullStorageGroupName,
          e);
      return false;
    }
    return true;
  }

  private static boolean handleWhenSomeSourceFilesLost(
      String fullStorageGroupName,
      TsFileResource targetTsFile,
      List<TsFileResource> selectedTsFileResourceList,
      TsFileResourceList tsFileResourceList,
      List<TsFileResource> lostSourceFiles) {
    boolean handleSuccess = true;
    try {
      if (TsFileUtils.isTsFileComplete(targetTsFile.getTsFile())) {
        // target file is complete, delete source files
        LOGGER.info(
            "{} [Compaction][ExceptionHandler] target file {} is complete, delete remaining source files",
            fullStorageGroupName,
            targetTsFile);
        for (TsFileResource sourceFile : selectedTsFileResourceList) {
          if (!sourceFile.remove()) {
            LOGGER.warn(
                "{} [Compaction][ExceptionHandler] failed to remove source file {}",
                fullStorageGroupName,
                sourceFile);
            handleSuccess = false;
          } else {
            tsFileResourceList.remove(sourceFile);
          }
        }

        InnerSpaceCompactionUtils.deleteModificationForSourceFile(
            selectedTsFileResourceList, fullStorageGroupName);

        if (!tsFileResourceList.contains(targetTsFile)) {
          tsFileResourceList.keepOrderInsert(targetTsFile);
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
        handleSuccess = false;
      }
    } catch (Throwable e) {
      // If we are handling OOM error, OOM may occur again during checking target file
      LOGGER.error(
          "{} [Compaction][ExceptionHandler] Another exception occurs during handling exception",
          fullStorageGroupName,
          e);
      handleSuccess = false;
    }
    return handleSuccess;
  }
}
