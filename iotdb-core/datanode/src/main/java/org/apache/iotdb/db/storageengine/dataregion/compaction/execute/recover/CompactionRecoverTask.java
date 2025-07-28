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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.recover;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogAnalyzer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.TsFileIdentifier;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.ModificationFileV1;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.utils.TsFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** CompactionRecoverTask executes the recover process for all compaction tasks. */
public class CompactionRecoverTask {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private final File compactionLogFile;
  private final boolean isInnerSpace;
  private final String fullStorageGroupName;
  private final TsFileManager tsFileManager;

  public CompactionRecoverTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      TsFileManager tsFileManager,
      File logFile,
      boolean isInnerSpace) {
    this.compactionLogFile = logFile;
    this.isInnerSpace = isInnerSpace;
    this.fullStorageGroupName = logicalStorageGroupName + "-" + virtualStorageGroupName;
    this.tsFileManager = tsFileManager;
  }

  @SuppressWarnings({"squid:S3776", "squid:S6541"})
  public void doCompaction() {
    boolean recoverSuccess = true;
    LOGGER.info(
        "{} [Compaction][Recover] compaction log is {}", fullStorageGroupName, compactionLogFile);
    try {
      if (compactionLogFile.exists()) {
        LOGGER.info(
            "{} [Compaction][Recover] compaction log file {} exists, start to recover it",
            fullStorageGroupName,
            compactionLogFile);
        CompactionLogAnalyzer logAnalyzer = new CompactionLogAnalyzer(compactionLogFile);
        logAnalyzer.analyze();
        List<TsFileIdentifier> sourceFileIdentifiers = logAnalyzer.getSourceFileInfos();
        List<TsFileIdentifier> targetFileIdentifiers = logAnalyzer.getTargetFileInfos();
        List<TsFileIdentifier> deletedTargetFileIdentifiers =
            logAnalyzer.getDeletedTargetFileInfos();

        // compaction log file is incomplete
        if (targetFileIdentifiers.isEmpty() || sourceFileIdentifiers.isEmpty()) {
          LOGGER.info(
              "{} [Compaction][Recover] incomplete log file, abort recover", fullStorageGroupName);
          return;
        }

        // check is all source files existed
        boolean isAllSourcesFileExisted = true;
        for (TsFileIdentifier sourceFileIdentifier : sourceFileIdentifiers) {
          File sourceFile = sourceFileIdentifier.getFileFromDataDirs();
          if (sourceFile == null) {
            isAllSourcesFileExisted = false;
            break;
          }
        }

        if (isAllSourcesFileExisted) {
          recoverSuccess =
              handleWithAllSourceFilesExist(targetFileIdentifiers, sourceFileIdentifiers);

        } else {
          recoverSuccess =
              handleWithSomeSourceFilesLost(
                  targetFileIdentifiers, deletedTargetFileIdentifiers, sourceFileIdentifiers);
        }
      }
    } catch (IOException e) {
      LOGGER.error("Recover compaction error", e);
    } finally {
      if (!recoverSuccess) {
        LOGGER.error("{} [Compaction][Recover] Failed to recover compaction", fullStorageGroupName);
      } else {
        if (compactionLogFile.exists()) {
          try {
            LOGGER.info(
                "{} [Compaction][Recover] Recover compaction successfully, delete log file {}",
                fullStorageGroupName,
                compactionLogFile);
            FileUtils.delete(compactionLogFile);
          } catch (IOException e) {
            LOGGER.error(
                "{} [Compaction][Recover] Exception occurs while deleting log file {}",
                fullStorageGroupName,
                compactionLogFile,
                e);
          }
        }
      }
    }
  }

  /**
   * All source files exist: (1) delete all the target files and tmp target files (2) delete
   * compaction mods files.
   */
  private boolean handleWithAllSourceFilesExist(
      List<TsFileIdentifier> targetFileIdentifiers, List<TsFileIdentifier> sourceFileIdentifiers) {
    LOGGER.info(
        "{} [Compaction][Recover] all source files exists, delete all target files.",
        fullStorageGroupName);

    // remove tmp target files and target files
    for (TsFileIdentifier targetFileIdentifier : targetFileIdentifiers) {
      // xxx.inner or xxx.cross
      File tmpTargetFile = targetFileIdentifier.getFileFromDataDirs();
      // xxx.tsfile
      File targetFile =
          getFileFromDataDirs(
              targetFileIdentifier
                  .getFilePath()
                  .replace(
                      isInnerSpace
                          ? IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX
                          : IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX,
                      TsFileConstant.TSFILE_SUFFIX));
      TsFileResource targetResource = null;
      if (tmpTargetFile != null) {
        targetResource = new TsFileResource(tmpTargetFile);
      } else if (targetFile != null) {
        targetResource = new TsFileResource(targetFile);
      }

      if (targetResource != null && !targetResource.remove()) {
        // failed to remove tmp target tsfile
        // system should not carry out the subsequent compaction in case of data redundant
        LOGGER.error(
            "{} [Compaction][Recover] failed to remove target file {}",
            fullStorageGroupName,
            targetResource);
        return false;
      }
    }

    // delete compaction mods files
    List<TsFileResource> sourceTsFileResourceList = new ArrayList<>();
    for (TsFileIdentifier sourceFileIdentifier : sourceFileIdentifiers) {
      sourceTsFileResourceList.add(new TsFileResource(sourceFileIdentifier.getFileFromDataDirs()));
    }
    try {
      CompactionUtils.deleteCompactionModsFile(sourceTsFileResourceList, Collections.emptyList());
    } catch (IOException e) {
      LOGGER.error(
          "{} [Compaction][Recover] Exception occurs while deleting compaction mods file",
          fullStorageGroupName,
          e);
      return false;
    }
    return true;
  }

  /**
   * Some source files lost: delete remaining source files, including: tsfile, resource file, mods
   * file and compaction mods file.
   *
   * @throws IOException the io operations on file fails
   */
  private boolean handleWithSomeSourceFilesLost(
      List<TsFileIdentifier> targetFileIdentifiers,
      List<TsFileIdentifier> deletedTargetFileIdentifiers,
      List<TsFileIdentifier> sourceFileIdentifiers)
      throws IOException {
    // some source files have been deleted, while target file must exist and complete.
    if (!checkIsTargetFilesComplete(targetFileIdentifiers, deletedTargetFileIdentifiers)) {
      return false;
    }

    boolean handleSuccess = true;
    for (TsFileIdentifier sourceFileIdentifier : sourceFileIdentifiers) {
      if (!deleteFile(sourceFileIdentifier)) {
        handleSuccess = false;
      }
    }
    return handleSuccess;
  }

  /**
   * This method find the File object of given filePath by searching it in every data directory. If
   * the file is not found, it will return null.
   */
  private File getFileFromDataDirs(String filePath) {
    String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getLocalDataDirs();
    for (String dataDir : dataDirs) {
      File f = new File(dataDir, filePath);
      if (f.exists()) {
        return f;
      }
    }
    return null;
  }

  private boolean checkIsTargetFilesComplete(
      List<TsFileIdentifier> targetFileIdentifiers,
      List<TsFileIdentifier> deletedTargetFileIdentifiers)
      throws IOException {
    for (TsFileIdentifier targetFileIdentifier : targetFileIdentifiers) {
      targetFileIdentifier.setFilename(
          targetFileIdentifier
              .getFilename()
              .replace(
                  isInnerSpace
                      ? IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX
                      : IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX,
                  TsFileConstant.TSFILE_SUFFIX));
      boolean isTargetFileDeleted = deletedTargetFileIdentifiers.contains(targetFileIdentifier);
      if (isTargetFileDeleted) {
        if (!deleteFile(targetFileIdentifier)) {
          return false;
        }
        continue;
      }
      // xxx.tsfile
      File targetFile = getFileFromDataDirs(targetFileIdentifier.getFilePath());
      if (targetFile == null
          || !TsFileUtils.isTsFileComplete(new TsFileResource(targetFile).getTsFile())) {
        LOGGER.error(
            "{} [Compaction][ExceptionHandler] target file {} is not complete, "
                + "and some source files is lost, do nothing.",
            fullStorageGroupName,
            targetFileIdentifier.getFilePath());
        return false;
      }
    }
    return true;
  }

  /**
   * Delete tsfile and its corresponding files, including resource file, mods file and compaction
   * mods file. Return true if the file is not existed or if the file is existed and has been
   * deleted correctly. Otherwise, return false.
   */
  private boolean deleteFile(TsFileIdentifier tsFileIdentifier) {
    boolean success = true;
    // delete tsfile
    File tsFile = tsFileIdentifier.getFileFromDataDirs();
    if (!checkAndDeleteFile(tsFile)) {
      success = false;
    }

    // delete resource file
    File resourceFile =
        getFileFromDataDirs(tsFileIdentifier.getFilePath() + TsFileResource.RESOURCE_SUFFIX);
    if (!checkAndDeleteFile(resourceFile)) {
      success = false;
    }

    // delete mods file
    File exclusiveModFile =
        getFileFromDataDirs(
            ModificationFile.getExclusiveMods(new File(tsFileIdentifier.getFilePath())).getPath());
    if (!checkAndDeleteFile(exclusiveModFile)) {
      success = false;
    }

    // delete compaction mods file
    File compactionModFile =
        getFileFromDataDirs(
            ModificationFile.getCompactionMods(new File(tsFileIdentifier.getFilePath())).getPath());
    if (!checkAndDeleteFile(compactionModFile)) {
      success = false;
    }

    File oldModFile =
        getFileFromDataDirs(
            ModificationFileV1.getNormalMods(new File(tsFileIdentifier.getFilePath())).getPath());
    if (!checkAndDeleteFile(oldModFile)) {
      success = false;
    }

    File oldCompactionModFile =
        getFileFromDataDirs(
            ModificationFileV1.getCompactionMods(new File(tsFileIdentifier.getFilePath()))
                .getPath());
    if (!checkAndDeleteFile(oldCompactionModFile)) {
      success = false;
    }

    return success;
  }

  /**
   * Return true if the file is not existed or if the file is existed and has been deleted
   * correctly. Otherwise, return false.
   */
  private boolean checkAndDeleteFile(File file) {
    if (file == null || !file.exists()) {
      return true;
    }
    try {
      Files.delete(file.toPath());
    } catch (IOException e) {
      LOGGER.error(
          "{} [Compaction][Recover] failed to remove file {}, exception: {}",
          fullStorageGroupName,
          file,
          e);
      return false;
    }
    return true;
  }
}
