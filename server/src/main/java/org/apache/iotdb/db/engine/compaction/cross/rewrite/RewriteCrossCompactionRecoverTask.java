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
package org.apache.iotdb.db.engine.compaction.cross.rewrite;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.TsFileIdentifier;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogAnalyzer;
import org.apache.iotdb.db.engine.compaction.inner.utils.InnerSpaceCompactionUtils;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RewriteCrossCompactionRecoverTask extends RewriteCrossSpaceCompactionTask {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(RewriteCrossCompactionRecoverTask.class);
  private File compactionLogFile;
  private String dataDir;
  private String logicalStorageGroupName;
  private String virtualStorageGroup;

  public RewriteCrossCompactionRecoverTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartitionId,
      String storageGroupDir,
      TsFileResourceList selectedSeqTsFileResourceList,
      TsFileResourceList selectedUnSeqTsFileResourceList,
      File logFile,
      AtomicInteger currentTaskNum,
      TsFileManager tsFileManager) {
    super(
        logicalStorageGroupName,
        virtualStorageGroupName,
        timePartitionId,
        storageGroupDir,
        tsFileManager,
        null,
        null,
        selectedSeqTsFileResourceList,
        selectedUnSeqTsFileResourceList,
        currentTaskNum);
    this.compactionLogFile = logFile;
    this.dataDir = storageGroupDir;
    this.logicalStorageGroupName = logicalStorageGroupName;
    this.virtualStorageGroup = virtualStorageGroupName;
  }

  @Override
  public void doCompaction() {
    boolean handleSuccess = true;
    LOGGER.info(
        "{} [Compaction][Recover] cross space compaction log is {}",
        fullStorageGroupName,
        compactionLogFile);
    try {
      if (compactionLogFile.exists()) {
        LOGGER.info(
            "{}-{} [Compaction][Recover] cross space compaction log file {} exists, start to recover it",
            logicalStorageGroupName,
            virtualStorageGroup,
            compactionLogFile);
        RewriteCrossSpaceCompactionLogAnalyzer logAnalyzer =
            new RewriteCrossSpaceCompactionLogAnalyzer(compactionLogFile);
        logAnalyzer.analyze();
        List<TsFileIdentifier> sourceFileIdentifiers = logAnalyzer.getSourceFileInfos();
        List<TsFileIdentifier> targetFileIdentifiers = logAnalyzer.getTargetFileInfos();

        // compaction log file is incomplete
        if (targetFileIdentifiers.isEmpty() || sourceFileIdentifiers.isEmpty()) {
          LOGGER.info(
              "{}-{} [Compaction][Recover] incomplete log file, abort recover",
              logicalStorageGroupName,
              virtualStorageGroup);
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
          handleSuccess =
              handleWithAllSourceFilesExist(
                  logAnalyzer, targetFileIdentifiers, sourceFileIdentifiers, fullStorageGroupName);
        } else {
          handleSuccess = handleWithoutAllSourceFilesExist(sourceFileIdentifiers);
        }
      }
    } catch (IOException e) {
      LOGGER.error("recover cross space compaction error", e);
    } finally {
      if (!handleSuccess) {
        LOGGER.error(
            "{} [Compaction][Recover] Failed to recover cross space compaction, set allowCompaction to false",
            fullStorageGroupName);
        tsFileManager.setAllowCompaction(false);
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
                "{} [Compaction][Recover] Exception occurs while deleting log file {}, set allowCompaction to false",
                fullStorageGroupName,
                compactionLogFile,
                e);
            tsFileManager.setAllowCompaction(false);
          }
        }
      }
    }
  }

  private boolean handleWithAllSourceFilesExist(
      RewriteCrossSpaceCompactionLogAnalyzer analyzer,
      List<TsFileIdentifier> targetFileIdentifiers,
      List<TsFileIdentifier> sourceFileIdentifiers,
      String fullStorageGroupName) {
    // all source files exist, delete all the target files and tmp target files
    LOGGER.info(
        "{} [Compaction][Recover] all source files exists, delete all target files.",
        fullStorageGroupName);
    List<TsFileResource> sourceTsFileResourceList = new ArrayList<>();
    for (TsFileIdentifier sourceFileIdentifier : sourceFileIdentifiers) {
      sourceTsFileResourceList.add(new TsFileResource(sourceFileIdentifier.getFileFromDataDirs()));
    }
    for (TsFileIdentifier targetFileIdentifier : targetFileIdentifiers) {
      // xxx.merge
      File tmpTargetFile = targetFileIdentifier.getFileFromDataDirs();
      // xxx.tsfile
      File targetFile =
          getFileFromDataDirs(
              targetFileIdentifier
                  .getFilePath()
                  .replace(
                      IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX,
                      TsFileConstant.TSFILE_SUFFIX));
      TsFileResource targetResource;
      if (tmpTargetFile != null) {
        targetResource = new TsFileResource(tmpTargetFile);
      } else {
        targetResource = new TsFileResource(targetFile);
      }

      if (!targetResource.remove()) {
        // failed to remove tmp target tsfile
        // system should not carry out the subsequent compaction in case of data redundant
        LOGGER.warn(
            "{} [Compaction][Recover] failed to remove target file {}",
            fullStorageGroupName,
            targetResource);
        return false;
      }
    }
    // deal with compaction modification
    try {
      InnerSpaceCompactionUtils.appendNewModificationsToOldModsFile(sourceTsFileResourceList);
    } catch (Throwable e) {
      LOGGER.error(
          "{} Exception occurs while handling exception, set allowCompaction to false",
          fullStorageGroupName,
          e);
      return false;
    }
    return true;
  }

  private boolean handleWithoutAllSourceFilesExist(List<TsFileIdentifier> sourceFileIdentifiers) {
    // some source files have been deleted, while target file must exist.
    boolean handleSuccess = true;
    List<TsFileResource> remainSourceTsFileResources = new ArrayList<>();
    for (TsFileIdentifier sourceFileIdentifier : sourceFileIdentifiers) {
      File sourceFile = sourceFileIdentifier.getFileFromDataDirs();
      if (sourceFile != null) {
        remainSourceTsFileResources.add(new TsFileResource(sourceFile));
      }
      // delete .compaction.mods file and .mods file of all source files
      File compactionModFile =
          getFileFromDataDirs(
              sourceFileIdentifier.getFilePath() + ModificationFile.COMPACTION_FILE_SUFFIX);
      File modFile =
          getFileFromDataDirs(sourceFileIdentifier.getFilePath() + ModificationFile.FILE_SUFFIX);
      if (compactionModFile != null && !compactionModFile.delete()) {
        LOGGER.error(
            "{}-{} [Compaction][Recover] fail to delete target file {}, this may cause data incorrectness",
            logicalStorageGroupName,
            virtualStorageGroup,
            compactionModFile);
        handleSuccess = false;
      }
      if (modFile != null && !modFile.delete()) {
        LOGGER.error(
            "{}-{} [Compaction][Recover] fail to delete target file {}, this may cause data incorrectness",
            logicalStorageGroupName,
            virtualStorageGroup,
            modFile);
        handleSuccess = false;
      }
    }
    // delete remaining source files
    if (!InnerSpaceCompactionUtils.deleteTsFilesInDisk(
        remainSourceTsFileResources, fullStorageGroupName)) {
      LOGGER.error(
          "{}-{} [Compaction][Recover] fail to delete remaining source files.",
          logicalStorageGroupName,
          virtualStorageGroup);
      handleSuccess = false;
    }
    return handleSuccess;
  }

  /**
   * This method find the File object of given filePath by searching it in every data directory. If
   * the file is not found, it will return null.
   */
  private File getFileFromDataDirs(String filePath) {
    String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
    for (String dataDir : dataDirs) {
      File f = new File(dataDir, filePath);
      if (f.exists()) {
        return f;
      }
    }
    return null;
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask other) {
    if (other instanceof RewriteCrossCompactionRecoverTask) {
      return compactionLogFile.equals(
          ((RewriteCrossCompactionRecoverTask) other).compactionLogFile);
    }
    return false;
  }

  @Override
  public boolean checkValidAndSetMerging() {
    return compactionLogFile.exists();
  }
}
