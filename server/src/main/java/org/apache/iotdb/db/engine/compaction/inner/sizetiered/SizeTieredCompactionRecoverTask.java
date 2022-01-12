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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.TsFileIdentifier;
import org.apache.iotdb.db.engine.compaction.inner.InnerSpaceCompactionExceptionHandler;
import org.apache.iotdb.db.engine.compaction.inner.utils.InnerSpaceCompactionUtils;
import org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogAnalyzer;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SizeTieredCompactionRecoverTask extends SizeTieredCompactionTask {
  private static final Logger LOGGER = LoggerFactory.getLogger("COMPACTION");
  protected File compactionLogFile;
  protected String dataDir;
  protected String logicalStorageGroupName;
  protected String virtualStorageGroup;

  public SizeTieredCompactionRecoverTask(
      String logicalStorageGroupName,
      String virtualStorageGroup,
      long timePartition,
      File compactionLogFile,
      String dataDir,
      boolean sequence,
      AtomicInteger currentTaskNum) {
    super(
        logicalStorageGroupName,
        virtualStorageGroup,
        timePartition,
        null,
        null,
        new ArrayList<>(),
        sequence,
        currentTaskNum);
    this.compactionLogFile = compactionLogFile;
    this.dataDir = dataDir;
    this.logicalStorageGroupName = logicalStorageGroupName;
    this.virtualStorageGroup = virtualStorageGroup;
  }

  /**
   * We support tmp target file is xxx.target, target file is xxx.tsfile, resource file is
   * xxx.tsfile.resource. To clear unfinished compaction task, there are several situations:
   *
   * <ol>
   *   <li>Compaction log is incomplete, then delete it.
   *   <li>All source files exist, then delete tmp target file, target file, resource file, mods
   *       file of target file and compaction log if exist. Also append new modifications of all
   *       source files to corresponding mods file.
   *   <li>Not all source files exist, then delete the remaining source files, all mods files of
   *       each source file and compaction log.
   * </ol>
   */
  @Override
  public void doCompaction() {
    boolean handleSuccess = true;
    LOGGER.info(
        "{} [Compaction][Recover] compaction log is {}", fullStorageGroupName, compactionLogFile);
    try {
      if (compactionLogFile.exists()) {
        LOGGER.info(
            "{}-{} [Compaction][Recover] compaction log file {} exists, start to recover it",
            logicalStorageGroupName,
            virtualStorageGroup,
            compactionLogFile);
        SizeTieredCompactionLogAnalyzer logAnalyzer =
            new SizeTieredCompactionLogAnalyzer(compactionLogFile);
        logAnalyzer.analyze();
        List<TsFileIdentifier> sourceFileIdentifiers = logAnalyzer.getSourceFileInfos();
        TsFileIdentifier targetFileIdentifier = logAnalyzer.getTargetFileInfo();

        // compaction log file is incomplete
        if (targetFileIdentifier == null || sourceFileIdentifiers.isEmpty()) {
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
          // xxx.target
          File tmpTargetFile = targetFileIdentifier.getFileFromDataDirs();
          // xxx.tsfile
          File targetFile =
              getFileFromDataDirs(
                  targetFileIdentifier
                      .getFilePath()
                      .replace(
                          IoTDBConstant.COMPACTION_TMP_FILE_SUFFIX, TsFileConstant.TSFILE_SUFFIX));
          TsFileResource targetResource;
          if (tmpTargetFile != null) {
            targetResource = new TsFileResource(tmpTargetFile);
          } else {
            targetResource = new TsFileResource(targetFile);
          }
          List<TsFileResource> sourceResources = new ArrayList<>();
          for (TsFileIdentifier sourceFileIdentifier : sourceFileIdentifiers) {
            sourceResources.add(new TsFileResource(sourceFileIdentifier.getFileFromDataDirs()));
          }
          handleSuccess =
              InnerSpaceCompactionExceptionHandler.handleWhenAllSourceFilesExist(
                  fullStorageGroupName, targetResource, sourceResources);
        } else {
          handleSuccess = handleWithoutAllSourceFilesExist(sourceFileIdentifiers);
        }
      }
    } catch (IOException e) {
      LOGGER.error("recover inner space compaction error", e);
    } finally {
      if (!handleSuccess) {
        LOGGER.error(
            "{} [Compaction][Recover] Failed to recover compaction, set allowCompaction to false",
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

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask other) {
    if (other instanceof SizeTieredCompactionRecoverTask) {
      SizeTieredCompactionRecoverTask otherTask = (SizeTieredCompactionRecoverTask) other;
      if (!compactionLogFile.equals(otherTask.compactionLogFile)
          || !dataDir.equals(otherTask.dataDir)) {
        return false;
      }
      return true;
    }
    return false;
  }

  @Override
  public boolean checkValidAndSetMerging() {
    return compactionLogFile.exists();
  }

  private boolean handleWithoutAllSourceFilesExist(List<TsFileIdentifier> sourceFileIdentifiers) {
    // some source files have been deleted, while .tsfile and .tsfile.resource must exist.
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
}
