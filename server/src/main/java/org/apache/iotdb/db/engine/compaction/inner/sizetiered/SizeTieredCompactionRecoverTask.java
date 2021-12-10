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
import org.apache.iotdb.db.engine.compaction.inner.utils.InnerSpaceCompactionUtils;
import org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogAnalyzer;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

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
        null,
        sequence,
        currentTaskNum);
    this.compactionLogFile = compactionLogFile;
    this.dataDir = dataDir;
    this.logicalStorageGroupName = logicalStorageGroupName;
    this.virtualStorageGroup = virtualStorageGroup;
  }

  /**
   * We support tmpTargetFile is xxx.target, tmpTargetResourceFile is xxx.target.resource,
   * targetFile is xxx.tsfile, targetResourceFile is xxx.tsfile.resource. To clear unfinished
   * compaction task, there are several situations:
   *
   * <ol>
   *   <li>Compaction log is incomplete, then delete it and return.
   *   <li>TmpTargetFile exists, then delete targetFile if exists and move tmpTargetFile to
   *       targetFile.
   *   <li>Both tmpTargetFile and targetFile do not exist, then delete tmpTargetResourceFile and
   *       targetResourceFile if exist, also delete the compaction log, return.
   *   <li><b>TargetResourceFile does not exist or targetFile is incomplete</b>: delete the
   *       targetFile, tmpTargetResourceFile and compaction log.
   *   <li><b>TargetFile is completed, not all source files have been deleted</b>: delete the source
   *       files and compaction logs
   *   <li><b>TargetFile is completed, all source files have been deleted, compaction log file
   *       exists</b>: delete the compaction log
   *   <li><b>No compaction log file exists</b>: do nothing
   * </ol>
   */
  @Override
  public void doCompaction() {
    // read log -> Set<Device> -> doCompaction -> clear
    try {
      LOGGER.info(
          "{} [Compaction][Recover] compaction log is {}", fullStorageGroupName, compactionLogFile);
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

        // xxx.target
        File tmpTargetFile = targetFileIdentifier.getFileFromDataDirs();
        // xxx.tsfile
        File targetFile =
            getFileFromDataDirs(
                targetFileIdentifier
                    .getFilePath()
                    .replace(
                        IoTDBConstant.COMPACTION_TMP_FILE_SUFFIX, TsFileConstant.TSFILE_SUFFIX));
        // xxx.tsfile.resource
        File targetResourceFile =
            getFileFromDataDirs(
                targetFileIdentifier
                        .getFilePath()
                        .replace(
                            IoTDBConstant.COMPACTION_TMP_FILE_SUFFIX, TsFileConstant.TSFILE_SUFFIX)
                    + TsFileResource.RESOURCE_SUFFIX);

        // check is all source files existed
        boolean isAllSourcesFileExisted = true;
        List<TsFileResource> sourceTsFileResources = new ArrayList<>();
        for (TsFileIdentifier sourceFileIdentifier : sourceFileIdentifiers) {
          File sourceFile = sourceFileIdentifier.getFileFromDataDirs();
          if (sourceFile == null) {
            isAllSourcesFileExisted = false;
            break;
          }
        }

        if (isAllSourcesFileExisted) {
          // all source files existed
          if (tmpTargetFile != null && !tmpTargetFile.delete()) {
            LOGGER.error(
                "{}-{} [Compaction][Recover] fail to delete target file {}, this may cause data incorrectness",
                logicalStorageGroupName,
                virtualStorageGroup,
                tmpTargetFile);
          }
          if (targetFile != null && !targetFile.delete()) {
            LOGGER.error(
                "{}-{} [Compaction][Recover] fail to delete target file {}, this may cause data incorrectness",
                logicalStorageGroupName,
                virtualStorageGroup,
                targetFile);
          }
          if (targetResourceFile != null && !targetResourceFile.delete()) {
            LOGGER.error(
                "{}-{} [Compaction][Recover] fail to delete target file {}, this may cause data incorrectness",
                logicalStorageGroupName,
                virtualStorageGroup,
                targetResourceFile);
          }
        } else {
          // some source files have been deleted, which means .tsfile and .tsfile.resource exist.
          sourceTsFileResources.clear();
          for (TsFileIdentifier sourceFileIdentifier : sourceFileIdentifiers) {
            File sourceFile = sourceFileIdentifier.getFileFromDataDirs();
            if (sourceFile != null) {
              sourceTsFileResources.add(new TsFileResource(sourceFile));
            }
          }
          TsFileResource targetResource = new TsFileResource(targetFile);
          InnerSpaceCompactionUtils.deleteTsFilesInDisk(
              sourceTsFileResources, fullStorageGroupName);
          combineModsInCompaction(sourceTsFileResources, targetResource);
        }
      }
    } catch (IOException e) {
      LOGGER.error("recover inner space compaction error", e);
    } finally {
      // delete compaction log if exists
      if (compactionLogFile.exists()) {
        if (!compactionLogFile.delete()) {
          LOGGER.warn(
              "{}-{} [Compaction][Recover] fail to delete {}",
              logicalStorageGroupName,
              virtualStorageGroup,
              compactionLogFile);
        } else {
          LOGGER.info(
              "{}-{} [Compaction][Recover] delete compaction log {}",
              logicalStorageGroupName,
              virtualStorageGroup,
              compactionLogFile);
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
