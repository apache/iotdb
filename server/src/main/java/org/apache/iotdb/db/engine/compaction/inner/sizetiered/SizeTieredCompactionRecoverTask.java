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

import org.apache.iotdb.db.engine.compaction.TsFileIdentifier;
import org.apache.iotdb.db.engine.compaction.inner.utils.InnerSpaceCompactionUtils;
import org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogAnalyzer;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

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
   * Clear unfinished compaction task, there are several situations:
   *
   * <ol>
   *   <li><b>Target file is uncompleted</b>: delete the target file and compaction log.
   *   <li><b>Target file is completed, not all source files have been deleted</b>: delete the
   *       source files and compaction logs
   *   <li><b>Target file is completed, all source files have been deleted, compaction log file
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
        if (targetFileIdentifier == null || sourceFileIdentifiers.isEmpty()) {
          LOGGER.info(
              "{}-{} [Compaction][Recover] incomplete log file, abort recover",
              logicalStorageGroupName,
              virtualStorageGroup);
          return;
        }
        File targetFile = targetFileIdentifier.getFileFromDataDirs();
        if (targetFile == null) {
          // cannot find target file from data dirs
          LOGGER.info(
              "{}-{} [Compaction][Recover] cannot find target file {} from data dirs, abort recover",
              logicalStorageGroupName,
              virtualStorageGroup,
              targetFileIdentifier);
          return;
        }
        File resourceFile = new File(targetFile.getPath() + ".resource");

        RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(targetFile, false);
        if (writer.hasCrashed()) {
          LOGGER.info(
              "{}-{} [Compaction][Recover] target file {} crash, start to delete it",
              logicalStorageGroupName,
              virtualStorageGroup,
              targetFile);
          // the target tsfile is crashed, it is not completed
          writer.close();
          if (!targetFile.delete()) {
            LOGGER.error(
                "{}-{} [Compaction][Recover] fail to delete target file {}, this may cause data incorrectness",
                logicalStorageGroupName,
                virtualStorageGroup,
                targetFile);
          }
          if (resourceFile.exists() && !resourceFile.delete()) {
            LOGGER.error(
                "{}-{} [Compaction][Recover] fail to delete target file {}, this may cause data incorrectness",
                logicalStorageGroupName,
                virtualStorageGroup,
                resourceFile);
          }
        } else {
          // the target tsfile is completed
          LOGGER.info(
              "{}-{} [Compaction][Recover] target file {} is completed, delete source files {}",
              logicalStorageGroupName,
              virtualStorageGroup,
              targetFile,
              sourceFileIdentifiers);
          TsFileResource targetResource = new TsFileResource(targetFile);
          List<TsFileResource> sourceTsFileResources = new ArrayList<>();
          for (TsFileIdentifier sourceFileIdentifier : sourceFileIdentifiers) {
            File sourceFile = sourceFileIdentifier.getFileFromDataDirs();
            if (sourceFile != null) {
              sourceTsFileResources.add(new TsFileResource(sourceFile));
            }
          }

          InnerSpaceCompactionUtils.deleteTsFilesInDisk(
              sourceTsFileResources, fullStorageGroupName);
          combineModsInCompaction(sourceTsFileResources, targetResource);
        }
      }
    } catch (IOException e) {
      LOGGER.error("recover inner space compaction error", e);
    } finally {
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
}
