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
package org.apache.iotdb.db.engine.compaction.inner.sizetired;

import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionRecoverTask;
import org.apache.iotdb.db.engine.compaction.inner.utils.InnerSpaceCompactionUtils;
import org.apache.iotdb.db.engine.compaction.inner.utils.SizeTiredCompactionLogAnalyzer;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SizeTiredCompactionRecoverTask extends SizeTiredCompactionTask {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractInnerSpaceCompactionRecoverTask.class);
  protected File compactionLogFile;
  protected String dataDir;

  public SizeTiredCompactionRecoverTask(
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
  public void doCompaction() {
    // read log -> Set<Device> -> doCompaction -> clear
    try {
      if (compactionLogFile.exists()) {
        SizeTiredCompactionLogAnalyzer logAnalyzer =
            new SizeTiredCompactionLogAnalyzer(compactionLogFile);
        logAnalyzer.analyze();
        List<String> sourceFileList = logAnalyzer.getSourceFiles();
        String targetFileName = logAnalyzer.getTargetFile();
        if (targetFileName == null || sourceFileList.isEmpty()) {
          return;
        }
        File targetFile = new File(targetFileName);
        File resourceFile = new File(targetFileName + ".resource");
        if (!targetFile.exists()) {
          if (resourceFile.exists()) {
            if (!resourceFile.delete()) {
              LOGGER.warn("Fail to delete tsfile resource {}", resourceFile);
            }
          }
          return;
        }

        RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(targetFile, false);
        if (writer.hasCrashed()) {
          // the target tsfile is crashed, it is not completed
          writer.close();
          if (!targetFile.delete()) {
            LOGGER.warn("Fail to delete uncompleted file {}", targetFile);
          }
          if (!resourceFile.delete()) {
            LOGGER.warn("Fail to delete tsfile resource {}", resourceFile);
          }
        } else {
          // the target tsfile is completed
          TsFileResource targetResource = new TsFileResource(targetFile);
          List<TsFileResource> sourceTsFileResources = new ArrayList<>();
          for (String sourceFileName : sourceFileList) {
            File sourceFile = new File(sourceFileName);
            sourceTsFileResources.add(new TsFileResource(sourceFile));
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
        try {
          Files.delete(compactionLogFile.toPath());
        } catch (IOException e) {
          LOGGER.error("delete inner space compaction log file error", e);
        }
      }
    }
  }
}
