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
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceManager;

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
  protected String storageGroupDir;
  protected TsFileResourceList tsFileResourceList;
  protected List<TsFileResource> recoverTsFileResources;

  public SizeTiredCompactionRecoverTask(
      String logicalStorageGroupName,
      String virtualStorageGroup,
      long timePartition,
      TsFileResourceManager tsFileResourceManager,
      File compactionLogFile,
      String storageGroupDir,
      TsFileResourceList tsFileResourceList,
      List<TsFileResource> recoverTsFileResources,
      boolean sequence,
      AtomicInteger currentTaskNum) {
    super(
        logicalStorageGroupName,
        virtualStorageGroup,
        timePartition,
        tsFileResourceManager,
        tsFileResourceList,
        null,
        sequence,
        currentTaskNum);
    this.compactionLogFile = compactionLogFile;
    this.storageGroupDir = storageGroupDir;
    this.tsFileResourceList = tsFileResourceList;
    this.recoverTsFileResources = recoverTsFileResources;
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
        String targetFile = logAnalyzer.getTargetFile();
        if (targetFile == null || sourceFileList.isEmpty()) {
          return;
        }

        TsFileResource targetResource = getRecoverTsFileResource(targetFile);
        if (targetResource != null) {
          // the target resource is in the recover list, it is not completed
          targetResource.remove();
        } else if ((targetResource = getSourceTsFile(targetFile)) != null) {
          // the target resource is in the tsfile list, it is completed
          List<TsFileResource> sourceTsFileResources = new ArrayList<>();
          tsFileResourceList.writeLock();
          try {
            for (String file : sourceFileList) {
              // get tsfile resource from list, as they have been recovered in StorageGroupProcessor
              TsFileResource resource = getSourceTsFile(file);
              if (resource == null) {
                // source file is not in tsfile list, it is removed
                continue;
              }
              tsFileResourceList.remove(resource);
              sourceTsFileResources.add(resource);
            }
          } finally {
            tsFileResourceList.writeUnlock();
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

  private TsFileResource getRecoverTsFileResource(String filePath) throws IOException {
    for (TsFileResource tsFileResource : recoverTsFileResources) {
      if (Files.isSameFile(tsFileResource.getTsFile().toPath(), new File(filePath).toPath())) {
        return tsFileResource;
      }
    }
    return null;
  }

  private TsFileResource getSourceTsFile(String filename) {
    tsFileResourceList.readLock();
    try {
      File fileToGet = new File(filename);
      for (TsFileResource resource : tsFileResourceList) {
        if (Files.isSameFile(resource.getTsFile().toPath(), fileToGet.toPath())) {
          return resource;
        }
      }
      LOGGER.error("cannot get tsfile resource path: {}", filename);
      return null;
    } catch (IOException e) {
      LOGGER.error("cannot get tsfile resource path: {}", filename);
      return null;
    } finally {
      tsFileResourceList.readUnlock();
    }
  }
}
