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

package org.apache.iotdb.db.engine.compaction.task;

import org.apache.iotdb.db.engine.compaction.CompactionContext;
import org.apache.iotdb.db.engine.compaction.utils.CompactionLogAnalyzer;
import org.apache.iotdb.db.engine.compaction.utils.CompactionLogger;
import org.apache.iotdb.db.engine.compaction.utils.CompactionUtils;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class InnerSpaceCompactionRecoverTask extends InnerSpaceCompactionTask {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(InnerSpaceCompactionRecoverTask.class);
  protected File compactionLogFile;
  protected String storageGroupDir;
  protected TsFileResourceList tsFileResourceList;

  public InnerSpaceCompactionRecoverTask(CompactionContext context) {
    super(context);
    compactionLogFile = context.getCompactionLogFile();
    storageGroupDir = context.getStorageGroupDir();
    tsFileResourceList =
        context.isSequence()
            ? context.getSequenceFileResourceList()
            : context.getUnsequenceFileResourceList();
  }

  @Override
  public void doCompaction() {
    // read log -> Set<Device> -> doCompaction -> clear
    try {
      if (compactionLogFile.exists()) {
        CompactionLogAnalyzer logAnalyzer = new CompactionLogAnalyzer(compactionLogFile);
        logAnalyzer.analyze();
        Set<String> deviceSet = logAnalyzer.getDeviceSet();
        List<String> sourceFileList = logAnalyzer.getSourceFiles();
        long offset = logAnalyzer.getOffset();
        String targetFile = logAnalyzer.getTargetFile();
        boolean fullMerge = logAnalyzer.isFullMerge();
        boolean isSeq = logAnalyzer.isSeq();
        if (targetFile == null || sourceFileList.isEmpty()) {
          return;
        }
        File target = new File(targetFile);
        if (deviceSet.isEmpty()) {
          // if not in compaction, just delete the target file
          if (target.exists()) {
            Files.delete(target.toPath());
          }
          return;
        }
        if (fullMerge) {
          // get tsfile resource from list, as they have been recovered in StorageGroupProcessor
          TsFileResource targetTsFileResource = new TsFileResource(new File(targetFile));
          long timePartition = targetTsFileResource.getTimePartition();
          RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(target);
          // if not complete compaction, resume merge
          if (writer.hasCrashed()) {
            if (offset > 0) {
              writer.getIOWriterOut().truncate(offset - 1);
            }
            writer.close();
            CompactionLogger compactionLogger =
                new CompactionLogger(storageGroupDir, storageGroupName);
            List<Modification> modifications = new ArrayList<>();
            CompactionUtils.compact(
                targetTsFileResource,
                tsFileResourceList,
                storageGroupName,
                compactionLogger,
                deviceSet,
                isSeq,
                modifications);
            compactionLogger.close();
          } else {
            writer.close();
          }
        } else {
          // get tsfile resource from list, as they have been recovered in StorageGroupProcessor
          TsFileResource targetResource = new TsFileResource(new File(targetFile));
          long timePartition = targetResource.getTimePartition();
          List<TsFileResource> sourceTsFileResources = new ArrayList<>();
          for (String file : sourceFileList) {
            // get tsfile resource from list, as they have been recovered in StorageGroupProcessor
            sourceTsFileResources.add(getSourceTsFile(file));
          }
          RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(target);
          // if not complete compaction, resume merge
          if (writer.hasCrashed()) {
            if (offset > 0) {
              writer.getIOWriterOut().truncate(offset - 1);
            }
            writer.close();
            CompactionLogger compactionLogger =
                new CompactionLogger(storageGroupDir, storageGroupName);
            List<Modification> modifications = new ArrayList<>();
            CompactionUtils.compact(
                targetResource,
                sourceTsFileResources,
                storageGroupName,
                compactionLogger,
                deviceSet,
                isSeq,
                modifications);
            // complete compaction and delete source file
            tsFileResourceList.writeLock();
            try {
              if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException(
                    String.format("%s [Compaction] abort", storageGroupName));
              }
              tsFileResourceList.insertBefore(sourceTsFileResources.get(0), targetResource);
              for (TsFileResource resource : tsFileResourceList) {
                tsFileResourceList.remove(resource);
              }
            } finally {
              tsFileResourceList.writeUnlock();
            }
            CompactionUtils.deleteTsFilesInDisk(sourceTsFileResources, storageGroupName);
            // TODO: Operation with Modification
            // renameLevelFilesMods(modifications, sourceTsFileResources,
            // targetResource);
            compactionLogger.close();
          } else {
            writer.close();
          }
        }
      }
    } catch (IOException | IllegalPathException | InterruptedException e) {
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
