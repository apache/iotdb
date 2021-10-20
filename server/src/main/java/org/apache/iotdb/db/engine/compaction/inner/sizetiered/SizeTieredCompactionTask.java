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

import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.inner.utils.InnerSpaceCompactionUtils;
import org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.exception.WriteLockFailedException;
import org.apache.iotdb.db.rescon.TsFileResourceManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger.SOURCE_NAME;
import static org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger.TARGET_NAME;

/**
 * SizeTiredCompactionTask compact several inner space files selected by {@link
 * SizeTieredCompactionSelector} into one file.
 */
public class SizeTieredCompactionTask extends AbstractInnerSpaceCompactionTask {
  private static final Logger LOGGER = LoggerFactory.getLogger("COMPACTION");
  protected TsFileResourceList tsFileResourceList;
  protected TsFileManager tsFileManager;

  public SizeTieredCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartition,
      TsFileManager tsFileManager,
      TsFileResourceList tsFileResourceList,
      List<TsFileResource> selectedTsFileResourceList,
      boolean sequence,
      AtomicInteger currentTaskNum) {
    super(
        logicalStorageGroupName + "-" + virtualStorageGroupName,
        timePartition,
        currentTaskNum,
        sequence,
        selectedTsFileResourceList);
    this.tsFileResourceList = tsFileResourceList;
    this.tsFileManager = tsFileManager;
  }

  public static void combineModsInCompaction(
      Collection<TsFileResource> mergeTsFiles, TsFileResource targetTsFile) throws IOException {
    List<Modification> modifications = new ArrayList<>();
    for (TsFileResource mergeTsFile : mergeTsFiles) {
      try (ModificationFile sourceCompactionModificationFile =
          ModificationFile.getCompactionMods(mergeTsFile)) {
        modifications.addAll(sourceCompactionModificationFile.getModifications());
        if (sourceCompactionModificationFile.exists()) {
          sourceCompactionModificationFile.remove();
        }
      }
      ModificationFile sourceModificationFile = ModificationFile.getNormalMods(mergeTsFile);
      if (sourceModificationFile.exists()) {
        sourceModificationFile.remove();
      }
    }
    if (!modifications.isEmpty()) {
      try (ModificationFile modificationFile = ModificationFile.getNormalMods(targetTsFile)) {
        for (Modification modification : modifications) {
          // we have to set modification offset to MAX_VALUE, as the offset of source chunk may
          // change after compaction
          modification.setFileOffset(Long.MAX_VALUE);
          modificationFile.write(modification);
        }
      }
    }
  }

  @Override
  protected void doCompaction() throws Exception {
    long startTime = System.currentTimeMillis();
    // get resource of target file
    String dataDirectory = selectedTsFileResourceList.get(0).getTsFile().getParent();
    String targetFileName =
        TsFileNameGenerator.getInnerCompactionFileName(selectedTsFileResourceList, sequence)
            .getName();
    TsFileResource targetTsFileResource =
        new TsFileResource(new File(dataDirectory + File.separator + targetFileName));
    LOGGER.info(
        "{} [Compaction] starting compaction task with {} files",
        fullStorageGroupName,
        selectedTsFileResourceList.size());
    File logFile = null;
    try {
      logFile =
          new File(
              dataDirectory
                  + File.separator
                  + targetFileName
                  + SizeTieredCompactionLogger.COMPACTION_LOG_NAME);
      SizeTieredCompactionLogger sizeTieredCompactionLogger =
          new SizeTieredCompactionLogger(logFile.getPath());
      for (TsFileResource resource : selectedTsFileResourceList) {
        sizeTieredCompactionLogger.logFile(SOURCE_NAME, resource.getTsFile());
      }
      sizeTieredCompactionLogger.logSequence(sequence);
      sizeTieredCompactionLogger.logFile(TARGET_NAME, targetTsFileResource.getTsFile());
      LOGGER.info(
          "{} [Compaction] compaction with {}", fullStorageGroupName, selectedTsFileResourceList);
      // carry out the compaction
      InnerSpaceCompactionUtils.compact(
          targetTsFileResource,
          selectedTsFileResourceList,
          fullStorageGroupName,
          sizeTieredCompactionLogger,
          new HashSet<>(),
          sequence);
      LOGGER.info(
          "{} [SizeTiredCompactionTask] compact finish, close the logger", fullStorageGroupName);
      sizeTieredCompactionLogger.close();
    } finally {
      for (TsFileResource resource : selectedTsFileResourceList) {
        resource.setMerging(false);
      }
    }
    LOGGER.info(
        "{} [Compaction] compaction finish, start to delete old files", fullStorageGroupName);
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedException(String.format("%s [Compaction] abort", fullStorageGroupName));
    }
    // get write lock for TsFileResource list with timeout
    try {
      tsFileManager.writeLockWithTimeout("size-tired compaction", 60_000);
    } catch (WriteLockFailedException e) {
      // if current compaction thread couldn't get writelock
      // a WriteLockFailException will be thrown, then terminate the thread itself
      LOGGER.warn(
          "{} [SizeTiredCompactionTask] failed to get write lock, abort the task and delete the target file {}",
          fullStorageGroupName,
          targetTsFileResource.getTsFile(),
          e);
      targetTsFileResource.getTsFile().delete();
      logFile.delete();
      throw new InterruptedException(
          String.format(
              "%s [Compaction] compaction abort because cannot acquire write lock",
              fullStorageGroupName));
    }
    try {
      // replace the old files with new file, the new is in same position as the old
      for (TsFileResource resource : selectedTsFileResourceList) {
        TsFileResourceManager.getInstance().removeTsFileResource(resource);
      }
      tsFileResourceList.insertBefore(selectedTsFileResourceList.get(0), targetTsFileResource);
      TsFileResourceManager.getInstance().registerSealedTsFileResource(targetTsFileResource);
      for (TsFileResource resource : selectedTsFileResourceList) {
        tsFileResourceList.remove(resource);
      }
    } finally {
      tsFileManager.writeUnlock();
    }
    // delete the old files
    InnerSpaceCompactionUtils.deleteTsFilesInDisk(selectedTsFileResourceList, fullStorageGroupName);
    LOGGER.info(
        "{} [SizeTiredCompactionTask] old file deleted, start to rename mods file",
        fullStorageGroupName);
    combineModsInCompaction(selectedTsFileResourceList, targetTsFileResource);
    long costTime = System.currentTimeMillis() - startTime;
    LOGGER.info(
        "{} [SizeTiredCompactionTask] all compaction task finish, target file is {},"
            + "time cost is {} s",
        fullStorageGroupName,
        targetFileName,
        costTime / 1000);
    if (logFile.exists()) {
      logFile.delete();
    }
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask other) {
    if (other instanceof SizeTieredCompactionTask) {
      SizeTieredCompactionTask otherSizeTieredTask = (SizeTieredCompactionTask) other;
      if (!selectedTsFileResourceList.equals(otherSizeTieredTask.selectedTsFileResourceList)) {
        return false;
      }
      return true;
    }
    return false;
  }

  @Override
  public boolean checkValidAndSetMerging() {
    long minVersionNum = Long.MAX_VALUE;
    try {
      for (TsFileResource resource : selectedTsFileResourceList) {
        if (resource.isMerging() | !resource.isClosed() || !resource.getTsFile().exists()) {
          return false;
        }
        TsFileNameGenerator.TsFileName tsFileName =
            TsFileNameGenerator.getTsFileName(resource.getTsFile().getName());
        if (tsFileName.getVersion() < minVersionNum) {
          minVersionNum = tsFileName.getVersion();
        }
      }
    } catch (IOException e) {
      LOGGER.error("CompactionTask exists while check valid", e);
    }
    for (TsFileResource resource : selectedTsFileResourceList) {
      resource.setMerging(true);
    }
    return true;
  }
}
