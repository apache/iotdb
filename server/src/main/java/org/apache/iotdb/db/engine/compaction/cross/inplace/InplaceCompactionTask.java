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
package org.apache.iotdb.db.engine.compaction.cross.inplace;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.cross.AbstractCrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.cross.inplace.recover.InplaceCompactionLogger;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.WriteLockFailedException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.rescon.TsFileResourceManager;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.engine.compaction.cross.inplace.recover.InplaceCompactionLogger.MAGIC_STRING;
import static org.apache.iotdb.db.engine.compaction.cross.inplace.recover.InplaceCompactionLogger.STR_SEQ_FILES;
import static org.apache.iotdb.db.engine.compaction.cross.inplace.recover.InplaceCompactionLogger.STR_TARGET_FILES;
import static org.apache.iotdb.db.engine.compaction.cross.inplace.recover.InplaceCompactionLogger.STR_UNSEQ_FILES;

public class InplaceCompactionTask extends AbstractCrossSpaceCompactionTask {

  private static final Logger logger = LoggerFactory.getLogger("COMPACTION");
  protected String storageGroupDir;
  protected List<TsFileResource> selectedSeqTsFileResourceList;
  protected List<TsFileResource> selectedUnSeqTsFileResourceList;
  protected String logicalStorageGroupName;
  protected String virtualStorageGroupName;
  private TsFileManager tsFileManager;
  private TsFileResourceList seqTsFileResourceList;
  private TsFileResourceList unseqTsFileResourceList;

  public static final String MERGE_SUFFIX = ".merge";
  private List<TsFileResource> targetTsfileResourceList;
  private List<TsFileResource> holdReadLockList = new ArrayList<>();
  private List<TsFileResource> holdWriteLockList = new ArrayList<>();
  private boolean getWriteLockOfManager = false;
  private final long ACQUIRE_WRITE_LOCK_TIMEOUT =
      IoTDBDescriptor.getInstance().getConfig().getCompactionAcquireWriteLockTimeout();

  String storageGroupName;
  InplaceCompactionLogger inplaceCompactionLogger;
  String taskName;
  States states = States.START;

  public InplaceCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartitionId,
      String storageGroupDir,
      TsFileManager tsFileManager,
      TsFileResourceList seqTsFileResourceList,
      TsFileResourceList unseqTsFileResourceList,
      List<TsFileResource> selectedSeqTsFileResourceList,
      List<TsFileResource> selectedUnSeqTsFileResourceList,
      AtomicInteger currentTaskNum) {
    super(
        logicalStorageGroupName + "-" + virtualStorageGroupName,
        timePartitionId,
        currentTaskNum,
        selectedSeqTsFileResourceList,
        selectedUnSeqTsFileResourceList);
    this.logicalStorageGroupName = logicalStorageGroupName;
    this.virtualStorageGroupName = virtualStorageGroupName;
    this.storageGroupDir = storageGroupDir;
    this.selectedSeqTsFileResourceList = selectedSeqTsFileResourceList;
    this.selectedUnSeqTsFileResourceList = selectedUnSeqTsFileResourceList;
    this.tsFileManager = tsFileManager;
    this.seqTsFileResourceList = seqTsFileResourceList;
    this.unseqTsFileResourceList = unseqTsFileResourceList;
    taskName = fullStorageGroupName + "-" + System.currentTimeMillis();
  }

  @Override
  protected void doCompaction() throws Exception {
    try {
      startCompaction();
    } catch (Exception e) {
      logger.error("Runtime exception in merge {}", taskName, e);
      abort();
    } finally {
      releaseAllLock();
      resetCompactionStatus();
      if (getWriteLockOfManager) {
        tsFileManager.writeUnlock();
      }
    }
  }

  private void abort() throws IOException {
    states = States.ABORTED;
    cleanUp();
  }

  private void startCompaction()
      throws IOException, StorageEngineException, MetadataException, InterruptedException,
          WriteProcessException {
    long startTime = System.currentTimeMillis();
    targetTsfileResourceList =
        TsFileNameGenerator.getCrossCompactionTargetFileResources(selectedSeqTsFileResourceList);
    if (targetTsfileResourceList.isEmpty()
        && selectedSeqTsFileResourceList.isEmpty()
        && selectedUnSeqTsFileResourceList.isEmpty()) {
      return;
    }

    logger.info("{}-crossSpaceCompactionTask start.", storageGroupName);
    inplaceCompactionLogger = new InplaceCompactionLogger(storageGroupDir);
    // print the path of the temporary file first for priority check during recovery
    inplaceCompactionLogger.logFiles(targetTsfileResourceList, STR_TARGET_FILES);
    inplaceCompactionLogger.logFiles(selectedSeqTsFileResourceList, STR_SEQ_FILES);
    inplaceCompactionLogger.logFiles(selectedUnSeqTsFileResourceList, STR_UNSEQ_FILES);
    states = States.COMPACTION;
    CompactionUtils.compact(
        selectedSeqTsFileResourceList,
        selectedUnSeqTsFileResourceList,
        targetTsfileResourceList,
        storageGroupName);
    // indicates that the merge is complete and needs to be cleared
    // the result can be reused during a restart recovery
    inplaceCompactionLogger.logStringInfo(MAGIC_STRING);

    states = States.CLEAN_UP;
    CompactionUtils.moveToTargetFile(targetTsfileResourceList, false, storageGroupName);

    releaseReadAndLockWrite(selectedSeqTsFileResourceList);
    releaseReadAndLockWrite(selectedUnSeqTsFileResourceList);

    combineModesFiles();
    try {
      tsFileManager.writeLockWithTimeout("size-tired compaction", ACQUIRE_WRITE_LOCK_TIMEOUT);
      getWriteLockOfManager = true;
    } catch (WriteLockFailedException e) {
      // if current compaction thread couldn't get write lock
      // a WriteLockFailException will be thrown, then terminate the thread itself
      logger.warn(
          "{} [CrossSpaceCompactionTask] failed to get write lock, abort the task.",
          fullStorageGroupName,
          e);
      throw new InterruptedException(
          String.format(
              "%s [Compaction] compaction abort because cannot acquire write lock",
              fullStorageGroupName));
    }

    deleteOldFiles(selectedSeqTsFileResourceList);
    deleteOldFiles(selectedUnSeqTsFileResourceList);
    removeMergingModification();

    updateTsfileResource();
    cleanUp();
    logger.info(
        "{}-crossSpaceCompactionTask Costs {} s",
        storageGroupName,
        (System.currentTimeMillis() - startTime) / 1000);
  }

  private void updateTsfileResource() {
    for (TsFileResource resource : selectedSeqTsFileResourceList) {
      TsFileResourceManager.getInstance().removeTsFileResource(resource);
    }
    for (TsFileResource resource : selectedUnSeqTsFileResourceList) {
      TsFileResourceManager.getInstance().removeTsFileResource(resource);
    }
    for (TsFileResource targetTsFileResource : targetTsfileResourceList) {
      seqTsFileResourceList.insertBefore(
          selectedSeqTsFileResourceList.get(0), targetTsFileResource);
      TsFileResourceManager.getInstance().registerSealedTsFileResource(targetTsFileResource);
    }
    for (TsFileResource resource : selectedSeqTsFileResourceList) {
      seqTsFileResourceList.remove(resource);
    }
    for (TsFileResource resource : selectedUnSeqTsFileResourceList) {
      unseqTsFileResourceList.remove(resource);
    }
  }

  private void combineModesFiles() throws IOException {
    Map<String, TsFileResource> seqFileInfoMap = new HashMap<>();
    for (TsFileResource tsFileResource : selectedSeqTsFileResourceList) {
      seqFileInfoMap.put(
          TsFileNameGenerator.increaseCrossCompactionCnt(tsFileResource.getTsFile()).getName(),
          tsFileResource);
    }
    for (TsFileResource tsFileResource : targetTsfileResourceList) {
      updateMergeModification(
          tsFileResource,
          seqFileInfoMap.get(tsFileResource.getTsFile().getName()),
          selectedUnSeqTsFileResourceList);
    }
  }

  private boolean addReadLock(List<TsFileResource> tsFileResourceList) {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      tsFileResource.readLock();
      holdReadLockList.add(tsFileResource);
      if (tsFileResource.isMerging() | !tsFileResource.isClosed()
          || !tsFileResource.getTsFile().exists()
          || tsFileResource.isDeleted()) {
        releaseAllLock();
        return false;
      }
    }
    for (TsFileResource tsFileResource : tsFileResourceList) {
      tsFileResource.setMerging(true);
    }
    return true;
  }

  @Override
  public boolean checkValidAndSetMerging() {
    return addReadLock(selectedSeqTsFileResourceList)
        && addReadLock(selectedUnSeqTsFileResourceList);
  }

  private void releaseReadAndLockWrite(List<TsFileResource> tsFileResourceList) {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      tsFileResource.readUnlock();
      holdReadLockList.remove(tsFileResource);
      tsFileResource.writeLock();
      holdWriteLockList.add(tsFileResource);
    }
  }

  private void releaseAllLock() {
    for (TsFileResource tsFileResource : holdReadLockList) {
      tsFileResource.readUnlock();
    }
    holdReadLockList.clear();
    for (TsFileResource tsFileResource : holdWriteLockList) {
      tsFileResource.writeUnlock();
    }
    holdWriteLockList.clear();
  }

  private void resetCompactionStatus() {
    for (TsFileResource tsFileResource : selectedSeqTsFileResourceList) {
      tsFileResource.setMerging(false);
    }
    for (TsFileResource tsFileResource : selectedUnSeqTsFileResourceList) {
      tsFileResource.setMerging(false);
    }
  }

  void deleteOldFiles(List<TsFileResource> tsFileResourceList) throws IOException {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFilePath());
      tsFileResource.setDeleted(true);
      tsFileResource.delete();
      logger.info(
          "[CrossSpaceCompaction] Delete TsFile :{}.",
          tsFileResource.getTsFile().getAbsolutePath());
    }
  }

  void cleanUp() throws IOException {
    logger.info("{} is cleaning up", taskName);
    for (TsFileResource tsFileResource : targetTsfileResourceList) {
      tsFileResource.getTsFile().delete();
      tsFileResource.setMerging(false);
    }
    for (TsFileResource unseqFile : selectedUnSeqTsFileResourceList) {
      unseqFile.setMerging(false);
    }
    if (inplaceCompactionLogger != null) {
      inplaceCompactionLogger.close();
    }
    File logFile = new File(storageGroupDir, InplaceCompactionLogger.MERGE_LOG_NAME);
    logFile.delete();
  }

  public String getStorageGroupName() {
    return storageGroupName;
  }

  public String getProgress() {
    switch (states) {
      case ABORTED:
        return "Aborted";
      case CLEAN_UP:
        return "Cleaning up";
      case COMPACTION:
        return "Compaction";
      case START:
      default:
        return "Just started";
    }
  }

  public String getTaskName() {
    return taskName;
  }

  enum States {
    START,
    COMPACTION,
    CLEAN_UP,
    ABORTED
  }

  private void removeMergingModification() {
    try {
      for (TsFileResource seqFile : selectedSeqTsFileResourceList) {
        ModificationFile.getCompactionMods(seqFile).remove();
      }
      for (TsFileResource unseqFile : selectedUnSeqTsFileResourceList) {
        ModificationFile.getCompactionMods(unseqFile).remove();
      }
      for (TsFileResource targetFile : targetTsfileResourceList) {
        ModificationFile.getCompactionMods(targetFile).remove();
      }
    } catch (IOException e) {
      logger.error("{} cannot remove merging modification ", fullStorageGroupName, e);
    }
  }

  private void updateMergeModification(
      TsFileResource targetFile, TsFileResource seqFile, List<TsFileResource> unseqFiles) {
    try {
      // remove old modifications and write modifications generated during merge
      targetFile.removeModFile();
      ModificationFile compactionModificationFile = ModificationFile.getCompactionMods(targetFile);
      for (Modification modification : compactionModificationFile.getModifications()) {
        targetFile.getModFile().write(modification);
      }
      // write mods in the seq file
      if (seqFile != null) {
        ModificationFile seqCompactionModificationFile =
            ModificationFile.getCompactionMods(seqFile);
        for (Modification modification : seqCompactionModificationFile.getModifications()) {
          targetFile.getModFile().write(modification);
        }
      }
      // write mods in all un-seq files
      for (TsFileResource unseqFile : unseqFiles) {
        ModificationFile compactionUnseqModificationFile =
            ModificationFile.getCompactionMods(unseqFile);
        for (Modification modification : compactionUnseqModificationFile.getModifications()) {
          targetFile.getModFile().write(modification);
        }
      }
      try {
        targetFile.getModFile().close();
      } catch (IOException e) {
        logger.error(
            "Cannot close the ModificationFile {}", targetFile.getModFile().getFilePath(), e);
      }
    } catch (IOException e) {
      logger.error(
          "{} cannot clean the ModificationFile of {} after cross space merge",
          fullStorageGroupName,
          targetFile.getTsFile(),
          e);
    }
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask other) {
    if (other instanceof InplaceCompactionTask) {
      InplaceCompactionTask otherTask = (InplaceCompactionTask) other;
      if (!otherTask.selectedSeqTsFileResourceList.equals(selectedSeqTsFileResourceList)
          || !otherTask.selectedUnSeqTsFileResourceList.equals(selectedUnSeqTsFileResourceList)) {
        return false;
      }
      return true;
    }
    return false;
  }
}
