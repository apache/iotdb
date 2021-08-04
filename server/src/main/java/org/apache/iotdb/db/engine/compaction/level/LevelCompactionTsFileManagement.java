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

package org.apache.iotdb.db.engine.compaction.level;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.TsFileManagement;
import org.apache.iotdb.db.engine.compaction.utils.CompactionLogAnalyzer;
import org.apache.iotdb.db.engine.compaction.utils.CompactionLogger;
import org.apache.iotdb.db.engine.compaction.utils.CompactionUtils;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.COMPACTION_LOG_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.SOURCE_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.TARGET_NAME;

/** The TsFileManagement for LEVEL_COMPACTION, use level struct to manage TsFile list */
public class LevelCompactionTsFileManagement extends TsFileManagement {

  private static final Logger logger =
      LoggerFactory.getLogger(LevelCompactionTsFileManagement.class);

  private final int seqLevelNum =
      Math.max(IoTDBDescriptor.getInstance().getConfig().getSeqLevelNum(), 1);
  private final int seqFileNumInEachLevel =
      Math.max(IoTDBDescriptor.getInstance().getConfig().getSeqFileNumInEachLevel(), 1);
  private final int unseqLevelNum =
      Math.max(IoTDBDescriptor.getInstance().getConfig().getUnseqLevelNum(), 1);
  private final int unseqFileNumInEachLevel =
      Math.max(IoTDBDescriptor.getInstance().getConfig().getUnseqFileNumInEachLevel(), 1);

  private final boolean enableUnseqCompaction =
      IoTDBDescriptor.getInstance().getConfig().isEnableUnseqCompaction();

  // First map is partition list; Second list is level list; Third list is file list in level;
  private final Map<Long, List<SortedSet<TsFileResource>>> sequenceTsFileResources =
      new HashMap<>();
  private final Map<Long, List<List<TsFileResource>>> unSequenceTsFileResources = new HashMap<>();
  private final List<List<TsFileResource>> forkedSequenceTsFileResources = new ArrayList<>();
  private final List<List<TsFileResource>> forkedUnSequenceTsFileResources = new ArrayList<>();
  private final List<TsFileResource> sequenceRecoverTsFileResources = new ArrayList<>();
  private final List<TsFileResource> unSequenceRecoverTsFileResources = new ArrayList<>();

  public LevelCompactionTsFileManagement(String storageGroupName, String storageGroupDir) {
    super(storageGroupName, storageGroupDir);
    clear();
  }

  public void renameLevelFilesMods(
      Collection<Modification> filterModification,
      Collection<TsFileResource> mergeTsFiles,
      TsFileResource targetTsFile)
      throws IOException {
    logger.debug("{} [compaction] merge starts to rename real file's mod", storageGroupName);
    List<Modification> modifications = new ArrayList<>();
    for (TsFileResource mergeTsFile : mergeTsFiles) {
      try (ModificationFile sourceModificationFile =
          new ModificationFile(mergeTsFile.getTsFilePath() + ModificationFile.FILE_SUFFIX)) {
        modifications.addAll(sourceModificationFile.getModifications());
        if (sourceModificationFile.exists()) {
          sourceModificationFile.remove();
        }
      }
    }
    modifications.removeAll(filterModification);
    if (!modifications.isEmpty()) {
      try (ModificationFile modificationFile =
          new ModificationFile(targetTsFile.getTsFilePath() + ModificationFile.FILE_SUFFIX)) {
        for (Modification modification : modifications) {
          // we have to set modification offset to MAX_VALUE, as the offset of source chunk may
          // change after compaction
          modification.setFileOffset(Long.MAX_VALUE);
          modificationFile.write(modification);
        }
      }
    }
  }

  private void deleteLevelFilesInDisk(Collection<TsFileResource> mergeTsFiles) {
    logger.debug("{} [compaction] merge starts to delete real file", storageGroupName);
    for (TsFileResource mergeTsFile : mergeTsFiles) {
      deleteLevelFile(mergeTsFile);
      logger.debug(
          "{} [Compaction] delete TsFile {}", storageGroupName, mergeTsFile.getTsFilePath());
    }
  }

  private void deleteLevelFilesInList(
      long timePartitionId, Collection<TsFileResource> mergeTsFiles, int level, boolean sequence) {
    logger.debug("{} [compaction] merge starts to delete file list", storageGroupName);
    if (sequence) {
      if (sequenceTsFileResources.containsKey(timePartitionId)) {
        if (sequenceTsFileResources.get(timePartitionId).size() > level) {
          sequenceTsFileResources.get(timePartitionId).get(level).removeAll(mergeTsFiles);
        }
      }
    } else {
      if (unSequenceTsFileResources.containsKey(timePartitionId)) {
        if (unSequenceTsFileResources.get(timePartitionId).size() > level) {
          unSequenceTsFileResources.get(timePartitionId).get(level).removeAll(mergeTsFiles);
        }
      }
    }
  }

  private void deleteLevelFile(TsFileResource seqFile) {
    seqFile.writeLock();
    try {
      ChunkCache.getInstance().clear();
      TimeSeriesMetadataCache.getInstance().clear();
      FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile.getTsFilePath());
      seqFile.setDeleted(true);
      seqFile.delete();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    } finally {
      seqFile.writeUnlock();
    }
  }

  @Deprecated
  @Override
  public List<TsFileResource> getTsFileList(boolean sequence) {
    readLock();
    try {
      List<TsFileResource> result = new ArrayList<>();
      if (sequence) {
        for (long timePartition : sequenceTsFileResources.keySet()) {
          result.addAll(getTsFileListByTimePartition(true, timePartition));
        }
      } else {
        for (long timePartition : unSequenceTsFileResources.keySet()) {
          result.addAll(getTsFileListByTimePartition(false, timePartition));
        }
      }
      return result;
    } finally {
      readUnLock();
    }
  }

  public List<TsFileResource> getTsFileListByTimePartition(boolean sequence, long timePartition) {
    readLock();
    try {
      List<TsFileResource> result = new ArrayList<>();
      if (sequence) {
        List<SortedSet<TsFileResource>> sequenceTsFileList =
            sequenceTsFileResources.get(timePartition);
        for (int i = sequenceTsFileList.size() - 1; i >= 0; i--) {
          result.addAll(sequenceTsFileList.get(i));
        }
      } else {
        List<List<TsFileResource>> unSequenceTsFileList =
            unSequenceTsFileResources.get(timePartition);
        for (int i = unSequenceTsFileList.size() - 1; i >= 0; i--) {
          result.addAll(unSequenceTsFileList.get(i));
        }
      }
      return result;
    } finally {
      readUnLock();
    }
  }

  @Override
  public Iterator<TsFileResource> getIterator(boolean sequence) {
    readLock();
    try {
      return getTsFileList(sequence).iterator();
    } finally {
      readUnLock();
    }
  }

  @Override
  public void remove(TsFileResource tsFileResource, boolean sequence) {
    logger.warn("Acquiring write lock in TsFileManagement");
    writeLock();
    logger.warn("Acquired write lock in TsFileManagement successfully");
    try {
      if (sequence) {
        for (SortedSet<TsFileResource> sequenceTsFileResource :
            sequenceTsFileResources.get(tsFileResource.getTimePartition())) {
          sequenceTsFileResource.remove(tsFileResource);
        }
      } else {
        for (List<TsFileResource> unSequenceTsFileResource :
            unSequenceTsFileResources.get(tsFileResource.getTimePartition())) {
          unSequenceTsFileResource.remove(tsFileResource);
        }
      }
    } finally {
      logger.warn("Release write lock in TsFileManagement");
      writeUnlock();
      logger.warn("Release write lock in TsFileManagementSuccessfully");
    }
  }

  @Override
  public void removeAll(List<TsFileResource> tsFileResourceList, boolean sequence) {
    logger.warn("Acquiring write lock in TsFileManagement");
    writeLock();
    logger.warn("Acquired write lock in TsFileManagement successfully");
    try {
      if (sequence) {
        for (List<SortedSet<TsFileResource>> partitionSequenceTsFileResource :
            sequenceTsFileResources.values()) {
          for (SortedSet<TsFileResource> levelTsFileResource : partitionSequenceTsFileResource) {
            levelTsFileResource.removeAll(tsFileResourceList);
          }
        }
      } else {
        for (List<List<TsFileResource>> partitionUnSequenceTsFileResource :
            unSequenceTsFileResources.values()) {
          for (List<TsFileResource> levelTsFileResource : partitionUnSequenceTsFileResource) {
            levelTsFileResource.removeAll(tsFileResourceList);
          }
        }
      }
    } finally {
      logger.warn("Release write lock in TsFileManagement");
      writeUnlock();
      logger.warn("Release write lock in TsFileManagementSuccessfully");
    }
  }

  @Override
  public void add(TsFileResource tsFileResource, boolean sequence) throws IOException {
    logger.warn("Acquiring write lock in TsFileManagement");
    writeLock();
    logger.warn("Acquired write lock in TsFileManagement successfully");
    try {
      long timePartitionId = tsFileResource.getTimePartition();
      int level = TsFileResource.getMergeLevel(tsFileResource.getTsFile().getName());
      if (sequence) {
        if (level <= seqLevelNum - 1) {
          // current file has normal level
          sequenceTsFileResources
              .computeIfAbsent(timePartitionId, this::newSequenceTsFileResources)
              .get(level)
              .add(tsFileResource);
        } else {
          // current file has too high level
          sequenceTsFileResources
              .computeIfAbsent(timePartitionId, this::newSequenceTsFileResources)
              .get(seqLevelNum - 1)
              .add(tsFileResource);
        }
      } else {
        if (level <= unseqLevelNum - 1) {
          // current file has normal level
          unSequenceTsFileResources
              .computeIfAbsent(timePartitionId, this::newUnSequenceTsFileResources)
              .get(level)
              .add(tsFileResource);
        } else {
          // current file has too high level
          unSequenceTsFileResources
              .computeIfAbsent(timePartitionId, this::newUnSequenceTsFileResources)
              .get(unseqLevelNum - 1)
              .add(tsFileResource);
        }
      }
    } finally {
      logger.warn("Release write lock in TsFileManagement");
      writeUnlock();
      logger.warn("Release write lock in TsFileManagementSuccessfully");
    }
  }

  @Override
  public void addRecover(TsFileResource tsFileResource, boolean sequence) {
    if (sequence) {
      sequenceRecoverTsFileResources.add(tsFileResource);
    } else {
      unSequenceRecoverTsFileResources.add(tsFileResource);
    }
  }

  @Override
  public void addAll(List<TsFileResource> tsFileResourceList, boolean sequence) throws IOException {
    logger.warn("Acquiring write lock in TsFileManagement");
    writeLock();
    logger.warn("Acquired write lock in TsFileManagement successfully");
    try {
      for (TsFileResource tsFileResource : tsFileResourceList) {
        add(tsFileResource, sequence);
      }
    } finally {
      logger.warn("Release write lock in TsFileManagement");
      writeUnlock();
      logger.warn("Release write lock in TsFileManagementSuccessfully");
    }
  }

  @Override
  public boolean contains(TsFileResource tsFileResource, boolean sequence) {
    readLock();
    try {
      if (sequence) {
        for (SortedSet<TsFileResource> sequenceTsFileResource :
            sequenceTsFileResources.computeIfAbsent(
                tsFileResource.getTimePartition(), this::newSequenceTsFileResources)) {
          if (sequenceTsFileResource.contains(tsFileResource)) {
            return true;
          }
        }
      } else {
        for (List<TsFileResource> unSequenceTsFileResource :
            unSequenceTsFileResources.computeIfAbsent(
                tsFileResource.getTimePartition(), this::newUnSequenceTsFileResources)) {
          if (unSequenceTsFileResource.contains(tsFileResource)) {
            return true;
          }
        }
      }
      return false;
    } finally {
      readUnLock();
    }
  }

  @Override
  public void clear() {
    logger.warn("Acquiring write lock in TsFileManagement");
    writeLock();
    logger.warn("Acquired write lock in TsFileManagement successfully");
    try {
      sequenceTsFileResources.clear();
      unSequenceTsFileResources.clear();
    } finally {
      logger.warn("Release write lock in TsFileManagement");
      writeUnlock();
      logger.warn("Release write lock in TsFileManagementSuccessfully");
    }
  }

  @Override
  @SuppressWarnings("squid:S3776")
  public boolean isEmpty(boolean sequence) {
    readLock();
    try {
      if (sequence) {
        for (List<SortedSet<TsFileResource>> partitionSequenceTsFileResource :
            sequenceTsFileResources.values()) {
          for (SortedSet<TsFileResource> sequenceTsFileResource : partitionSequenceTsFileResource) {
            if (!sequenceTsFileResource.isEmpty()) {
              return false;
            }
          }
        }
      } else {
        for (List<List<TsFileResource>> partitionUnSequenceTsFileResource :
            unSequenceTsFileResources.values()) {
          for (List<TsFileResource> unSequenceTsFileResource : partitionUnSequenceTsFileResource) {
            if (!unSequenceTsFileResource.isEmpty()) {
              return false;
            }
          }
        }
      }
      return true;
    } finally {
      readUnLock();
    }
  }

  @Override
  public int size(boolean sequence) {
    readLock();
    try {
      int result = 0;
      if (sequence) {
        for (List<SortedSet<TsFileResource>> partitionSequenceTsFileResource :
            sequenceTsFileResources.values()) {
          for (int i = seqLevelNum - 1; i >= 0; i--) {
            result += partitionSequenceTsFileResource.get(i).size();
          }
        }
      } else {
        for (List<List<TsFileResource>> partitionUnSequenceTsFileResource :
            unSequenceTsFileResources.values()) {
          for (int i = unseqLevelNum - 1; i >= 0; i--) {
            result += partitionUnSequenceTsFileResource.get(i).size();
          }
        }
      }
      return result;
    } finally {
      readUnLock();
    }
  }

  /** recover files */
  @Override
  @SuppressWarnings("squid:S3776")
  public void recover() {
    File logFile =
        FSFactoryProducer.getFSFactory()
            .getFile(storageGroupDir, storageGroupName + COMPACTION_LOG_NAME);
    try {
      if (logFile.exists()) {
        CompactionLogAnalyzer logAnalyzer = new CompactionLogAnalyzer(logFile);
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
          TsFileResource targetTsFileResource = getRecoverTsFileResource(targetFile, isSeq);
          if (targetTsFileResource == null) {
            targetTsFileResource = getTsFileResource(targetFile, isSeq);
            if (targetTsFileResource == null) {
              logger.warn("get null targetTsFileResource");
              return;
            }
          }
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
            CompactionUtils.merge(
                targetTsFileResource,
                getTsFileList(isSeq),
                storageGroupName,
                compactionLogger,
                deviceSet,
                isSeq,
                modifications);
            compactionLogger.close();
          } else {
            writer.close();
          }
          // complete compaction and delete source file
          deleteAllSubLevelFiles(isSeq, timePartition);
        } else {
          // get tsfile resource from list, as they have been recovered in StorageGroupProcessor
          TsFileResource targetResource = getRecoverTsFileResource(targetFile, isSeq);
          if (targetResource == null) {
            // new file already merged but old file not deleted
            targetResource = getTsFileResource(targetFile, isSeq);
            if (targetResource == null) {
              throw new IOException();
            }
          }
          long timePartition = targetResource.getTimePartition();
          List<TsFileResource> sourceTsFileResources = new ArrayList<>();
          for (String file : sourceFileList) {
            // get tsfile resource from list, as they have been recovered in StorageGroupProcessor
            TsFileResource sourceTsFileResource = getTsFileResource(file, isSeq);
            if (sourceTsFileResource == null) {
              throw new IOException();
            }
            sourceTsFileResources.add(sourceTsFileResource);
          }
          int level = TsFileResource.getMergeLevel(new File(sourceFileList.get(0)).getName());
          RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(target);
          List<Modification> modifications = new ArrayList<>();
          // if not complete compaction, resume merge
          if (writer.hasCrashed()) {
            if (offset > 0) {
              writer.getIOWriterOut().truncate(offset - 1);
            }
            writer.close();
            CompactionLogger compactionLogger =
                new CompactionLogger(storageGroupDir, storageGroupName);
            CompactionUtils.merge(
                targetResource,
                sourceTsFileResources,
                storageGroupName,
                compactionLogger,
                deviceSet,
                isSeq,
                modifications);
            compactionLogger.close();
            // complete compaction and add target tsfile
            int targetLevel = TsFileResource.getMergeLevel(targetResource.getTsFile().getName());
            if (isSeq) {
              sequenceTsFileResources.get(timePartition).get(targetLevel).add(targetResource);
              sequenceRecoverTsFileResources.clear();
            } else {
              unSequenceTsFileResources.get(timePartition).get(targetLevel).add(targetResource);
              unSequenceRecoverTsFileResources.clear();
            }
          } else {
            // complete compaction, just close writer
            writer.close();
          }
          // complete compaction, delete source files
          logger.warn("Acquiring write lock in TsFileManagement");
          writeLock();
          logger.warn("Acquired write lock in TsFileManagement successfully");
          try {
            if (Thread.currentThread().isInterrupted()) {
              throw new InterruptedException(
                  String.format("%s [Compaction] abort", storageGroupName));
            }
            deleteLevelFilesInList(timePartition, sourceTsFileResources, level, isSeq);
          } finally {
            logger.warn("Release write lock in TsFileManagement");
            writeUnlock();
            logger.warn("Release write lock in TsFileManagementSuccessfully");
          }
          for (TsFileResource tsFileResource : sourceTsFileResources) {
            logger.warn(
                "{} recover storage group delete source file {}",
                storageGroupName,
                tsFileResource.getTsFile().getName());
          }
          deleteLevelFilesInDisk(sourceTsFileResources);
          renameLevelFilesMods(modifications, sourceTsFileResources, targetResource);
        }
      }
    } catch (IOException | IllegalPathException | InterruptedException e) {
      logger.error("recover level tsfile management error ", e);
      restoreCompaction();
    } finally {
      if (logFile.exists()) {
        try {
          Files.delete(logFile.toPath());
        } catch (IOException e) {
          logger.error("delete level tsfile management log file error ", e);
        }
      }
    }
  }

  private void deleteAllSubLevelFiles(boolean isSeq, long timePartition) {
    if (isSeq) {
      for (int level = 0; level < sequenceTsFileResources.get(timePartition).size(); level++) {
        SortedSet<TsFileResource> currLevelMergeFile =
            sequenceTsFileResources.get(timePartition).get(level);
        deleteLevelFilesInDisk(currLevelMergeFile);
        deleteLevelFilesInList(timePartition, currLevelMergeFile, level, isSeq);
      }
    } else {
      for (int level = 0; level < unSequenceTsFileResources.get(timePartition).size(); level++) {
        SortedSet<TsFileResource> currLevelMergeFile =
            sequenceTsFileResources.get(timePartition).get(level);
        deleteLevelFilesInDisk(currLevelMergeFile);
        deleteLevelFilesInList(timePartition, currLevelMergeFile, level, isSeq);
      }
    }
  }

  @Override
  public void forkCurrentFileList(long timePartition) {
    logger.warn("try getting read lock");
    readLock();
    logger.warn("get read lock");
    try {
      logger.warn("forking sequence tsfile list");
      forkTsFileList(
          forkedSequenceTsFileResources,
          sequenceTsFileResources.computeIfAbsent(timePartition, this::newSequenceTsFileResources),
          seqLevelNum);
      // we have to copy all unseq file
      logger.warn("forking unsequence tsfile list");
      forkTsFileList(
          forkedUnSequenceTsFileResources,
          unSequenceTsFileResources.computeIfAbsent(
              timePartition, this::newUnSequenceTsFileResources),
          unseqLevelNum + 1);
    } finally {
      readUnLock();
      logger.warn("release the read lock");
    }
  }

  private void forkTsFileList(
      List<List<TsFileResource>> forkedTsFileResources, List rawTsFileResources, int currMaxLevel) {
    forkedTsFileResources.clear();
    for (int i = 0; i < currMaxLevel - 1; i++) {
      List<TsFileResource> forkedLevelTsFileResources = new ArrayList<>();
      Collection<TsFileResource> levelRawTsFileResources =
          (Collection<TsFileResource>) rawTsFileResources.get(i);
      for (TsFileResource tsFileResource : levelRawTsFileResources) {
        if (tsFileResource.isClosed()) {
          forkedLevelTsFileResources.add(tsFileResource);
        }
      }
      forkedTsFileResources.add(forkedLevelTsFileResources);
    }
  }

  @Override
  protected void merge(long timePartition) {
    // compacting sequence file in one time partition
    isMergeExecutedInCurrentTask =
        merge(
            forkedSequenceTsFileResources, true, timePartition, seqLevelNum, seqFileNumInEachLevel);
    if (enableUnseqCompaction
        && unseqLevelNum <= 1
        && forkedUnSequenceTsFileResources.get(0).size() > 0) {
      isMergeExecutedInCurrentTask =
          merge(
                  isForceFullMerge,
                  getTsFileListByTimePartition(true, timePartition),
                  forkedUnSequenceTsFileResources.get(0),
                  Long.MAX_VALUE)
              || isMergeExecutedInCurrentTask;
    } else {
      isMergeExecutedInCurrentTask =
          merge(
                  forkedUnSequenceTsFileResources,
                  false,
                  timePartition,
                  unseqLevelNum,
                  unseqFileNumInEachLevel)
              || isMergeExecutedInCurrentTask;
    }
  }

  @SuppressWarnings("squid:S3776")
  private boolean merge(
      List<List<TsFileResource>> mergeResources,
      boolean sequence,
      long timePartition,
      int currMaxLevel,
      int currMaxFileNumInEachLevel) {
    // wait until unseq merge has finished
    logger.warn(
        "Level Compaction in {} waiting for unsequence compaction finish",
        sequence ? "sequence" : "unsequence");
    while (isUnseqMerging) {
      try {
        wait(200);
      } catch (InterruptedException e) {
        logger.error("{} [Compaction] shutdown", storageGroupName, e);
        Thread.currentThread().interrupt();
        return false;
      }
    }
    logger.warn(
        "Level Compaction in {} begins to select files", sequence ? "sequence" : "unsequence");
    isSeqMerging = true;
    long startTimeMillis = System.currentTimeMillis();
    // whether execute merge chunk in the loop below
    boolean isMergeExecutedInCurrentTask = false;
    CompactionLogger compactionLogger = null;
    try {
      logger.debug("{} start to filter compaction condition", storageGroupName);
      for (int i = 0; i < currMaxLevel - 1; i++) {
        List<TsFileResource> currLevelTsFileResource = mergeResources.get(i);
        if (currMaxFileNumInEachLevel <= currLevelTsFileResource.size()) {
          // just merge part of the file
          isMergeExecutedInCurrentTask = true;
          // level is numbered from 0
          if (enableUnseqCompaction && !sequence && i == currMaxLevel - 2) {
            // do not merge current unseq file level to upper level and just merge all of them to
            // seq file
            isSeqMerging = false;
            isMergeExecutedInCurrentTask =
                merge(
                    isForceFullMerge,
                    getTsFileListByTimePartition(true, timePartition),
                    mergeResources.get(i),
                    Long.MAX_VALUE);
          } else {
            compactionLogger = new CompactionLogger(storageGroupDir, storageGroupName);
            List<TsFileResource> toMergeTsFiles =
                mergeResources.get(i).subList(0, currMaxFileNumInEachLevel);
            logger.warn("The size of TsFile to be compacted is {}", toMergeTsFiles.size());
            // log source file list and target file for recover
            for (TsFileResource mergeResource : toMergeTsFiles) {
              mergeResource.setMerging(true);
              compactionLogger.logFile(SOURCE_NAME, mergeResource.getTsFile());
            }
            File newLevelFile =
                TsFileResource.modifyTsFileNameMergeCnt(mergeResources.get(i).get(0).getTsFile());
            compactionLogger.logSequence(sequence);
            compactionLogger.logFile(TARGET_NAME, newLevelFile);
            logger.warn(
                "{} [Compaction] merge level-{}'s {} TsFiles to next level",
                storageGroupName,
                i,
                toMergeTsFiles.size());
            for (TsFileResource toMergeTsFile : toMergeTsFiles) {
              logger.warn(
                  "{} [Compaction] start to merge TsFile {}", storageGroupName, toMergeTsFile);
            }

            TsFileResource newResource = new TsFileResource(newLevelFile);
            List<Modification> modifications = new ArrayList<>();
            // merge, read from source files and write to target file
            CompactionUtils.merge(
                newResource,
                toMergeTsFiles,
                storageGroupName,
                compactionLogger,
                new HashSet<>(),
                sequence,
                modifications);
            logger.warn(
                "{} [Compaction] merged level-{}'s {} TsFiles to next level, and start to delete old files",
                storageGroupName,
                i,
                toMergeTsFiles.size());
            logger.warn("Acquiring write lock in TsFileManagement");
            writeLock();
            logger.warn("Acquired write lock in TsFileManagement successfully");
            try {
              if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException(
                    String.format("%s [Compaction] abort", storageGroupName));
              }

              if (sequence) {
                sequenceTsFileResources.get(timePartition).get(i + 1).add(newResource);
              } else {
                unSequenceTsFileResources.get(timePartition).get(i + 1).add(newResource);
              }
              deleteLevelFilesInList(timePartition, toMergeTsFiles, i, sequence);
              if (mergeResources.size() > i + 1) {
                mergeResources.get(i + 1).add(newResource);
              }
            } finally {
              logger.warn("Release write lock in TsFileManagement");
              writeUnlock();
              logger.warn("Release write lock in TsFileManagementSuccessfully");
            }
            deleteLevelFilesInDisk(toMergeTsFiles);
            renameLevelFilesMods(modifications, toMergeTsFiles, newResource);
            compactionLogger.close();
            File logFile =
                FSFactoryProducer.getFSFactory()
                    .getFile(storageGroupDir, storageGroupName + COMPACTION_LOG_NAME);
            if (logFile.exists()) {
              Files.delete(logFile.toPath());
            }
          }
        }
      }
    } catch (Exception e) {
      if (compactionLogger != null) {
        try {
          compactionLogger.close();
        } catch (IOException ioException) {
          logger.error("{} Compaction log close fail", storageGroupName + COMPACTION_LOG_NAME);
        }
      }
      isMergeExecutedInCurrentTask = false;
      restoreCompaction();
      logger.error("Error occurred in Compaction Merge thread", e);
    } finally {
      isSeqMerging = false;
      // reset the merge working state to false
      if (isMergeExecutedInCurrentTask) {
        logger.info(
            "{} [Compaction] merge end time isSeq = {}, consumption: {} ms",
            storageGroupName,
            sequence,
            System.currentTimeMillis() - startTimeMillis);
      }
    }
    return isMergeExecutedInCurrentTask;
  }

  private List<SortedSet<TsFileResource>> newSequenceTsFileResources(Long k) {
    List<SortedSet<TsFileResource>> newSequenceTsFileResources = new ArrayList<>();
    for (int i = 0; i < seqLevelNum; i++) {
      newSequenceTsFileResources.add(
          new TreeSet<>(
              (o1, o2) -> {
                try {
                  int rangeCompare =
                      Long.compare(
                          Long.parseLong(o1.getTsFile().getParentFile().getName()),
                          Long.parseLong(o2.getTsFile().getParentFile().getName()));
                  return rangeCompare == 0
                      ? compareFileName(o1.getTsFile(), o2.getTsFile())
                      : rangeCompare;
                } catch (NumberFormatException e) {
                  return compareFileName(o1.getTsFile(), o2.getTsFile());
                }
              }));
    }
    return newSequenceTsFileResources;
  }

  private List<List<TsFileResource>> newUnSequenceTsFileResources(Long k) {
    List<List<TsFileResource>> newUnSequenceTsFileResources = new ArrayList<>();
    for (int i = 0; i < unseqLevelNum; i++) {
      newUnSequenceTsFileResources.add(new ArrayList<>());
    }
    return newUnSequenceTsFileResources;
  }

  private TsFileResource getRecoverTsFileResource(String filePath, boolean isSeq)
      throws IOException {
    try {
      if (isSeq) {
        for (TsFileResource tsFileResource : sequenceRecoverTsFileResources) {
          if (Files.isSameFile(tsFileResource.getTsFile().toPath(), new File(filePath).toPath())) {
            return tsFileResource;
          }
        }
      } else {
        for (TsFileResource tsFileResource : unSequenceRecoverTsFileResources) {
          if (Files.isSameFile(tsFileResource.getTsFile().toPath(), new File(filePath).toPath())) {
            return tsFileResource;
          }
        }
      }
    } catch (IOException e) {
      logger.error("cannot get tsfile resource path: {}", filePath);
    }
    return null;
  }

  private TsFileResource getTsFileResource(String filePath, boolean isSeq) {
    readLock();
    try {
      File file = new File(filePath);
      if (!file.exists()) {
        return null;
      }
      if (isSeq) {
        for (List<SortedSet<TsFileResource>> tsFileResourcesWithLevel :
            sequenceTsFileResources.values()) {
          for (SortedSet<TsFileResource> tsFileResources : tsFileResourcesWithLevel) {
            for (TsFileResource tsFileResource : tsFileResources) {
              if (Files.isSameFile(
                  tsFileResource.getTsFile().toPath(), new File(filePath).toPath())) {
                return tsFileResource;
              }
            }
          }
        }
      } else {
        for (List<List<TsFileResource>> tsFileResourcesWithLevel :
            unSequenceTsFileResources.values()) {
          for (List<TsFileResource> tsFileResources : tsFileResourcesWithLevel) {
            for (TsFileResource tsFileResource : tsFileResources) {
              if (Files.isSameFile(
                  tsFileResource.getTsFile().toPath(), new File(filePath).toPath())) {
                return tsFileResource;
              }
            }
          }
        }
      }
    } catch (Exception e) {
      logger.error("cannot get tsfile resource path: {}", filePath, e);
      return null;
    } finally {
      readUnLock();
    }
    return null;
  }

  /** restore the files back to the status before the compaction task is submitted */
  private void restoreCompaction() {
    File logFile =
        FSFactoryProducer.getFSFactory()
            .getFile(storageGroupDir, storageGroupName + COMPACTION_LOG_NAME);
    try {
      if (logFile.exists()) {
        CompactionLogAnalyzer logAnalyzer = new CompactionLogAnalyzer(logFile);
        logAnalyzer.analyze();
        String targetFilePath = logAnalyzer.getTargetFile();
        List<String> sourceFileList = logAnalyzer.getSourceFiles();
        boolean isSeq = logAnalyzer.isSeq();
        for (String file : sourceFileList) {
          TsFileResource fileResource = getTsFileResource(file, isSeq);
          if (fileResource != null) {
            fileResource.setMerging(false);
          }
        }
        if (targetFilePath != null) {
          File targetFile = new File(targetFilePath);
          if (targetFile.exists()) {
            logger.error(
                "{} restore delete target file {} ", storageGroupName, targetFile.getName());
            if (!targetFile.delete()) {
              logger.warn("fail to delete {}", targetFile);
            }
          }
        }
      }
    } catch (IOException e) {
      logger.error("restore compaction failed", e);
    } finally {
      if (logFile.exists()) {
        try {
          Files.delete(logFile.toPath());
        } catch (IOException e) {
          logger.error("delete compaction log file error ", e);
        }
      }
    }
  }

  @TestOnly
  public Map<Long, List<SortedSet<TsFileResource>>> getSequenceTsFileResources() {
    return sequenceTsFileResources;
  }
}
