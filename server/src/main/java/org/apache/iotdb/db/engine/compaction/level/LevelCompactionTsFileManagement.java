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
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileInfo;
import org.apache.iotdb.db.engine.compaction.utils.CompactionLogAnalyzer;
import org.apache.iotdb.db.engine.compaction.utils.CompactionLogger;
import org.apache.iotdb.db.engine.compaction.utils.CompactionUtils;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.COMPACTION_LOG_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.SOURCE_INFO;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.TARGET_INFO;

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

  /**
   * Long -> partition list. Use treemap to keep the small partition in front.
   * List<SortedSet<TsFileResource>> -> File level list<file list in each level>
   */
  private final Map<Long, List<SortedSet<TsFileResource>>> sequenceTsFileResources =
      new TreeMap<>();

  private final Map<Long, List<List<TsFileResource>>> unSequenceTsFileResources = new TreeMap<>();
  private final List<List<TsFileResource>> forkedSequenceTsFileResources = new ArrayList<>();
  private final List<List<TsFileResource>> forkedUnSequenceTsFileResources = new ArrayList<>();

  public LevelCompactionTsFileManagement(
      String storageGroupName, String virtualStorageGroupId, String storageGroupDir) {
    super(storageGroupName, virtualStorageGroupId, storageGroupDir);
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

  protected void deleteLevelFilesInDisk(Collection<TsFileResource> mergeTsFiles) {
    logger.info("{} [compaction] merge starts to delete real file", storageGroupName);
    for (TsFileResource mergeTsFile : mergeTsFiles) {
      deleteLevelFile(mergeTsFile);
      logger.info(
          "{} [Compaction] delete TsFile {}", storageGroupName, mergeTsFile.getTsFilePath());
    }
  }

  protected void deleteLevelFilesInList(
      long timePartitionId, Collection<TsFileResource> mergeTsFiles, int level, boolean sequence) {
    logger.info("{} [compaction] merge starts to delete file list", storageGroupName);
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

  protected void deleteLevelFile(TsFileResource seqFile) {
    seqFile.writeLock();
    try {
      ChunkCache.getInstance().clear();
      TimeSeriesMetadataCache.getInstance().clear();
      FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile.getTsFilePath());
      seqFile.setDeleted(true);
      FileUtils.moveFile(
          seqFile.getTsFile(),
          new File(
              seqFile.getTsFile().getAbsolutePath().replace(".tsfile", ".compaction-old-file")));
      FileUtils.moveFile(
          new File(seqFile.getTsFile().getAbsolutePath() + ".resource"),
          new File(
              seqFile
                  .getTsFile()
                  .getAbsolutePath()
                  .replace(".tsfile", ".compaction-old-file.resource")));
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
            sequenceTsFileResources.getOrDefault(timePartition, new ArrayList<>());
        for (int i = sequenceTsFileList.size() - 1; i >= 0; i--) {
          result.addAll(sequenceTsFileList.get(i));
        }
      } else {
        List<List<TsFileResource>> unSequenceTsFileList =
            unSequenceTsFileResources.getOrDefault(timePartition, new ArrayList<>());
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
    writeLock();
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
      writeUnlock();
    }
  }

  @Override
  public void removeAll(List<TsFileResource> tsFileResourceList, boolean sequence) {
    writeLock();
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
      writeUnlock();
    }
  }

  @Override
  public void add(TsFileResource tsFileResource, boolean sequence) throws IOException {
    writeLock();
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
      writeUnlock();
    }
  }

  @Override
  public void addAll(List<TsFileResource> tsFileResourceList, boolean sequence) throws IOException {
    writeLock();
    try {
      for (TsFileResource tsFileResource : tsFileResourceList) {
        add(tsFileResource, sequence);
      }
    } finally {
      writeUnlock();
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
    writeLock();
    try {
      sequenceTsFileResources.clear();
      unSequenceTsFileResources.clear();
    } finally {
      writeUnlock();
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
        List<CompactionFileInfo> sourceFileInfo = logAnalyzer.getSourceFileInfo();
        CompactionFileInfo targetFileInfo = logAnalyzer.getTargetFileInfo();
        String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
        File targetFile = null;
        if (targetFileInfo == null || sourceFileInfo.isEmpty()) {
          return;
        }
        // get tsfile resource from list, as they have been recovered in StorageGroupProcessor
        TsFileResource targetResource = null;
        File targetResourceFile = null;
        for (String dataDir : dataDirs) {
          if ((targetFile = targetFileInfo.getFile(dataDir)).exists()) {
            targetResource = new TsFileResource(targetFile);
            targetResourceFile = new File(targetFile.getPath() + TsFileResource.RESOURCE_SUFFIX);
          }
        }
        if (targetResource != null) {
          if (!targetResourceFile.exists()) {
            // target file resource has not been generated yet
            // delete target file if exists
            targetResource.remove();
          } else {
            // complete compaction, delete source files
            logger.info(
                "[Compaction][Recover] target file {} is compeleted, remove resource file",
                targetResource);
            List<TsFileResource> sourceTsFileResources = new ArrayList<>();
            for (CompactionFileInfo sourceInfo : sourceFileInfo) {
              // get tsfile resource from list, as they have been recovered in StorageGroupProcessor
              File sourceFile = null;
              TsFileResource sourceTsFileResource = null;
              for (String dataDir : dataDirs) {
                if ((sourceFile = sourceInfo.getFile(dataDir)).exists()) {
                  sourceTsFileResource = new TsFileResource(sourceFile);
                  break;
                }
              }
              if (sourceTsFileResource == null) {
                // if sourceTsFileResource is null, it has been deleted
                continue;
              }
              sourceTsFileResources.add(sourceTsFileResource);
            }
            if (sourceFileInfo.size() != 0) {
              List<Modification> modifications = new ArrayList<>();
              // if not complete compaction, remove target file
              for (TsFileResource tsFileResource : sourceTsFileResources) {
                logger.info(
                    "{} recover storage group delete source file {}",
                    storageGroupName,
                    tsFileResource.getTsFile().getName());
              }
              deleteLevelFilesInDisk(sourceTsFileResources);
              renameLevelFilesMods(modifications, sourceTsFileResources, targetResource);
            }
          }
        }
      }
      if (logFile.exists()) {
        Files.delete(logFile.toPath());
      }
    } catch (Throwable e) {
      logger.error("exception occurs during recovering compaction", e);
      canMerge = false;
    }
  }

  @Override
  public void forkCurrentFileList(long timePartition) {
    readLock();
    try {
      forkTsFileList(
          forkedSequenceTsFileResources,
          sequenceTsFileResources.computeIfAbsent(timePartition, this::newSequenceTsFileResources),
          seqLevelNum);
      // we have to copy all unseq file
      forkTsFileList(
          forkedUnSequenceTsFileResources,
          unSequenceTsFileResources.computeIfAbsent(
              timePartition, this::newUnSequenceTsFileResources),
          unseqLevelNum + 1);
    } finally {
      readUnLock();
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
    List<TsFileResource> mergingFiles = new ArrayList<>();
    while (isUnseqMerging) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        logger.error("{} [Compaction] shutdown", storageGroupName, e);
        Thread.currentThread().interrupt();
        return false;
      }
    }
    isSeqMerging = true;
    long startTimeMillis = System.currentTimeMillis();
    // whether execute merge chunk in the loop below
    boolean isMergeExecutedInCurrentTask = false;
    CompactionLogger compactionLogger = null;
    TsFileResource newResource = null;
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
            List<TsFileResource> toMergeTsFiles =
                mergeResources.get(i).subList(0, currMaxFileNumInEachLevel);
            compactionSelectionLock.lock();
            try {
              if (!checkAndSetFilesMergingIfNotSet(toMergeTsFiles, null)) {
                return false;
              }
              mergingFiles.addAll(toMergeTsFiles);
            } finally {
              compactionSelectionLock.unlock();
            }
            compactionLogger = new CompactionLogger(storageGroupDir, storageGroupName);
            // log source file list and target file for recover
            for (TsFileResource mergeResource : toMergeTsFiles) {
              compactionLogger.logFile(
                  SOURCE_INFO,
                  storageGroupName,
                  virtualStorageGroupId,
                  timePartition,
                  mergeResource.getTsFile(),
                  sequence);
            }
            File newLevelFile =
                TsFileResource.modifyTsFileNameMergeCnt(mergeResources.get(i).get(0).getTsFile());
            compactionLogger.logSequence(sequence);
            compactionLogger.logFile(
                TARGET_INFO,
                storageGroupName,
                virtualStorageGroupId,
                timePartition,
                newLevelFile,
                sequence);
            logger.info(
                "{} [Compaction] merge level-{}'s {} TsFiles to next level",
                storageGroupName,
                i,
                toMergeTsFiles.size());
            for (TsFileResource toMergeTsFile : toMergeTsFiles) {
              logger.info(
                  "{} [Compaction] start to merge TsFile {}", storageGroupName, toMergeTsFile);
            }

            newResource = new TsFileResource(newLevelFile);
            List<Modification> modifications = new ArrayList<>();
            // merge, read from source files and write to target file
            CompactionUtils.merge(
                newResource,
                toMergeTsFiles,
                storageGroupName,
                compactionLogger,
                new HashSet<>(),
                sequence,
                modifications,
                null);
            logger.info(
                "{} [Compaction] merged level-{}'s {} TsFiles to next level, and start to delete old files",
                storageGroupName,
                i,
                toMergeTsFiles.size());
            writeLock();
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
              writeUnlock();
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
    } catch (Throwable e) {
      if (compactionLogger != null) {
        try {
          compactionLogger.close();
        } catch (IOException ioException) {
          logger.error("{} Compaction log close fail", storageGroupName + COMPACTION_LOG_NAME);
        }
      }
      for (TsFileResource resource : mergingFiles) {
        resource.setMerging(false);
      }
      isMergeExecutedInCurrentTask = false;
      handleExceptionForCompaction(mergingFiles, newResource, sequence);
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
  private void handleExceptionForCompaction(
      List<TsFileResource> sourceTsFiles, TsFileResource targetTsFile, boolean sequence) {
    File logFile =
        FSFactoryProducer.getFSFactory()
            .getFile(storageGroupDir, storageGroupName + COMPACTION_LOG_NAME);
    logger.info(
        "{}-{} [Compaction][Restore] Start to restore compaction",
        storageGroupName,
        virtualStorageGroupId);
    try {
      if (targetTsFile == null || !targetTsFile.getTsFile().exists()) {
        // target file not exists
        logger.warn(
            "{}-{} [Compaction][Restore] Cannot find compaction target file {}",
            storageGroupName,
            virtualStorageGroupId,
            targetTsFile);
      } else {
        RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(targetTsFile.getTsFile());
        if (writer.hasCrashed()) {
          // target file is incomplete
          writer.close();
          logger.info(
              "{}-{} [Compaction][Restore] target file {} is incomplete, delete it",
              storageGroupName,
              virtualStorageGroupId,
              targetTsFile);
          Files.delete(targetTsFile.getTsFile().toPath());
        } else {
          // target file is complete, delete source files
          logger.info(
              "{}-{} [Compaction][Restore] target file {} is complete, delete source files",
              storageGroupName,
              virtualStorageGroupId,
              targetTsFile);
          for (TsFileResource sourceFile : sourceTsFiles) {
            logger.info(
                "{}-{} [Compaction][Restore] deleting source file {}",
                storageGroupName,
                virtualStorageGroupId,
                sourceFile);
            sourceFile.remove();
            remove(sourceFile, sequence);
          }
        }
      }
      if (logFile.exists()) {
        Files.delete(logFile.toPath());
      }
    } catch (IOException e) {
      logger.error(
          "{}-{} [Compaction][Restore] exception occurs during restoring compaction with source files {}, target file {}, set canMerge to false",
          storageGroupName,
          virtualStorageGroupId,
          sourceTsFiles,
          targetTsFile,
          e);
      canMerge = false;
    }
  }

  @TestOnly
  public Map<Long, List<SortedSet<TsFileResource>>> getSequenceTsFileResources() {
    return sequenceTsFileResources;
  }
}
