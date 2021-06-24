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
import org.apache.iotdb.db.engine.compaction.utils.CompactionSeparateFileUtils;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.selector.IMergeFileSelector;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.COMPACTION_LOG_NAME;
import static org.apache.iotdb.db.engine.merge.task.MergeTask.MERGE_SUFFIX;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

/** The TsFileManagement for LEVEL_COMPACTION, use level struct to manage TsFile list */
public class TraditionalLevelCompactionTsFileManagement extends TsFileManagement {

  private static final Logger logger =
      LoggerFactory.getLogger(TraditionalLevelCompactionTsFileManagement.class);

  private final long BASE_FILE_SIZE = IoTDBDescriptor.getInstance().getConfig().getBaseFileSize();
  private final int FILE_SIZE_RATE = IoTDBDescriptor.getInstance().getConfig().getFileSizeRate();
  private final int MAX_LEVEL_NUM = IoTDBDescriptor.getInstance().getConfig().getMaxLevelNum();

  // First map is partition list; Second list is level list; Third list is file list in level;
  private final Map<Long, List<SortedSet<TsFileResource>>> sequenceTsFileResources =
      new HashMap<>();
  private final Map<Long, List<List<TsFileResource>>> unSequenceTsFileResources = new HashMap<>();
  private final List<List<TsFileResource>> forkedSequenceTsFileResources = new ArrayList<>();
  private final List<List<TsFileResource>> forkedUnSequenceTsFileResources = new ArrayList<>();
  private final List<TsFileResource> sequenceRecoverTsFileResources = new ArrayList<>();
  private final List<TsFileResource> unSequenceRecoverTsFileResources = new ArrayList<>();
  private FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  public TraditionalLevelCompactionTsFileManagement(
      String storageGroupName, String storageGroupDir) {
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
      logger.info(
          "{} [Compaction] delete TsFile {}", storageGroupName, mergeTsFile.getTsFilePath());
    }
  }

  private void deleteLevelFile(
      long timePartitionId, TsFileResource mergeTsFile, int level, boolean sequence) {
    logger.debug("{} [compaction] merge starts to delete file list", storageGroupName);
    if (sequence) {
      if (sequenceTsFileResources.containsKey(timePartitionId)) {
        if (sequenceTsFileResources.get(timePartitionId).size() > level) {
          sequenceTsFileResources.get(timePartitionId).get(level).remove(mergeTsFile);
        }
      }
    } else {
      if (unSequenceTsFileResources.containsKey(timePartitionId)) {
        if (unSequenceTsFileResources.get(timePartitionId).size() > level) {
          unSequenceTsFileResources.get(timePartitionId).get(level).remove(mergeTsFile);
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
      seqFile
          .getTsFile()
          .renameTo(new File(seqFile.getTsFilePath() + "_" + System.currentTimeMillis()));
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
  public void add(TsFileResource tsFileResource, boolean sequence) {
    writeLock();
    try {
      long timePartitionId = tsFileResource.getTimePartition();
      int level = getMergeLevel(tsFileResource.getTsFile());
      if (sequence) {
        if (level <= MAX_LEVEL_NUM - 1) {
          // current file has normal level
          sequenceTsFileResources
              .computeIfAbsent(timePartitionId, this::newSequenceTsFileResources)
              .get(level)
              .add(tsFileResource);
        } else {
          // current file has too high level
          sequenceTsFileResources
              .computeIfAbsent(timePartitionId, this::newSequenceTsFileResources)
              .get(MAX_LEVEL_NUM - 1)
              .add(tsFileResource);
        }
      } else {
        if (level <= MAX_LEVEL_NUM - 1) {
          // current file has normal level
          unSequenceTsFileResources
              .computeIfAbsent(timePartitionId, this::newUnSequenceTsFileResources)
              .get(level)
              .add(tsFileResource);
        } else {
          // current file has too high level
          unSequenceTsFileResources
              .computeIfAbsent(timePartitionId, this::newUnSequenceTsFileResources)
              .get(MAX_LEVEL_NUM - 1)
              .add(tsFileResource);
        }
      }
    } finally {
      writeUnlock();
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
  public void addAll(List<TsFileResource> tsFileResourceList, boolean sequence) {
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
          for (int i = MAX_LEVEL_NUM - 1; i >= 0; i--) {
            result += partitionSequenceTsFileResource.get(i).size();
          }
        }
      } else {
        for (List<List<TsFileResource>> partitionUnSequenceTsFileResource :
            unSequenceTsFileResources.values()) {
          for (int i = MAX_LEVEL_NUM - 1; i >= 0; i--) {
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
  public void recover() {}

  @Override
  public void forkCurrentFileList(long timePartition) {
    readLock();
    try {
      forkTsFileList(
          forkedSequenceTsFileResources,
          sequenceTsFileResources.computeIfAbsent(timePartition, this::newSequenceTsFileResources));
      // we have to copy all unseq file
      forkTsFileList(
          forkedUnSequenceTsFileResources,
          unSequenceTsFileResources.computeIfAbsent(
              timePartition, this::newUnSequenceTsFileResources));
    } finally {
      readUnLock();
    }
  }

  private void forkTsFileList(
      List<List<TsFileResource>> forkedTsFileResources, List rawTsFileResources) {
    forkedTsFileResources.clear();
    for (int i = 0; i < MAX_LEVEL_NUM; i++) {
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
    isMergeExecutedInCurrentTask = merge(forkedUnSequenceTsFileResources, timePartition);
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

  private long getFileSizeThreshold(int levelIndex) {
    long result = BASE_FILE_SIZE;
    for (int i = 0; i < levelIndex; i++) {
      result *= FILE_SIZE_RATE;
    }
    return result;
  }

  @SuppressWarnings("squid:S3776")
  private boolean merge(List<List<TsFileResource>> mergeResources, long timePartition) {
    long startTimeMillis = System.currentTimeMillis();
    // whether execute merge chunk in the loop below
    boolean isMergeExecutedInCurrentTask = false;
    try {
      logger.info("{} start to filter compaction condition", storageGroupName);
      for (int i = 0; i < MAX_LEVEL_NUM - 1; i++) {
        List<TsFileResource> currLevelTsFileResource = mergeResources.get(i);
        long currTotalFileSize = 0L;
        for (TsFileResource tsFileResource : currLevelTsFileResource) {
          currTotalFileSize += tsFileResource.getTsFileSize();
        }
        if (currTotalFileSize >= getFileSizeThreshold(i)) {
          // just merge part of the file
          isMergeExecutedInCurrentTask = true;
          // log source file list and target file for recover
          TsFileResource toMergeTsFile = currLevelTsFileResource.get(0);
          logger.info(
              "{} [Compaction] merge TsFile {} to next level",
              storageGroupName,
              toMergeTsFile.getTsFile().getName());
          long budget = IoTDBDescriptor.getInstance().getConfig().getMergeMemoryBudget();
          long timeLowerBound = System.currentTimeMillis() - Long.MAX_VALUE;
          List<TsFileResource> unSeqMergeList = new ArrayList<>();
          unSeqMergeList.add(toMergeTsFile);
          MergeResource mergeResource =
              new MergeResource(mergeResources.get(i + 1), unSeqMergeList, timeLowerBound);
          IMergeFileSelector fileSelector = getMergeFileSelector(budget, mergeResource);
          try {
            List[] mergeFiles = fileSelector.select();
            if (mergeFiles.length == 0 || mergeFiles[0].size() == 0 || mergeFiles[1].size() == 0) {
              writeLock();
              try {
                File newLevelFile =
                    TsFileResource.modifyTsFileNameMergeCnt(toMergeTsFile.getTsFile());
                fsFactory.moveFile(toMergeTsFile.getTsFile(), newLevelFile);
                fsFactory.moveFile(
                    fsFactory.getFile(
                        toMergeTsFile.getTsFile().getAbsolutePath()
                            + TsFileResource.RESOURCE_SUFFIX),
                    fsFactory.getFile(
                        newLevelFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX));
                if (toMergeTsFile.getModFile().exists()) {
                  fsFactory.moveFile(
                      fsFactory.getFile(toMergeTsFile.getModFile().getFilePath()),
                      fsFactory.getFile(
                          newLevelFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX));
                }
                toMergeTsFile.setFile(newLevelFile);
                unSequenceTsFileResources.get(timePartition).get(i + 1).add(toMergeTsFile);
                deleteLevelFile(timePartition, toMergeTsFile, i, false);
                if (mergeResources.size() > i + 1) {
                  mergeResources.get(i + 1).add(toMergeTsFile);
                }
              } finally {
                writeUnlock();
              }
            } else {
              List<TsFileResource> seqResources = mergeResource.getSeqFiles();
              List<TsFileResource> unseqResources = mergeResource.getUnseqFiles();
              List<TsFileResource> sourceResources = new ArrayList<>(seqResources);
              sourceResources.addAll(unseqResources);
              File newLevelFile =
                  TsFileResource.modifyTsFileNameMergeCnt(mergeResources.get(i).get(0).getTsFile());
              TsFileResource newResource = new TsFileResource(newLevelFile);
              List<TsFileResource> targetTsFileResources =
                  CompactionSeparateFileUtils.mergeWithFileSeparate(
                      newResource, sourceResources, storageGroupName);
              logger.info(
                  "{} [Compaction] merged level-{}'s {} TsFiles to next level's {} TsFiles, and start to delete old files",
                  storageGroupName,
                  i,
                  unseqResources.size(),
                  targetTsFileResources.size());
              writeLock();
              try {
                if (Thread.currentThread().isInterrupted()) {
                  throw new InterruptedException(
                      String.format("%s [Compaction] abort", storageGroupName));
                }
                unSequenceTsFileResources
                    .get(timePartition)
                    .get(i + 1)
                    .addAll(targetTsFileResources);
                deleteLevelFilesInList(timePartition, unSeqMergeList, i, false);
                deleteLevelFilesInList(timePartition, seqResources, i + 1, false);
                if (mergeResources.size() > i + 1) {
                  mergeResources.get(i + 1).removeAll(seqResources);
                  mergeResources.get(i + 1).addAll(targetTsFileResources);
                }
              } catch (InterruptedException interruptedException) {
                interruptedException.printStackTrace();
              } finally {
                writeUnlock();
              }
              deleteLevelFilesInDisk(sourceResources);
            }
            // avoid pending tasks holds the metadata and streams
            mergeResource.clear();
          } catch (MergeException | IOException e) {
            logger.error("{} cannot select file for merge", storageGroupName, e);
            return false;
          }
        }
      }
    } catch (Exception e) {
      logger.error("Error occurred in Compaction Merge thread", e);
    } finally {
      // reset the merge working state to false
      logger.info(
          "{} [Compaction] merge end time isSeq = {}, consumption: {} ms",
          storageGroupName,
          false,
          System.currentTimeMillis() - startTimeMillis);
    }
    return isMergeExecutedInCurrentTask;
  }

  public void mergeEndAction(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles, File mergeLog) {
    logger.info("{} a merge task is ending...", storageGroupName);

    if (Thread.currentThread().isInterrupted() || unseqFiles.isEmpty()) {
      // merge task abort, or merge runtime exception arose, just end this merge
      isUnseqMerging = false;
      logger.info("{} a merge task abnormally ends", storageGroupName);
      return;
    }
    removeUnseqFiles(unseqFiles);

    for (int i = 0; i < seqFiles.size(); i++) {
      TsFileResource seqFile = seqFiles.get(i);
      // get both seqFile lock and merge lock
      doubleWriteLock(seqFile);

      try {
        // if meet error(like file not found) in merge task, the .merge file may not be deleted
        File mergedFile =
            FSFactoryProducer.getFSFactory().getFile(seqFile.getTsFilePath() + MERGE_SUFFIX);
        if (mergedFile.exists()) {
          mergedFile.delete();
        }
        updateMergeModification(seqFile);
      } finally {
        doubleWriteUnlock(seqFile);
      }
    }

    try {
      removeMergingModification();
      isUnseqMerging = false;
      Files.delete(mergeLog.toPath());
    } catch (IOException e) {
      logger.error(
          "{} a merge task ends but cannot delete log {}", storageGroupName, mergeLog.toPath());
    }

    logger.info("{} a merge task ends", storageGroupName);
  }

  private List<SortedSet<TsFileResource>> newSequenceTsFileResources(Long k) {
    List<SortedSet<TsFileResource>> newSequenceTsFileResources = new ArrayList<>();
    for (int i = 0; i < MAX_LEVEL_NUM; i++) {
      newSequenceTsFileResources.add(
          new TreeSet<>(
              (o1, o2) -> {
                try {
                  String[] item1Strs = o1.getTsFile().getParent().split("/");
                  String[] item2Strs = o2.getTsFile().getParent().split("/");
                  int rangeCompare = 0;
                  int index = 0;
                  do {
                    Pattern pattern = Pattern.compile("[0-9]+");
                    Matcher isNum = pattern.matcher(item1Strs[index]);
                    if (isNum.matches()) {
                      rangeCompare =
                          Integer.parseInt(item1Strs[index]) - Integer.parseInt(item2Strs[index]);
                    }
                    index++;
                  } while (rangeCompare == 0
                      && item1Strs.length > index
                      && item2Strs.length > index);
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
    for (int i = 0; i < MAX_LEVEL_NUM; i++) {
      newUnSequenceTsFileResources.add(new ArrayList<>());
    }
    return newUnSequenceTsFileResources;
  }

  public static int getMergeLevel(File file) {
    String mergeLevelStr =
        file.getPath()
            .substring(file.getPath().lastIndexOf(FILE_NAME_SEPARATOR) + 1)
            .replaceAll(TSFILE_SUFFIX, "");
    return Integer.parseInt(mergeLevelStr);
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
        if (targetFilePath != null) {
          File targetFile = new File(targetFilePath);
          if (targetFile.exists()) {
            targetFile.delete();
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
