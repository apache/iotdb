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
package org.apache.iotdb.db.engine.compaction.cross.rewrite;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.cross.ICrossSpaceSelector;
import org.apache.iotdb.db.engine.compaction.cross.utils.AbstractCompactionEstimator;
import org.apache.iotdb.db.engine.compaction.task.ICompactionSelector;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

public class RewriteCrossSpaceCompactionSelector implements ICrossSpaceSelector {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static boolean hasPrintLog = false;
  private final int SELECT_WARN_THRESHOLD = 10;
  protected String logicalStorageGroupName;
  protected String dataRegionId;
  protected long timePartition;
  protected TsFileManager tsFileManager;

  private CrossSpaceCompactionResource resource;

  private long totalCost;
  private long totalSize;
  private final long memoryBudget;
  private final int maxCrossCompactionFileNum;
  private final long maxCrossCompactionFileSize;

  private List<TsFileResource> selectedUnseqFiles;
  private List<TsFileResource> selectedSeqFiles;

  private Collection<Integer> tmpSelectedSeqFiles;

  private boolean[] seqSelected;
  private int seqSelectedNum;

  private AbstractCompactionEstimator compactionEstimator;

  public RewriteCrossSpaceCompactionSelector(
      String logicalStorageGroupName,
      String dataRegionId,
      long timePartition,
      TsFileManager tsFileManager) {
    this.logicalStorageGroupName = logicalStorageGroupName;
    this.dataRegionId = dataRegionId;
    this.timePartition = timePartition;
    this.tsFileManager = tsFileManager;
    this.memoryBudget =
        (long)
            ((double)
                    (SystemInfo.getInstance().getMemorySizeForCompaction()
                        / IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount())
                * config.getUsableCompactionMemoryProportion());
    this.maxCrossCompactionFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxCrossCompactionCandidateFileNum();
    this.maxCrossCompactionFileSize =
        IoTDBDescriptor.getInstance().getConfig().getMaxCrossCompactionCandidateFileSize();

    this.compactionEstimator =
        ICompactionSelector.getCompactionEstimator(
            IoTDBDescriptor.getInstance().getConfig().getCrossCompactionPerformer(), false);
  }

  /**
   * Select merge candidates from seqFiles and unseqFiles under the given memoryBudget. This process
   * iteratively adds the next unseqFile from unseqFiles and its overlapping seqFiles as newly-added
   * candidates and computes their estimated memory cost. If the current cost pluses the new cost is
   * still under the budget, accept the unseqFile and the seqFiles as candidates, otherwise go to
   * the next iteration. The memory cost of a file is calculated in two ways: The rough estimation:
   * for a seqFile, the size of its metadata is used for estimation. Since in the worst case, the
   * file only contains one timeseries and all its metadata will be loaded into memory with at most
   * one actual data chunk (which is negligible) and writing the timeseries into a new file generate
   * metadata of the similar size, so the size of all seqFiles' metadata (generated when writing new
   * chunks) pluses the largest one (loaded when reading a timeseries from the seqFiles) is the
   * total estimation of all seqFiles; for an unseqFile, since the merge reader may read all chunks
   * of a series to perform a merge read, the whole file may be loaded into memory, so we use the
   * file's length as the maximum estimation. The tight estimation: based on the rough estimation,
   * we scan the file's metadata to count the number of chunks for each series, find the series
   * which have the most chunks in the file and use its chunk proportion to refine the rough
   * estimation. The rough estimation is performed first, if no candidates can be found using rough
   * estimation, we run the selection again with tight estimation.
   *
   * @return two lists of TsFileResource, the former is selected seqFiles and the latter is selected
   *     unseqFiles or an empty array if there are no proper candidates by the budget.
   */
  private List[] select() throws MergeException {
    long startTime = System.currentTimeMillis();
    try {
      LOGGER.debug(
          "Selecting merge candidates from {} seqFile, {} unseqFiles",
          resource.getSeqFiles().size(),
          resource.getUnseqFiles().size());
      selectSourceFiles();
      if (selectedUnseqFiles.isEmpty()) {
        LOGGER.debug("No merge candidates are found");
        return new List[0];
      }
    } catch (IOException e) {
      throw new MergeException(e);
    } finally {
      try {
        compactionEstimator.clear();
      } catch (IOException e) {
        throw new MergeException(e);
      }
    }
    LOGGER.info(
        "Selected merge candidates, {} seqFiles, {} unseqFiles, total memory cost {}, "
            + "time consumption {}ms",
        selectedSeqFiles.size(),
        selectedUnseqFiles.size(),
        totalCost,
        System.currentTimeMillis() - startTime);

    return new List[] {selectedSeqFiles, selectedUnseqFiles};
  }

  /**
   * In a preset time (30 seconds), for each unseqFile, find the list of seqFiles that overlap with
   * it and have not been selected by the file selector of this compaction task. After finding each
   * unseqFile and its corresponding overlap seqFile list, estimate the additional memory overhead
   * that may be added by compacting them (preferably using the loop estimate), and if it does not
   * exceed the memory overhead preset by the system for the compaction thread, put them into the
   * selectedSeqFiles and selectedUnseqFiles.
   */
  void selectSourceFiles() throws IOException {
    tmpSelectedSeqFiles = new HashSet<>();
    seqSelected = new boolean[resource.getSeqFiles().size()];
    seqSelectedNum = 0;
    selectedSeqFiles = new ArrayList<>();
    selectedUnseqFiles = new ArrayList<>();

    totalCost = 0;
    totalSize = 0;

    int unseqIndex = 0;
    long startTime = System.currentTimeMillis();
    long timeConsumption = 0;
    long timeLimit =
        IoTDBDescriptor.getInstance().getConfig().getCrossCompactionFileSelectionTimeBudget();
    if (timeLimit < 0) {
      timeLimit = Long.MAX_VALUE;
    }
    while (unseqIndex < resource.getUnseqFiles().size() && timeConsumption < timeLimit) {
      // select next unseq files
      TsFileResource unseqFile = resource.getUnseqFiles().get(unseqIndex);
      if (!unseqFile.getTsFile().exists() || unseqFile.isDeleted()) {
        break;
      }

      if (seqSelectedNum != resource.getSeqFiles().size()) {
        selectOverlappedSeqFiles(unseqFile);
      }
      boolean isSeqFilesValid = checkIsSeqFilesValid();
      if (!isSeqFilesValid) {
        tmpSelectedSeqFiles.clear();
        break;
      }

      // Filter out the selected seq files
      for (int i = 0; i < seqSelected.length; i++) {
        if (seqSelected[i]) {
          tmpSelectedSeqFiles.remove(i);
        }
      }

      List<TsFileResource> tmpSelectedSeqFileResources = new ArrayList<>();
      for (int seqIndex : tmpSelectedSeqFiles) {
        TsFileResource tsFileResource = resource.getSeqFiles().get(seqIndex);
        tmpSelectedSeqFileResources.add(tsFileResource);
        totalSize += tsFileResource.getTsFileSize();
      }
      totalSize += unseqFile.getTsFileSize();

      long newCost =
          compactionEstimator.estimateCrossCompactionMemory(tmpSelectedSeqFileResources, unseqFile);
      if (!updateSelectedFiles(newCost, unseqFile)) {
        // older unseq files must be merged before newer ones
        break;
      }

      tmpSelectedSeqFiles.clear();
      unseqIndex++;
      timeConsumption = System.currentTimeMillis() - startTime;
    }
    for (int i = 0; i < seqSelected.length; i++) {
      if (seqSelected[i]) {
        selectedSeqFiles.add(resource.getSeqFiles().get(i));
      }
    }
  }

  private boolean updateSelectedFiles(long newCost, TsFileResource unseqFile) {
    if (selectedUnseqFiles.size() == 0
        || (seqSelectedNum + selectedUnseqFiles.size() + 1 + tmpSelectedSeqFiles.size()
                <= maxCrossCompactionFileNum
            && totalSize <= maxCrossCompactionFileSize
            && totalCost + newCost < memoryBudget)) {
      selectedUnseqFiles.add(unseqFile);

      for (Integer seqIdx : tmpSelectedSeqFiles) {
        if (!seqSelected[seqIdx]) {
          seqSelectedNum++;
          seqSelected[seqIdx] = true;
        }
      }
      totalCost += newCost;
      LOGGER.debug(
          "Adding a new unseqFile {} and seqFiles {} as candidates, new cost {}, total"
              + " cost {}",
          unseqFile,
          tmpSelectedSeqFiles,
          newCost,
          totalCost);
      return true;
    }
    return false;
  }

  /**
   * To avoid redundant data in seq files, cross space compaction should select all the seq files
   * which have overlap with unseq files whether they are compacting or not. Therefore, before
   * adding task into the queue, cross space compaction task should check whether source seq files
   * are being compacted or not to speed up compaction.
   */
  private boolean checkIsSeqFilesValid() {
    for (Integer seqIdx : tmpSelectedSeqFiles) {
      if (resource.getSeqFiles().get(seqIdx).isCompactionCandidate()
          || resource.getSeqFiles().get(seqIdx).isCompacting()
          || !resource.getSeqFiles().get(seqIdx).isClosed()
          || !resource.getSeqFiles().get(seqIdx).getTsFile().exists()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Put the index of the seqFile that has an overlap with the specific unseqFile and has not been
   * selected by the file selector of the compaction task into the tmpSelectedSeqFiles list. To
   * determine whether overlap exists is to traverse each device ChunkGroup in unseqFiles, and
   * determine whether it overlaps with the same device ChunkGroup of each seqFile that are not
   * selected by the compaction task, if so, select this seqFile.
   *
   * @param unseqFile the tsFileResource of unseqFile to be compacted
   */
  private void selectOverlappedSeqFiles(TsFileResource unseqFile) {
    for (String deviceId : unseqFile.getDevices()) {
      long unseqStartTime = unseqFile.getStartTime(deviceId);
      long unseqEndTime = unseqFile.getEndTime(deviceId);

      boolean noMoreOverlap = false;
      for (int i = 0; i < resource.getSeqFiles().size() && !noMoreOverlap; i++) {
        TsFileResource seqFile = resource.getSeqFiles().get(i);
        if (!seqFile.mayContainsDevice(deviceId)) {
          continue;
        }

        int crossSpaceCompactionTimes = 0;
        try {
          TsFileNameGenerator.TsFileName tsFileName =
              TsFileNameGenerator.getTsFileName(seqFile.getTsFile().getName());
          crossSpaceCompactionTimes = tsFileName.getCrossCompactionCnt();
        } catch (IOException e) {
          LOGGER.warn("Meets IOException when selecting files for cross space compaction", e);
        }

        long seqEndTime = seqFile.getEndTime(deviceId);
        long seqStartTime = seqFile.getStartTime(deviceId);
        if (!seqFile.isClosed()) {
          // for unclosed file, only select those that overlap with the unseq file
          if (unseqEndTime >= seqStartTime) {
            tmpSelectedSeqFiles.add(i);
            if (crossSpaceCompactionTimes >= SELECT_WARN_THRESHOLD) {
              LOGGER.warn(
                  "{} is selected for cross space compaction, it is overlapped with {} in device {}. Sequence file time range:[{},{}], Unsequence file time range:[{},{}]",
                  seqFile.getTsFile().getAbsolutePath(),
                  unseqFile.getTsFile().getAbsolutePath(),
                  deviceId,
                  seqStartTime,
                  seqEndTime,
                  unseqStartTime,
                  unseqEndTime);
            }
          }
        } else if (unseqEndTime <= seqEndTime) {
          // if time range in unseq file is 10-20, seq file is 30-40, or
          // time range in unseq file is 10-20, seq file is 15-25, then select this seq file and
          // there is no more overlap later.
          tmpSelectedSeqFiles.add(i);
          noMoreOverlap = true;
          if (crossSpaceCompactionTimes >= SELECT_WARN_THRESHOLD) {
            LOGGER.warn(
                "{} is selected for cross space compaction, it is overlapped with {} in device {}. Sequence file time range:[{},{}], Unsequence file time range:[{},{}]",
                seqFile.getTsFile().getAbsolutePath(),
                unseqFile.getTsFile().getAbsolutePath(),
                deviceId,
                seqStartTime,
                seqEndTime,
                unseqStartTime,
                unseqEndTime);
          }
        } else if (unseqStartTime <= seqEndTime) {
          // if time range in unseq file is 10-20, seq file is 0-15, then select this seq file and
          // there may be overlap later.
          tmpSelectedSeqFiles.add(i);
          if (crossSpaceCompactionTimes >= SELECT_WARN_THRESHOLD) {
            LOGGER.warn(
                "{} is selected for cross space compaction, it is overlapped with {} in device {}. Sequence file time range:[{},{}], Unsequence file time range:[{},{}]",
                seqFile.getTsFile().getAbsolutePath(),
                unseqFile.getTsFile().getAbsolutePath(),
                deviceId,
                seqStartTime,
                seqEndTime,
                unseqStartTime,
                unseqEndTime);
          }
        }
      }
    }
  }

  /**
   * This method creates a specific file selector according to the file selection strategy of
   * crossSpace compaction, uses the file selector to select all unseqFiles and seqFiles to be
   * compacted under the time partition of the data region, and creates a compaction task for them.
   * The task is put into the compactionTaskQueue of the {@link CompactionTaskManager}.
   *
   * @return Returns whether the file was found and submits the merge task
   */
  @Override
  public List selectCrossSpaceTask(
      List<TsFileResource> sequenceFileList, List<TsFileResource> unsequenceFileList) {
    if ((CompactionTaskManager.currentTaskNum.get() >= config.getCompactionThreadCount())
        || (!config.isEnableCrossSpaceCompaction())) {
      return Collections.emptyList();
    }
    Iterator<TsFileResource> seqIterator = sequenceFileList.iterator();
    Iterator<TsFileResource> unSeqIterator = unsequenceFileList.iterator();
    List<TsFileResource> seqFileList = new ArrayList<>();
    List<TsFileResource> unSeqFileList = new ArrayList<>();
    while (seqIterator.hasNext()) {
      seqFileList.add(seqIterator.next());
    }
    while (unSeqIterator.hasNext()) {
      unSeqFileList.add(unSeqIterator.next());
    }
    if (seqFileList.isEmpty() || unSeqFileList.isEmpty()) {
      return Collections.emptyList();
    }
    long timeLowerBound = System.currentTimeMillis() - Long.MAX_VALUE;
    this.resource = new CrossSpaceCompactionResource(seqFileList, unSeqFileList, timeLowerBound);

    try {
      List[] mergeFiles = select();
      if (mergeFiles.length == 0) {
        if (!hasPrintLog) {
          LOGGER.info(
              "{} [Compaction] Cannot select any files, because source files may be occupied by other compaction threads.",
              logicalStorageGroupName + "-" + dataRegionId);
          hasPrintLog = true;
        }
        return Collections.emptyList();
      }
      hasPrintLog = false;

      if (mergeFiles[0].size() > 0 && mergeFiles[1].size() > 0) {
        LOGGER.info(
            "{} [Compaction] submit a task with {} sequence file and {} unseq files",
            logicalStorageGroupName + "-" + dataRegionId,
            mergeFiles[0].size(),
            mergeFiles[1].size());
        return Collections.singletonList(new Pair<>(mergeFiles[0], mergeFiles[1]));
      }

    } catch (MergeException e) {
      LOGGER.error("{} cannot select file for cross space compaction", logicalStorageGroupName, e);
    }
    return Collections.emptyList();
  }

  @Override
  public List<Long> getCompactionMemoryCost() {
    return Collections.singletonList(totalCost);
  }
}
