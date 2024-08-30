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

package org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.db.service.metrics.CompactionMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleContext;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.ICompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.ICrossSpaceSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.AbstractCrossSpaceEstimator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.CompactionEstimateUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.CrossCompactionTaskResource;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.CrossSpaceCompactionCandidate;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.DeviceInfo;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.InsertionCompactionCandidateStatus;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.InsertionCrossCompactionTaskResource;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.TsFileResourceCandidate;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;

import org.apache.tsfile.exception.StopReadTsFileByInterruptException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.TimeRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class RewriteCrossSpaceCompactionSelector implements ICrossSpaceSelector {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  protected String logicalStorageGroupName;
  protected String dataRegionId;
  protected long timePartition;
  protected TsFileManager tsFileManager;

  private static boolean hasPrintedLog = false;
  private static int maxDeserializedFileNumToCheckInsertionCandidateValid = 500;

  private final long memoryBudget;
  private final int maxCrossCompactionFileNum;
  private final long maxCrossCompactionFileSize;

  private final AbstractCrossSpaceEstimator compactionEstimator;
  private CompactionScheduleContext context;

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
            ((double) SystemInfo.getInstance().getMemorySizeForCompaction()
                / IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount()
                * config.getUsableCompactionMemoryProportion());
    this.maxCrossCompactionFileNum =
        IoTDBDescriptor.getInstance().getConfig().getFileLimitPerCrossTask();
    this.maxCrossCompactionFileSize =
        IoTDBDescriptor.getInstance().getConfig().getMaxCrossCompactionCandidateFileSize();

    this.compactionEstimator =
        (AbstractCrossSpaceEstimator)
            ICompactionSelector.getCompactionEstimator(
                IoTDBDescriptor.getInstance().getConfig().getCrossCompactionPerformer(), false);
    this.context = null;
  }

  public RewriteCrossSpaceCompactionSelector(
      String logicalStorageGroupName,
      String dataRegionId,
      long timePartition,
      TsFileManager tsFileManager,
      CompactionScheduleContext context) {
    this(logicalStorageGroupName, dataRegionId, timePartition, tsFileManager);
    this.context = context;
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
   * @throws MergeException in task resources selection.
   */
  @SuppressWarnings({"squid:S1163", "squid:S1143"})
  public CrossCompactionTaskResource selectOneTaskResources(CrossSpaceCompactionCandidate candidate)
      throws MergeException {
    if (candidate.getSeqFiles().isEmpty() || candidate.getUnseqFiles().isEmpty()) {
      return new CrossCompactionTaskResource();
    }
    try {
      LOGGER.debug(
          "Selecting cross compaction task resources from {} seqFile, {} unseqFiles",
          candidate.getSeqFiles().size(),
          candidate.getUnseqFiles().size());

      return executeTaskResourceSelection(candidate);
    } catch (Exception e) {
      if (e instanceof StopReadTsFileByInterruptException || Thread.interrupted()) {
        Thread.currentThread().interrupt();
        return new CrossCompactionTaskResource();
      }
      throw new MergeException(e);
    } finally {
      compactionEstimator.cleanup();
    }
  }

  public InsertionCrossCompactionTaskResource selectOneInsertionTask(
      CrossSpaceCompactionCandidate candidate) throws MergeException {
    if (candidate.getUnseqFileCandidates().isEmpty()) {
      return new InsertionCrossCompactionTaskResource();
    }
    InsertionCrossSpaceCompactionSelector insertionCrossSpaceCompactionSelector =
        new InsertionCrossSpaceCompactionSelector(candidate);
    try {
      LOGGER.debug(
          "Selecting insertion cross compaction task resources from {} seqFile, {} unseqFiles",
          candidate.getSeqFiles().size(),
          candidate.getUnseqFiles().size());
      InsertionCrossCompactionTaskResource result =
          insertionCrossSpaceCompactionSelector.executeInsertionCrossSpaceCompactionTaskSelection();
      if (result.isValid()) {
        return result;
      }
    } catch (IOException e) {
      throw new MergeException(e);
    }
    return new InsertionCrossCompactionTaskResource();
  }

  private boolean isAllFileCandidateValid(List<TsFileResourceCandidate> tsFileResourceCandidates) {
    for (TsFileResourceCandidate candidate : tsFileResourceCandidates) {
      if (!candidate.isValidCandidate) {
        return false;
      }
    }
    return true;
  }

  /**
   * In a preset time (30 seconds), for each unseqFile, find the list of seqFiles that overlap with
   * it and have not been selected by the file selector of this compaction task. After finding each
   * unseqFile and its corresponding overlap seqFile list, estimate the additional memory overhead
   * that may be added by compacting them (preferably using the loop estimate), and if it does not
   * exceed the memory overhead preset by the system for the compaction thread, put them into the
   * selectedSeqFiles and selectedUnseqFiles.
   *
   * @throws IOException in prepare next split
   */
  @SuppressWarnings("squid:S135")
  private CrossCompactionTaskResource executeTaskResourceSelection(
      CrossSpaceCompactionCandidate candidate) throws IOException {
    CrossCompactionTaskResource taskResource = new CrossCompactionTaskResource();

    while (candidate.hasNextSplit()) {
      CrossSpaceCompactionCandidate.CrossCompactionTaskResourceSplit split = candidate.nextSplit();
      TsFileResource unseqFile = split.unseqFile.resource;
      List<TsFileResource> targetSeqFiles =
          split.seqFiles.stream().map(c -> c.resource).collect(Collectors.toList());

      if (!split.atLeastOneSeqFileSelected) {
        LOGGER.debug("Unseq file {} does not overlap with any seq files.", unseqFile);
        TsFileResourceCandidate latestSealedSeqFile =
            getLatestSealedSeqFile(candidate.getSeqFileCandidates());
        if (latestSealedSeqFile == null) {
          break;
        }
        if (!latestSealedSeqFile.selected) {
          targetSeqFiles.add(latestSealedSeqFile.resource);
          latestSealedSeqFile.markAsSelected();
        }
      }

      List<TsFileResource> newSelectedSeqResources = new ArrayList<>(taskResource.getSeqFiles());
      newSelectedSeqResources.addAll(targetSeqFiles);
      List<TsFileResource> newSelectedUnseqResources =
          new ArrayList<>(taskResource.getUnseqFiles());
      newSelectedUnseqResources.add(unseqFile);

      long roughEstimatedMemoryCost =
          compactionEstimator.roughEstimateCrossCompactionMemory(
              newSelectedSeqResources, newSelectedUnseqResources);
      long memoryCost =
          CompactionEstimateUtils.shouldAccurateEstimate(roughEstimatedMemoryCost)
              ? roughEstimatedMemoryCost
              : compactionEstimator.estimateCrossCompactionMemory(
                  newSelectedSeqResources, newSelectedUnseqResources);
      if (!canAddToTaskResource(taskResource, unseqFile, targetSeqFiles, memoryCost)) {
        break;
      }
      taskResource.putResources(unseqFile, targetSeqFiles, memoryCost);
      LOGGER.debug(
          "Adding a new unseqFile {} and seqFiles {} as candidates, new cost {}, total cost {}",
          unseqFile,
          targetSeqFiles,
          memoryCost,
          taskResource.getTotalMemoryCost());
    }
    taskResource.sortSeqFiles(candidate.getSeqFiles());
    return taskResource;
  }

  private TsFileResourceCandidate getLatestSealedSeqFile(
      List<TsFileResourceCandidate> seqResourceCandidateList) {
    for (int i = seqResourceCandidateList.size() - 1; i >= 0; i--) {
      TsFileResourceCandidate seqResourceCandidate = seqResourceCandidateList.get(i);
      if (seqResourceCandidate.resource.isClosed()) {
        // We must select the latest sealed and valid seq file to compact with, in order to avoid
        // overlapping of the new compacted files with the subsequent seq files.
        if (seqResourceCandidate.isValidCandidate) {
          LOGGER.debug(
              "Select one valid seq file {} for nonOverlap unseq file to compact with.",
              seqResourceCandidate.resource);
          return seqResourceCandidate;
        }
        break;
      }
    }
    return null;
  }

  // TODO: (xingtanzjr) need to confirm whether we should strictly guarantee the conditions
  // If we guarantee the condition strictly, the smallest collection of cross task resource may not
  // satisfied
  @SuppressWarnings("squid:S1135")
  private boolean canAddToTaskResource(
      CrossCompactionTaskResource taskResource,
      TsFileResource unseqFile,
      List<TsFileResource> seqFiles,
      long memoryCost)
      throws IOException {
    if (memoryCost == -1) {
      // there is file been deleted during selection
      return false;
    }
    TsFileNameGenerator.TsFileName unseqFileName =
        TsFileNameGenerator.getTsFileName(unseqFile.getTsFile().getName());
    long targetCompactionFileSize = config.getTargetCompactionFileSize();
    // we add a hard limit for cross compaction that selected unseqFile should reach a certain size
    // or be compacted in inner
    // space at least once. This is used to make to improve the priority of inner compaction and
    // avoid too much cross compaction with small files.
    if (unseqFile.getTsFileSize() < targetCompactionFileSize
        && unseqFileName.getInnerCompactionCnt() < config.getMinCrossCompactionUnseqFileLevel()) {
      return false;
    }

    long totalFileSize = unseqFile.getTsFileSize();
    for (TsFileResource f : seqFiles) {
      if (f.getTsFileSize() >= targetCompactionFileSize * 1.5) {
        // to avoid serious write amplification caused by cross space compaction, we restrict that
        // seq files are no longer be compacted when the size reaches the threshold.
        return false;
      }
      totalFileSize += f.getTsFileSize();
    }

    // currently, we must allow at least one unseqFile be selected to handle the situation that
    // an unseqFile has huge time range but few data points.
    // IMPORTANT: this logic is opposite to previous level control
    if (taskResource.getUnseqFiles().isEmpty()) {
      return true;
    }

    return taskResource.getTotalFileNums() + 1 + seqFiles.size() <= maxCrossCompactionFileNum
        && taskResource.getTotalFileSize() + totalFileSize <= maxCrossCompactionFileSize
        && memoryCost < memoryBudget;
  }

  private boolean canSubmitCrossTask(
      List<TsFileResource> sequenceFileList, List<TsFileResource> unsequenceFileList) {
    return !sequenceFileList.isEmpty() && !unsequenceFileList.isEmpty();
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
  public List<CrossCompactionTaskResource> selectCrossSpaceTask(
      List<TsFileResource> sequenceFileList, List<TsFileResource> unsequenceFilelist) {
    return selectCrossSpaceTask(sequenceFileList, unsequenceFilelist, false);
  }

  public List<CrossCompactionTaskResource> selectInsertionCrossSpaceTask(
      List<TsFileResource> sequenceFileList, List<TsFileResource> unsequenceFileList) {
    return selectCrossSpaceTask(sequenceFileList, unsequenceFileList, true);
  }

  @SuppressWarnings({"squid:S1135", "squid:S2696"})
  public List<CrossCompactionTaskResource> selectCrossSpaceTask(
      List<TsFileResource> sequenceFileList,
      List<TsFileResource> unsequenceFileList,
      boolean isInsertionTask) {
    // TODO: (xingtanzjr) need to confirm what this ttl is used for
    long startTime = System.currentTimeMillis();
    long ttlLowerBound = System.currentTimeMillis() - Long.MAX_VALUE;
    // we record the variable `candidate` here is used for selecting more than one
    // CrossCompactionTaskResources in this method.
    // Add read lock for candidate source files to avoid being deleted during the selection.
    CrossSpaceCompactionCandidate candidate =
        new CrossSpaceCompactionCandidate(
            sequenceFileList, unsequenceFileList, ttlLowerBound, context);
    try {
      CrossCompactionTaskResource taskResources;
      if (isInsertionTask) {
        taskResources = selectOneInsertionTask(candidate);
      } else {
        taskResources = selectOneTaskResources(candidate);
      }
      String sgDataRegionId = logicalStorageGroupName + "-" + dataRegionId;
      if (!taskResources.isValid()) {
        if (!hasPrintedLog) {
          LOGGER.info(
              "{} [{}] Total source files: {} seqFiles, {} unseqFiles. "
                  + "Candidate source files: {} seqFiles, {} unseqFiles. "
                  + "Cannot select any files because they do not meet the conditions "
                  + "or may be occupied by other compaction threads.",
              isInsertionTask ? "InsertionCrossSpaceCompaction" : "CrossSpaceCompaction",
              sgDataRegionId,
              sequenceFileList.size(),
              unsequenceFileList.size(),
              candidate.getSeqFiles().size(),
              candidate.getUnseqFiles().size());
          hasPrintedLog = true;
        }
        return Collections.emptyList();
      }
      long timeCost = System.currentTimeMillis() - startTime;
      LOGGER.info(
          "{} [{}] Total source files: {} seqFiles, {} unseqFiles. "
              + "Candidate source files: {} seqFiles, {} unseqFiles. "
              + "Selected source files: {} seqFiles, "
              + "{} unseqFiles, estimated memory cost {} MB, "
              + "total selected file size is {} MB, "
              + "total selected seq file size is {} MB, "
              + "total selected unseq file size is {} MB, "
              + "time consumption {}ms.",
          sgDataRegionId,
          isInsertionTask ? "InsertionCrossSpaceCompaction" : "CrossSpaceCompaction",
          sequenceFileList.size(),
          unsequenceFileList.size(),
          candidate.getSeqFiles().size(),
          candidate.getUnseqFiles().size(),
          taskResources.getSeqFiles().size(),
          taskResources.getUnseqFiles().size(),
          (float) (taskResources.getTotalMemoryCost()) / 1024 / 1024,
          (float) (taskResources.getTotalFileSize()) / 1024 / 1024,
          taskResources.getTotalSeqFileSize() / 1024 / 1024,
          taskResources.getTotalUnseqFileSize() / 1024 / 1024,
          timeCost);
      CompactionMetrics.getInstance()
          .updateCompactionTaskSelectionTimeCost(
              isInsertionTask ? CompactionTaskType.INSERTION : CompactionTaskType.CROSS, timeCost);
      hasPrintedLog = false;
      return Collections.singletonList(taskResources);

    } catch (MergeException e) {
      // This exception may be caused by drop database
      if (!tsFileManager.isAllowCompaction()) {
        return Collections.emptyList();
      }
      LOGGER.error("{} cannot select file for cross space compaction", logicalStorageGroupName, e);
    }
    return Collections.emptyList();
  }

  public static class InsertionCrossSpaceCompactionSelector {

    private List<TsFileResourceCandidate> seqFiles;
    private List<TsFileResourceCandidate> unseqFiles;

    public InsertionCrossSpaceCompactionSelector(CrossSpaceCompactionCandidate candidate) {
      seqFiles = candidate.getSeqFileCandidates();
      unseqFiles = candidate.getUnseqFileCandidates();
    }

    private InsertionCrossCompactionTaskResource executeInsertionCrossSpaceCompactionTaskSelection()
        throws IOException {
      InsertionCrossCompactionTaskResource result = new InsertionCrossCompactionTaskResource();
      if (unseqFiles.isEmpty()) {
        return result;
      }
      if (seqFiles.isEmpty()) {
        result.toInsertUnSeqFile = unseqFiles.get(0).resource;
        // ensure the target position is the head of seq space
        result.targetFileTimestamp =
            Math.min(System.currentTimeMillis(), getTimestampInFileName(unseqFiles.get(0)));
      } else {
        for (int i = 0; i < unseqFiles.size(); i++) {
          TsFileResourceCandidate unseqFile = unseqFiles.get(i);
          // skip unseq file which is overlapped with files in seq space or overlapped with previous
          // unseq files
          if (!isValidInsertionCompactionCandidate(unseqFiles, i)) {
            continue;
          }
          result = selectCurrentUnSeqFile(unseqFile);
          if (result.isValid()) {
            break;
          }
        }
      }
      // select the first unseq file to exclude other CrossSpaceCompactionTask
      // or InsertionCrossSpaceCompactionTask in current time partition
      TsFileResourceCandidate firstUnseqFile = unseqFiles.get(0);
      result.firstUnSeqFileInParitition = firstUnseqFile.resource;
      return result;
    }

    private InsertionCrossCompactionTaskResource selectCurrentUnSeqFile(
        TsFileResourceCandidate unseqFile) throws IOException {
      int previousSeqFileIndex = 0;
      int nextSeqFileIndex = seqFiles.size();
      InsertionCrossCompactionTaskResource result = new InsertionCrossCompactionTaskResource();

      boolean hasPreviousSeqFile = false;
      for (DeviceInfo unseqDeviceInfo : unseqFile.getDeviceInfoList()) {
        IDeviceID deviceId = unseqDeviceInfo.deviceId;
        long startTimeOfUnSeqDevice = unseqDeviceInfo.startTime;
        long endTimeOfUnSeqDevice = unseqDeviceInfo.endTime;
        for (int i = 0; i < seqFiles.size(); i++) {
          TsFileResourceCandidate seqFile = seqFiles.get(i);
          if (seqFile.unsealed()) {
            nextSeqFileIndex = Math.min(nextSeqFileIndex, i);
          }
          if (!seqFile.containsDevice(deviceId)) {
            continue;
          }
          DeviceInfo seqDeviceInfo = seqFile.getDeviceInfoById(deviceId);
          long startTimeOfSeqDevice = seqDeviceInfo.startTime;
          long endTimeOfSeqDevice = seqDeviceInfo.endTime;

          // overlap
          if (startTimeOfUnSeqDevice <= endTimeOfSeqDevice
              && endTimeOfUnSeqDevice >= startTimeOfSeqDevice) {
            unseqFile.resource.setInsertionCompactionTaskCandidate(
                InsertionCompactionCandidateStatus.NOT_VALID);
            return result;
          }

          if (startTimeOfUnSeqDevice > endTimeOfSeqDevice) {
            previousSeqFileIndex = Math.max(previousSeqFileIndex, i);
            hasPreviousSeqFile = true;
            continue;
          }
          nextSeqFileIndex = Math.min(nextSeqFileIndex, i);
          break;
        }
      }

      // select position to insert
      if (hasPreviousSeqFile) {
        boolean insertLastInSeqSpace =
            nextSeqFileIndex == seqFiles.size() && previousSeqFileIndex == seqFiles.size() - 1;
        if (insertLastInSeqSpace) {
          TsFileResourceCandidate prev = seqFiles.get(previousSeqFileIndex);
          long prevTimestamp = getTimestampInFileName(prev);
          if (prev.isValidCandidate) {
            result.prevSeqFile = prev.resource;
            result.targetFileTimestamp = prevTimestamp + 1;
            result.toInsertUnSeqFile = unseqFile.resource;
          }
          return result;
        }
        // insert the TsFileResource between 'prev' and 'next' in seq space
        for (int i = previousSeqFileIndex;
            i < Math.min(nextSeqFileIndex, seqFiles.size() - 1);
            i++) {
          TsFileResourceCandidate prev = seqFiles.get(i);
          TsFileResourceCandidate next = seqFiles.get(i + 1);
          if (prev.isValidCandidate && next.isValidCandidate) {
            long prevTimestamp = getTimestampInFileName(prev);
            long nextTimestamp = getTimestampInFileName(next);
            if (nextTimestamp - prevTimestamp > 1) {
              result.prevSeqFile = prev.resource;
              result.nextSeqFile = next.resource;
              result.targetFileTimestamp =
                  prevTimestamp + Math.max(1, (nextTimestamp - prevTimestamp) / 2);
              result.toInsertUnSeqFile = unseqFile.resource;
              break;
            }
          }
        }

      } else {
        // insert the TsFileResource to the head of seq space
        TsFileResourceCandidate next = seqFiles.get(0);
        long nextTimestamp = getTimestampInFileName(next);
        if (nextTimestamp < 1) {
          return result;
        }
        result.nextSeqFile = next.resource;
        result.targetFileTimestamp = nextTimestamp / 2;
        result.toInsertUnSeqFile = unseqFile.resource;
      }
      return result;
    }

    private boolean isValidInsertionCompactionCandidate(
        List<TsFileResourceCandidate> unseqFiles, int selectedUnseqFileIndex) throws IOException {
      TsFileResourceCandidate selectedUnseqFile = unseqFiles.get(selectedUnseqFileIndex);
      InsertionCompactionCandidateStatus status =
          selectedUnseqFile.resource.getInsertionCompactionCandidateStatus();
      if (status == InsertionCompactionCandidateStatus.NOT_VALID) {
        return false;
      }
      // The selected unseq file should not overlap with the previous unseq file
      // Example:
      // seq files:
      // 1-1-0-0.tsfile (device timestamps: [1-5]) 10-2-0-0.tsfile (device timestamps:[30-40])
      // unseq files:
      // 20-3-0-0.tsfile (device timestamps: [1-20]) 30-4-0-0.tsfile (device timestamps: [15-20])
      // If 30-40-0-0.tsfile is moved to the sequential area and a merge is triggered later, a
      // target file 1-1-1-0.tsfile will be generated.
      // Then, the version of the data originally in 30-4-0-0.tsfile changes to 1, and will be
      // overwritten by the overlapping data in 20-3-0-0.tsfile during query.
      if (status != InsertionCompactionCandidateStatus.NOT_CHECKED) {
        return true;
      }
      for (int i = 0; i < selectedUnseqFileIndex; i++) {
        TsFileResourceCandidate unseqFile = unseqFiles.get(i);
        if (isOverlap(
            selectedUnseqFile,
            unseqFile,
            selectedUnseqFileIndex > maxDeserializedFileNumToCheckInsertionCandidateValid)) {
          selectedUnseqFile.resource.setInsertionCompactionTaskCandidate(
              InsertionCompactionCandidateStatus.NOT_VALID);
          return false;
        }
      }
      selectedUnseqFile.resource.setInsertionCompactionTaskCandidate(
          InsertionCompactionCandidateStatus.VALID);
      return true;
    }

    private boolean isOverlap(
        TsFileResourceCandidate candidate1,
        TsFileResourceCandidate candidate2,
        boolean loadDeviceTimeIndex)
        throws IOException {
      TimeRange timeRangeOfFile1 =
          new TimeRange(
              candidate1.resource.getFileStartTime(), candidate1.resource.getFileEndTime());
      TimeRange timeRangeOfFile2 =
          new TimeRange(
              candidate2.resource.getFileStartTime(), candidate2.resource.getFileEndTime());
      boolean fileTimeOverlap = timeRangeOfFile1.overlaps(timeRangeOfFile2);
      if (!fileTimeOverlap) {
        return false;
      }

      // TimeIndex may be degraded after this check, but it will not affect the correctness of task
      // selection
      boolean candidate1NeedDeserialize =
          !candidate1.hasDetailedDeviceInfo()
              && candidate1.resource.getTimeIndexType() == ITimeIndex.FILE_TIME_INDEX_TYPE;
      boolean candidate2NeedDeserialize =
          !candidate2.hasDetailedDeviceInfo()
              && candidate2.resource.getTimeIndexType() == ITimeIndex.FILE_TIME_INDEX_TYPE;
      if (!loadDeviceTimeIndex && (candidate1NeedDeserialize || candidate2NeedDeserialize)) {
        return true;
      }

      for (DeviceInfo device : candidate2.getDeviceInfoList()) {
        IDeviceID deviceId = device.deviceId;
        if (!candidate1.containsDevice(deviceId)) {
          continue;
        }
        DeviceInfo deviceInfoOfFile1 = candidate1.getDeviceInfoById(deviceId);
        DeviceInfo deviceInfoOfFile2 = candidate2.getDeviceInfoById(deviceId);

        if (new TimeRange(deviceInfoOfFile1.startTime, deviceInfoOfFile1.endTime)
            .overlaps(new TimeRange(deviceInfoOfFile2.startTime, deviceInfoOfFile2.endTime))) {
          return true;
        }
      }
      return false;
    }

    private long getTimestampInFileName(TsFileResourceCandidate tsFileResourceCandidate)
        throws IOException {
      return TsFileNameGenerator.getTsFileName(
              tsFileResourceCandidate.resource.getTsFile().getName())
          .getTime();
    }
  }
}
