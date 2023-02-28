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
package org.apache.iotdb.db.engine.compaction.selector.impl;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.selector.ICompactionSelector;
import org.apache.iotdb.db.engine.compaction.selector.ICrossSpaceSelector;
import org.apache.iotdb.db.engine.compaction.selector.estimator.AbstractCompactionEstimator;
import org.apache.iotdb.db.engine.compaction.selector.utils.CrossCompactionTaskResource;
import org.apache.iotdb.db.engine.compaction.selector.utils.CrossSpaceCompactionCandidate;
import org.apache.iotdb.db.engine.compaction.selector.utils.CrossSpaceCompactionCandidate.CrossCompactionTaskResourceSplit;
import org.apache.iotdb.db.engine.compaction.selector.utils.CrossSpaceCompactionCandidate.TsFileResourceCandidate;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.db.rescon.SystemInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

  private final long memoryBudget;
  private final int maxCrossCompactionFileNum;
  private final long maxCrossCompactionFileSize;

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
            ((double) SystemInfo.getInstance().getMemorySizeForCompaction()
                / IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount()
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
  private CrossCompactionTaskResource selectOneTaskResources(
      CrossSpaceCompactionCandidate candidate) throws MergeException {
    try {
      LOGGER.debug(
          "Selecting cross compaction task resources from {} seqFile, {} unseqFiles",
          candidate.getSeqFiles().size(),
          candidate.getUnseqFiles().size());
      CrossCompactionTaskResource taskResource = executeTaskResourceSelection(candidate);

      return taskResource;
    } catch (IOException e) {
      throw new MergeException(e);
    } finally {
      try {
        compactionEstimator.clear();
      } catch (IOException e) {
        throw new MergeException(e);
      }
    }
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
   */
  private CrossCompactionTaskResource executeTaskResourceSelection(
      CrossSpaceCompactionCandidate candidate) throws IOException {
    CrossCompactionTaskResource taskResource = new CrossCompactionTaskResource();

    while (candidate.hasNextSplit()) {
      CrossCompactionTaskResourceSplit split = candidate.nextSplit();
      TsFileResource unseqFile = split.unseqFile.resource;
      List<TsFileResource> targetSeqFiles =
          split.seqFiles.stream().map(c -> c.resource).collect(Collectors.toList());
      long memoryCost =
          compactionEstimator.estimateCrossCompactionMemory(targetSeqFiles, unseqFile);
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

  // TODO: (xingtanzjr) need to confirm whether we should strictly guarantee the conditions
  // If we guarantee the condition strictly, the smallest collection of cross task resource may not
  // satisfied
  private boolean canAddToTaskResource(
      CrossCompactionTaskResource taskResource,
      TsFileResource unseqFile,
      List<TsFileResource> seqFiles,
      long memoryCost)
      throws IOException {
    TsFileNameGenerator.TsFileName unseqFileName =
        TsFileNameGenerator.getTsFileName(unseqFile.getTsFile().getName());
    // we add a hard limit for cross compaction that selected unseqFile should be compacted in inner
    // space at least once. This is used to make to improve the priority of inner compaction and
    // avoid too much cross compaction with small files.
    if (unseqFileName.getInnerCompactionCnt() < config.getMinCrossCompactionUnseqFileLevel()) {
      return false;
    }
    // currently, we must allow at least one unseqFile be selected to handle the situation that
    // an unseqFile has huge time range but few data points.
    // IMPORTANT: this logic is opposite to previous level control
    if (taskResource.getUnseqFiles().isEmpty()) {
      return true;
    }
    long totalFileSize = unseqFile.getTsFileSize();
    for (TsFileResource f : seqFiles) {
      totalFileSize += f.getTsFileSize();
    }
    if (taskResource.getTotalFileNums() + 1 + seqFiles.size() <= maxCrossCompactionFileNum
        && taskResource.getTotalFileSize() + totalFileSize <= maxCrossCompactionFileSize
        && taskResource.getTotalMemoryCost() + memoryCost < memoryBudget) {
      return true;
    }
    return false;
  }

  private boolean canSubmitCrossTask(
      List<TsFileResource> sequenceFileList, List<TsFileResource> unsequenceFileList) {
    return CompactionTaskManager.getInstance().getCompactionCandidateTaskCount()
            < config.getCandidateCompactionTaskQueueSize()
        && !sequenceFileList.isEmpty()
        && !unsequenceFileList.isEmpty();
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
      List<TsFileResource> sequenceFileList, List<TsFileResource> unsequenceFileList) {
    if (!canSubmitCrossTask(sequenceFileList, unsequenceFileList)) {
      return Collections.emptyList();
    }

    // TODO: (xingtanzjr) need to confirm what this ttl is used for
    long startTime = System.currentTimeMillis();
    long ttlLowerBound = System.currentTimeMillis() - Long.MAX_VALUE;
    // we record the variable `candidate` here is used for selecting more than one
    // CrossCompactionTaskResources in this method
    CrossSpaceCompactionCandidate candidate =
        new CrossSpaceCompactionCandidate(sequenceFileList, unsequenceFileList, ttlLowerBound);
    try {
      CrossCompactionTaskResource taskResources = selectOneTaskResources(candidate);
      if (!taskResources.isValid()) {
        if (!hasPrintedLog) {
          LOGGER.info(
              "{} [Compaction] Total source files: {} seqFiles, {} unseqFiles. Candidate source files: {} seqFiles, {} unseqFiles. Cannot select any files because they do not meet the conditions or may be occupied by other compaction threads.",
              logicalStorageGroupName + "-" + dataRegionId,
              sequenceFileList.size(),
              unsequenceFileList.size(),
              candidate.getSeqFiles().size(),
              candidate.getUnseqFiles().size());
          hasPrintedLog = true;
        }
        return Collections.emptyList();
      }
      LOGGER.info(
          "{} [Compaction] Total source files: {} seqFiles, {} unseqFiles. Candidate source files: {} seqFiles, {} unseqFiles. Selected source files: {} seqFiles, {} unseqFiles, total memory cost {}, time consumption {}ms.",
          logicalStorageGroupName + "-" + dataRegionId,
          sequenceFileList.size(),
          unsequenceFileList.size(),
          candidate.getSeqFiles().size(),
          candidate.getUnseqFiles().size(),
          taskResources.getSeqFiles().size(),
          taskResources.getUnseqFiles().size(),
          taskResources.getTotalMemoryCost(),
          System.currentTimeMillis() - startTime);
      hasPrintedLog = false;
      return Collections.singletonList(taskResources);

    } catch (MergeException e) {
      LOGGER.error("{} cannot select file for cross space compaction", logicalStorageGroupName, e);
    }
    return Collections.emptyList();
  }
}
