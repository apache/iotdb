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

package org.apache.iotdb.db.engine.compaction.newcross.fragment.selector;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionPriority;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.newcross.AbstractCrossSpaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.newcross.fragment.task.FragmentCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.db.engine.compaction.CompactionScheduler.currentTaskNum;

public class FragmentCompactionSelector extends AbstractCrossSpaceCompactionSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(FragmentCompactionSelector.class);
  private String logicalStorageGroup;
  private String virtualStorageGroup;
  private long timePartition;
  private List<TsFileResource> sequenceFileList;
  private List<TsFileResource> unsequenceFileList;
  // cache the start time and end time for unseq file
  private Map<TsFileResource, Long> startTimeCachedForUnseqFile;
  private Map<TsFileResource, Long> endTimeCacheForUnseqFile;

  public FragmentCompactionSelector(
      String logicalStorageGroup,
      String virtualStorageGroup,
      long timePartition,
      List<TsFileResource> sequenceFileList,
      List<TsFileResource> unsequenceFileList) {
    super();
    this.logicalStorageGroup = logicalStorageGroup;
    this.virtualStorageGroup = virtualStorageGroup;
    this.timePartition = timePartition;
    this.sequenceFileList = new ArrayList<>(sequenceFileList);
    this.unsequenceFileList = new ArrayList<>(unsequenceFileList);
  }

  /**
   * This method select sequence files and unseqence files to be compacted. We first select a
   * sequence file, and select the unsequence files overlapped with it. We select the sequence file
   * from new to old, and select the unsequence file from old to new. Once we meet an unselectable
   * file (sequnece or unsequence), we will try to submit a compaction task. If the compaction
   * priority is BALANCE, this function will submit one task at most. Else it will continously
   * submit runnable task until the thread pool is full or not more runnable task can be found.
   *
   * @return submit task or not
   */
  @Override
  public boolean selectAndSubmit() {
    // sort sequenceFileList from new to old
    sequenceFileList.sort(
        (o1, o2) -> TsFileResource.compareFileName(o1.getTsFile(), o2.getTsFile()));
    Collections.reverse(sequenceFileList);
    // sort unsequenceFileLIst from old to new
    unsequenceFileList.sort(
        (o1, o2) -> TsFileResource.compareFileName(o1.getTsFile(), o2.getTsFile()));
    // SequenceFile -> List<UnseqFile>
    Map<TsFileResource, List<TsFileResource>> selectedFiles = new HashMap<>();
    boolean hasSelected = false;

    for (TsFileResource sequenceFile : sequenceFileList) {
      // if sequence file is unselectable, try to submit a compaction task
      if (sequenceFile.isMerging() || !sequenceFile.isClosed()) {
        hasSelected |= tryToSubmitTask(selectedFiles);
        if (currentTaskNum.get()
                >= IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread()
            || (hasSelected
                && IoTDBDescriptor.getInstance().getConfig().getCompactionPriority()
                    == CompactionPriority.BALANCE)) {
          return hasSelected;
        }
        continue;
      }
      for (TsFileResource unsequenceFile : unsequenceFileList) {
        if (isOverlapped(sequenceFile, unsequenceFile)) {
          // if unsequence file is overlapped with sequencefile and unselectalbe
          // try to submit a compaction task
          if (!unsequenceFile.isClosed() || unsequenceFile.isMerging()) {
            hasSelected |= tryToSubmitTask(selectedFiles);
            if (currentTaskNum.get()
                    >= IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread()
                || (hasSelected
                    && IoTDBDescriptor.getInstance().getConfig().getCompactionPriority()
                        == CompactionPriority.BALANCE)) {
              return hasSelected;
            }
            break;
          }
          // add the unsequence file to candidate file list
          selectedFiles.computeIfAbsent(sequenceFile, o -> new ArrayList<>()).add(unsequenceFile);
          if (selectedFiles.get(sequenceFile).size()
              > IoTDBDescriptor.getInstance()
                  .getConfig()
                  .getMaxOpenFileNumInCrossSpaceCompaction()) {
            hasSelected |= tryToSubmitTask(selectedFiles);
            if (currentTaskNum.get()
                    >= IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread()
                || (hasSelected
                    && IoTDBDescriptor.getInstance().getConfig().getCompactionPriority()
                        == CompactionPriority.BALANCE)) {
              return hasSelected;
            }
            break;
          }
        }
      }
      // if we select enough sequence file, submit a task
      if (selectedFiles.size()
          > IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum()) {
        hasSelected |= tryToSubmitTask(selectedFiles);
        if (currentTaskNum.get()
                >= IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread()
            || (hasSelected
                && IoTDBDescriptor.getInstance().getConfig().getCompactionPriority()
                    == CompactionPriority.BALANCE)) {
          return hasSelected;
        }
      }
    }

    return hasSelected;
  }

  /**
   * This function will try to submit a cross space compaction task to CompactionTaskManager. In the
   * following situation task will not be submited
   *
   * <ul>
   *   <li>The selectedFiles is empty
   *   <li>The currentTaskNum is greater than or equals to MAX_COMPACTION_THREAD_NUM
   * </ul>
   *
   * If a compaction task is submitted, selectedFiles will be clear
   *
   * @param selectedFiles The selected files to be compacted
   * @return submit a task or not
   */
  private boolean tryToSubmitTask(Map<TsFileResource, List<TsFileResource>> selectedFiles) {
    if (selectedFiles.size() == 0
        || currentTaskNum.get()
            >= IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread()) {
      LOGGER.info(
          "Selector did submit a compaction task, because no selected files or usable thread");
      return false;
    }
    CompactionTaskManager.getInstance()
        .submitTask(
            logicalStorageGroup + "-" + virtualStorageGroup,
            timePartition,
            new FragmentCompactionTask(
                new HashMap<>(selectedFiles),
                logicalStorageGroup + "-" + virtualStorageGroup,
                timePartition));
    LOGGER.info(
        "submit a inplace cross space compaction task with {} sequence files",
        selectedFiles.size());
    selectedFiles.clear();
    return true;
  }

  /**
   * Determine whether a sequence file is overlapped with an unsequence file
   *
   * @return true if they are overlapped else false
   */
  private boolean isOverlapped(TsFileResource sequenceFile, TsFileResource unsequenceFile) {
    long startTimeForSequenceFile = getStartTimeForTsFile(sequenceFile, false);
    long endTimeForSequenceFile = getEndTimeForTsFile(sequenceFile, false);
    long startTimeForUnseqFile = getStartTimeForTsFile(unsequenceFile, true);
    long endTimeForUnseqFile = getEndTimeForTsFile(unsequenceFile, true);
    // The question of whether the files overlap can be equivalent to the question of whether the
    // two line segments intersect.
    // If one end of a line segment is within another line segment, the two line segments intersect.
    // Here we fix the line segment corresponding to the unsequence file, and determine whether any
    // end
    // of sequence file is within unsequence file.
    return (startTimeForSequenceFile >= startTimeForUnseqFile
            && startTimeForSequenceFile <= endTimeForUnseqFile)
        || (endTimeForSequenceFile >= startTimeForUnseqFile
            && endTimeForSequenceFile <= endTimeForUnseqFile);
  }

  /**
   * This function will get the device set from tsfile, and get the smallest start time as the start
   * time for current tsfile.
   *
   * @param tsFileResource TsFile to get the start time
   * @param unsequence if the tsfile is an unsequence tsfile, the start time will be cached for
   *     unsequence tsfile.
   * @return the start time of the file
   */
  private long getStartTimeForTsFile(TsFileResource tsFileResource, boolean unsequence) {
    if (unsequence && startTimeCachedForUnseqFile.containsKey(tsFileResource)) {
      return startTimeCachedForUnseqFile.get(tsFileResource);
    }
    Set<String> deviceSet = tsFileResource.getDevices();
    long startTime = Long.MAX_VALUE;
    for (String device : deviceSet) {
      startTime = Math.min(tsFileResource.getStartTime(device), startTime);
    }
    if (unsequence) {
      startTimeCachedForUnseqFile.put(tsFileResource, startTime);
    }
    return startTime;
  }

  /**
   * This function will get the device set from tsfile, and get the greatest end time as the end
   * time for current tsfile.
   *
   * @param tsFileResource TsFile to get the end time
   * @param unsequence if the tsfile is an unsequence tsfile, the end time will be cached for
   *     unsequence tsfile.
   * @return the end time of the file
   */
  private long getEndTimeForTsFile(TsFileResource tsFileResource, boolean unsequence) {
    if (unsequence && endTimeCacheForUnseqFile.containsKey(tsFileResource)) {
      return endTimeCacheForUnseqFile.get(tsFileResource);
    }
    Set<String> deviceSet = tsFileResource.getDevices();
    long endTime = Long.MIN_VALUE;
    for (String device : deviceSet) {
      endTime = Math.max(tsFileResource.getStartTime(device), endTime);
    }
    if (unsequence) {
      endTimeCacheForUnseqFile.put(tsFileResource, endTime);
    }
    return endTime;
  }
}
