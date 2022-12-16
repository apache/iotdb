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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.inner.IInnerSeqSpaceSelector;
import org.apache.iotdb.db.engine.compaction.inner.IInnerUnseqSpaceSelector;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * SizeTieredCompactionSelector selects files to be compacted based on the size of files. The
 * selector traverses the file list from old to new. If the size of selected files or the number of
 * select files exceed given threshold, a compaction task will be submitted to task queue in
 * CompactionTaskManager. In CompactionTaskManager, tasks are ordered by {@link
 * org.apache.iotdb.db.engine.compaction.comparator.ICompactionTaskComparator}. To maximize
 * compaction efficiency, selector searches compaction task from 0 compaction files(that is, file
 * that never been compacted, named level 0 file) to higher level files. If a compaction task is
 * found in some level, selector will not search higher level anymore.
 */
public class SizeTieredCompactionSelector
    implements IInnerSeqSpaceSelector, IInnerUnseqSpaceSelector {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  protected String storageGroupName;
  protected String dataRegionId;
  protected long timePartition;
  protected boolean sequence;
  protected TsFileManager tsFileManager;
  protected boolean hasNextTimePartition;

  public SizeTieredCompactionSelector(
      String storageGroupName,
      String dataRegionId,
      long timePartition,
      boolean sequence,
      TsFileManager tsFileManager) {
    this.storageGroupName = storageGroupName;
    this.dataRegionId = dataRegionId;
    this.timePartition = timePartition;
    this.sequence = sequence;
    this.tsFileManager = tsFileManager;
    hasNextTimePartition = tsFileManager.hasNextTimePartition(timePartition, sequence);
  }

  /**
   * This method searches for all files on the given level. If there are consecutive files on the
   * level that meet the system preset conditions (the number exceeds 10 or the total file size
   * exceeds 2G), a compaction task is created for the batch of files and placed in the
   * taskPriorityQueue queue , and continue to search for the next batch. If at least one batch of
   * files to be compacted is found on this layer, it will return false (indicating that it will no
   * longer search for higher layers), otherwise it will return true.
   *
   * @param level the level to be searched
   *     each batch
   * @return return whether to continue the search to higher levels
   * @throws IOException
   */
  private List<InnerCompactionCandidate> selectLevelTask(List<TsFileResource> tsFileResources, int level)
      throws IOException {
    List<InnerCompactionCandidate> result = new ArrayList<>();
    //sort TsFileResources by inner level in ascending
//    tsFileResources.sort(new TsFileResourceInnerLevelComparator());
    InnerCompactionCandidate candidate = new InnerCompactionCandidate();
    for (TsFileResource currentFile : tsFileResources) {
      if (tsFileShouldBeSkipped(currentFile, level)) {
        continue;
      }
      // 为什么 ？
      if (currentFile.getStatus() != TsFileResourceStatus.CLOSED) {
        candidate = new InnerCompactionCandidate();
        continue;
      }
      LOGGER.debug("file added. File is {}, size is {}", currentFile, currentFile.getTsFileSize());
      candidate.addTsFileResource(currentFile);

      // if the file size or file num reach threshold
      if (candidateSatisfied(candidate)) {
        // submit the task
        if (candidate.getFileCount() > 1) {
          result.add(candidate);
        }
        candidate = new InnerCompactionCandidate();
      }
    }

    // if next time partition exists
    // submit a merge task even it does not meet the requirement for file num or file size
    if (hasNextTimePartition && candidate.getTotalFileSize() > 1) {
      result.add(candidate);
    }
    return result;
  }

  private boolean candidateSatisfied(InnerCompactionCandidate candidate) {
    return candidate.getFileCount() >= config.getTargetCompactionFileSize() || candidate.getTotalFileSize() >= config.getMaxInnerCompactionCandidateFileNum();
  }

  private boolean tsFileShouldBeSkipped(TsFileResource tsFileResource, int level) throws IOException {
    TsFileNameGenerator.TsFileName currentFileName =
        TsFileNameGenerator.getTsFileName(tsFileResource.getTsFile().getName());
    return currentFileName.getInnerCompactionCnt() != level;
  }

  /**
   * This method searches for a batch of files to be compacted from layer 0 to the highest layer. If
   * there are more than a batch of files to be merged on a certain layer, it does not search to
   * higher layers. It creates a compaction thread for each batch of files and put it into the
   * candidateCompactionTaskQueue of the {@link CompactionTaskManager}.
   *
   * @return Returns whether the file was found and submits the merge task
   */
  @Override
  public List<List<TsFileResource>> selectInnerSpaceTask(List<TsFileResource> tsFileResources) {
    PriorityQueue<InnerCompactionCandidate> taskPriorityQueue =
        new PriorityQueue<>(new SizeTieredCompactionTaskComparator());
    try {
      int maxLevel = searchMaxFileLevel(tsFileResources);
      for (int currentLevel = 0; currentLevel <= maxLevel; currentLevel++) {
        List<InnerCompactionCandidate> candidates = selectLevelTask(tsFileResources, currentLevel);
        if (candidates.size() > 0) {
          taskPriorityQueue.addAll(candidates);
          break;
        }
      }
      List<List<TsFileResource>> taskList = new LinkedList<>();
      while (taskPriorityQueue.size() > 0) {
        List<TsFileResource> resources = taskPriorityQueue.poll().getTsFileResources();
        taskList.add(resources);
      }
      return taskList;
    } catch (Exception e) {
      LOGGER.error("Exception occurs while selecting files", e);
    }
    return Collections.emptyList();
  }

  private int searchMaxFileLevel(List<TsFileResource> tsFileResources) throws IOException {
    int maxLevel = -1;
    for (TsFileResource currentFile : tsFileResources) {
      TsFileNameGenerator.TsFileName currentName =
          TsFileNameGenerator.getTsFileName(currentFile.getTsFile().getName());
      if (currentName.getInnerCompactionCnt() > maxLevel) {
        maxLevel = currentName.getInnerCompactionCnt();
      }
    }
    return maxLevel;
  }

  private class SizeTieredCompactionTaskComparator
      implements Comparator<InnerCompactionCandidate> {

    @Override
    public int compare(InnerCompactionCandidate o1, InnerCompactionCandidate o2) {
      TsFileResource resourceOfO1 = o1.getTsFileResources().get(0);
      TsFileResource resourceOfO2 = o2.getTsFileResources().get(0);
      try {
        TsFileNameGenerator.TsFileName fileNameOfO1 =
            TsFileNameGenerator.getTsFileName(resourceOfO1.getTsFile().getName());
        TsFileNameGenerator.TsFileName fileNameOfO2 =
            TsFileNameGenerator.getTsFileName(resourceOfO2.getTsFile().getName());
        if (fileNameOfO1.getInnerCompactionCnt() != fileNameOfO2.getInnerCompactionCnt()) {
          return fileNameOfO2.getInnerCompactionCnt() - fileNameOfO1.getInnerCompactionCnt();
        }
        return (int) (fileNameOfO2.getVersion() - fileNameOfO1.getVersion());
      } catch (IOException e) {
        return 0;
      }
    }
  }

  private class TsFileResourceInnerLevelComparator implements Comparator<TsFileResource> {
    @Override
    public int compare(TsFileResource o1, TsFileResource o2) {
      try {
        TsFileNameGenerator.TsFileName o1Name =
            TsFileNameGenerator.getTsFileName(o1.getTsFile().getName());
        TsFileNameGenerator.TsFileName o2Name =
            TsFileNameGenerator.getTsFileName(o2.getTsFile().getName());
        return o1Name.getInnerCompactionCnt() - o2Name.getInnerCompactionCnt();
      } catch (IOException e) {
        return 0;
      }
    }
  }
}
