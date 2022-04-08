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
import org.apache.iotdb.db.engine.compaction.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.cross.CrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.cross.ICrossSpaceSelector;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.selector.ICrossSpaceMergeFileSelector;
import org.apache.iotdb.db.engine.compaction.performer.impl.ReadPointCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class RewriteCrossSpaceCompactionSelector implements ICrossSpaceSelector {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  protected String logicalStorageGroupName;
  protected String virtualGroupId;
  protected String storageGroupDir;
  protected long timePartition;
  protected TsFileManager tsFileManager;
  protected List<TsFileResource> sequenceFileList;
  protected List<TsFileResource> unsequenceFileList;

  public RewriteCrossSpaceCompactionSelector(
      String logicalStorageGroupName,
      String virtualStorageGroupId,
      String storageGroupDir,
      long timePartition,
      TsFileManager tsFileManager) {
    this.storageGroupDir = storageGroupDir;
    this.logicalStorageGroupName = logicalStorageGroupName;
    this.virtualGroupId = virtualStorageGroupId;
    this.timePartition = timePartition;
    this.tsFileManager = tsFileManager;
    this.sequenceFileList = tsFileManager.getSequenceListByTimePartition(timePartition);
    this.unsequenceFileList = tsFileManager.getUnsequenceListByTimePartition(timePartition);
  }

  /**
   * This method creates a specific file selector according to the file selection strategy of
   * crossSpace compaction, uses the file selector to select all unseqFiles and seqFiles to be
   * compacted under the time partition of the virtual storage group, and creates a compaction task
   * for them. The task is put into the compactionTaskQueue of the {@link CompactionTaskManager}.
   *
   * @return Returns whether the file was found and submits the merge task
   */
  @Override
  public List<AbstractCompactionTask> select() {
    if ((CompactionTaskManager.currentTaskNum.get() >= config.getConcurrentCompactionThread())
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
    long budget = config.getCrossCompactionMemoryBudget();
    long timeLowerBound = System.currentTimeMillis() - Long.MAX_VALUE;
    RewriteCrossSpaceCompactionResource mergeResource =
        new RewriteCrossSpaceCompactionResource(seqFileList, unSeqFileList, timeLowerBound);

    ICrossSpaceMergeFileSelector fileSelector =
        CompactionUtils.getCrossSpaceFileSelector(budget, mergeResource);
    try {
      List[] mergeFiles = fileSelector.select();
      // avoid pending tasks holds the metadata and streams
      mergeResource.clear();
      if (mergeFiles.length == 0) {
        if (mergeResource.getUnseqFiles().size() > 0) {
          // still have unseq files but cannot be selected
          LOGGER.warn(
              "{} cannot select merge candidates under the budget {}",
              logicalStorageGroupName,
              budget);
        }
        return Collections.emptyList();
      }
      LOGGER.info(
          "select files for cross compaction, sequence files: {}, unsequence files {}",
          mergeFiles[0],
          mergeFiles[1]);

      if (mergeFiles[0].size() > 0 && mergeFiles[1].size() > 0) {
        LOGGER.info(
            "{} [Compaction] submit a task with {} sequence file and {} unseq files",
            logicalStorageGroupName + "-" + virtualGroupId,
            mergeResource.getSeqFiles().size(),
            mergeResource.getUnseqFiles().size());
        return Collections.singletonList(
            new CrossSpaceCompactionTask(
                logicalStorageGroupName,
                virtualGroupId,
                timePartition,
                tsFileManager,
                mergeFiles[0],
                mergeFiles[1],
                new ReadPointCompactionPerformer(),
                CompactionTaskManager.currentTaskNum));
      }

    } catch (MergeException | IOException e) {
      LOGGER.error("{} cannot select file for cross space compaction", logicalStorageGroupName, e);
    }
    return Collections.emptyList();
  }
}
