package org.apache.iotdb.db.engine.compaction.cross.inplace;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionContext;
import org.apache.iotdb.db.engine.compaction.CompactionScheduler;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.cross.AbstractCrossSpaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.compaction.cross.inplace.selector.ICrossSpaceMergeFileSelector;
import org.apache.iotdb.db.engine.compaction.inner.sizetired.SizeTiredCompactionSelector;
import org.apache.iotdb.db.engine.compaction.inner.utils.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.compaction.task.ICompactionTaskFactory;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.exception.MergeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class InplaceCompactionSelector extends AbstractCrossSpaceCompactionSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(SizeTiredCompactionSelector.class);
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  public InplaceCompactionSelector(
      String storageGroupName,
      String storageGroupDir,
      long timePartition,
      TsFileResourceList sequenceFileList,
      TsFileResourceList unsequenceFileList,
      ICompactionTaskFactory taskFactory) {
    super(
        storageGroupName,
        storageGroupDir,
        timePartition,
        sequenceFileList,
        unsequenceFileList,
        taskFactory);
  }

  @Override
  public boolean selectAndSubmit() {
    boolean taskSubmitted = false;
    if ((CompactionScheduler.currentTaskNum.get() >= config.getConcurrentCompactionThread())
        || (!config.isEnableCrossSpaceCompaction())
        || CompactionScheduler.isPartitionCompacting(storageGroupName, timePartition)) {
      return false;
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
      return false;
    }
    if (unSeqFileList.size() > config.getMaxOpenFileNumInCrossSpaceCompaction()) {
      unSeqFileList = unSeqFileList.subList(0, config.getMaxOpenFileNumInCrossSpaceCompaction());
    }
    long budget = config.getMergeMemoryBudget();
    long timeLowerBound = System.currentTimeMillis() - Long.MAX_VALUE;
    CrossSpaceMergeResource mergeResource =
        new CrossSpaceMergeResource(seqFileList, unSeqFileList, timeLowerBound);

    ICrossSpaceMergeFileSelector fileSelector =
        CompactionUtils.getCrossSpaceFileSelector(budget, mergeResource);
    try {
      List[] mergeFiles = fileSelector.select();
      if (mergeFiles.length == 0) {
        LOGGER.info(
            "{} cannot select merge candidates under the budget {}", storageGroupName, budget);
        return false;
      }
      // avoid pending tasks holds the metadata and streams
      mergeResource.clear();
      // do not cache metadata until true candidates are chosen, or too much metadata will be
      // cached during selection
      mergeResource.setCacheDeviceMeta(true);

      for (TsFileResource tsFileResource : mergeResource.getSeqFiles()) {
        tsFileResource.setMerging(true);
      }
      for (TsFileResource tsFileResource : mergeResource.getUnseqFiles()) {
        tsFileResource.setMerging(true);
      }

      CompactionContext context = new CompactionContext();
      context.setStorageGroupName(storageGroupName);
      context.setStorageGroupDir(storageGroupDir);
      context.setTimePartitionId(timePartition);

      context.setMergeResource(mergeResource);
      context.setSequenceFileResourceList(sequenceFileList);
      context.setSelectedSequenceFiles(mergeResource.getSeqFiles());
      context.setUnsequenceFileResourceList(unsequenceFileList);
      context.setSelectedUnsequenceFiles(mergeResource.getUnseqFiles());
      context.setConcurrentMergeCount(fileSelector.getConcurrentMergeNum());

      AbstractCompactionTask compactionTask = taskFactory.createTask(context);
      CompactionTaskManager.getInstance()
          .submitTask(storageGroupName, timePartition, compactionTask);
      taskSubmitted = true;
    } catch (MergeException | IOException e) {
      LOGGER.error("{} cannot select file for cross space compaction", storageGroupName, e);
    }

    return taskSubmitted;
  }
}
