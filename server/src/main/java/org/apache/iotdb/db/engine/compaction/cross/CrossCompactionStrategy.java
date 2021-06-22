package org.apache.iotdb.db.engine.compaction.cross;

import org.apache.iotdb.db.engine.compaction.cross.inplace.InplaceCompactionRecoverTask;
import org.apache.iotdb.db.engine.compaction.cross.inplace.InplaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.cross.inplace.InplaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;

import java.io.File;
import java.util.List;

public enum CrossCompactionStrategy {
  INPLACE_COMPACTION;

  public AbstractCrossSpaceCompactionTask getCompactionTask(
      String storageGroupName,
      long timePartitionId,
      CrossSpaceMergeResource mergeResource,
      String storageGroupDir,
      TsFileResourceList seqTsFileResourceList,
      TsFileResourceList unSeqTsFileResourceList,
      List<TsFileResource> selectedSeqTsFileResourceList,
      List<TsFileResource> selectedUnSeqTsFileResourceList,
      int concurrentMergeCount) {
    switch (this) {
      case INPLACE_COMPACTION:
      default:
        return new InplaceCompactionTask(
            storageGroupName,
            timePartitionId,
            mergeResource,
            storageGroupDir,
            seqTsFileResourceList,
            unSeqTsFileResourceList,
            selectedSeqTsFileResourceList,
            selectedUnSeqTsFileResourceList,
            concurrentMergeCount);
    }
  }

  public AbstractCrossSpaceCompactionTask getCompactionRecoverTask(
      String storageGroupName,
      long timePartitionId,
      String storageGroupDir,
      TsFileResourceList seqTsFileResourceList,
      TsFileResourceList unSeqTsFileResourceList,
      int concurrentMergeCount,
      File logFile) {
    switch (this) {
      case INPLACE_COMPACTION:
      default:
        return new InplaceCompactionRecoverTask(
            storageGroupName,
            timePartitionId,
            storageGroupDir,
            seqTsFileResourceList,
            unSeqTsFileResourceList,
            concurrentMergeCount,
            logFile);
    }
  }

  public AbstractCrossSpaceCompactionSelector getCompactionSelector(
      String storageGroupName,
      String virtualGroupId,
      String storageGroupDir,
      long timePartition,
      TsFileResourceList sequenceFileList,
      TsFileResourceList unsequenceFileList,
      CrossSpaceCompactionTaskFactory taskFactory) {
    switch (this) {
      case INPLACE_COMPACTION:
      default:
        return new InplaceCompactionSelector(
            storageGroupName,
            virtualGroupId,
            storageGroupDir,
            timePartition,
            sequenceFileList,
            unsequenceFileList,
            taskFactory);
    }
  }
}
