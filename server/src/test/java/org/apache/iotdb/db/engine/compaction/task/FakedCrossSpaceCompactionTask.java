package org.apache.iotdb.db.engine.compaction.task;

import org.apache.iotdb.db.engine.compaction.cross.inplace.InplaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.storagegroup.FakedTsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;

import java.util.List;

public class FakedCrossSpaceCompactionTask extends InplaceCompactionTask {
  public FakedCrossSpaceCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartitionId,
      CrossSpaceMergeResource mergeResource,
      String storageGroupDir,
      TsFileResourceList seqTsFileResourceList,
      TsFileResourceList unSeqTsFileResourceList,
      List<TsFileResource> selectedSeqTsFileResourceList,
      List<TsFileResource> selectedUnSeqTsFileResourceList,
      int concurrentMergeCount) {
    super(
        logicalStorageGroupName,
        virtualStorageGroupName,
        timePartitionId,
        mergeResource,
        storageGroupDir,
        seqTsFileResourceList,
        unSeqTsFileResourceList,
        selectedSeqTsFileResourceList,
        selectedUnSeqTsFileResourceList,
        concurrentMergeCount);
  }

  @Override
  protected void doCompaction() {
    long totalUnseqFileSize = 0;
    for (TsFileResource resource : selectedUnSeqTsFileResourceList) {
      totalUnseqFileSize += resource.getTsFileSize();
    }
    long avgSizeAddToSeqFile = totalUnseqFileSize / selectedSeqTsFileResourceList.size();
    for (TsFileResource resource : selectedSeqTsFileResourceList) {
      ((FakedTsFileResource) resource)
          .setTsFileSize(resource.getTsFileSize() + avgSizeAddToSeqFile);
    }
    selectedUnSeqTsFileResourceList.clear();
    unSeqTsFileResourceList.clear();
  }
}
