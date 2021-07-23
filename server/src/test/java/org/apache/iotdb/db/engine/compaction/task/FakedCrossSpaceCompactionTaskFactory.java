package org.apache.iotdb.db.engine.compaction.task;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;

import java.util.List;

public class FakedCrossSpaceCompactionTaskFactory {
  public AbstractCompactionTask createTask(
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
    return IoTDBDescriptor.getInstance()
        .getConfig()
        .getCrossCompactionStrategy()
        .getCompactionTask(
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
}
