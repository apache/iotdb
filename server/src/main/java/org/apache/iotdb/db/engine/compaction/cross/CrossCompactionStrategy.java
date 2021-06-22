package org.apache.iotdb.db.engine.compaction.cross;

import org.apache.iotdb.db.engine.compaction.CompactionContext;
import org.apache.iotdb.db.engine.compaction.cross.inplace.InplaceCompactionRecoverTask;
import org.apache.iotdb.db.engine.compaction.cross.inplace.InplaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.cross.inplace.InplaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.task.ICompactionTaskFactory;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;

public enum CrossCompactionStrategy {
  INPLACE_COMPACTION;

  public AbstractCrossSpaceCompactionTask getCompactionTask(CompactionContext context) {
    switch (this) {
      case INPLACE_COMPACTION:
      default:
        return new InplaceCompactionTask(context);
    }
  }

  public AbstractCrossSpaceCompactionTask getCompactionRecoverTask(CompactionContext context) {
    switch (this) {
      case INPLACE_COMPACTION:
      default:
        return new InplaceCompactionRecoverTask(context);
    }
  }

  public AbstractCrossSpaceCompactionSelector getCompactionSelector(
      String storageGroupName,
      String storageGroupDir,
      long timePartition,
      TsFileResourceList sequenceFileList,
      TsFileResourceList unsequenceFileList,
      ICompactionTaskFactory taskFactory) {
    switch (this) {
      case INPLACE_COMPACTION:
      default:
        return new InplaceCompactionSelector(
            storageGroupName,
            storageGroupDir,
            timePartition,
            sequenceFileList,
            unsequenceFileList,
            taskFactory);
    }
  }
}
