package org.apache.iotdb.db.engine.compaction.cross;

import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionSelector;
import org.apache.iotdb.db.engine.compaction.task.ICompactionTaskFactory;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;

public abstract class AbstractCrossSpaceCompactionSelector extends AbstractCompactionSelector {
  protected String storageGroupName;
  protected String virtualGroupId;
  protected String storageGroupDir;
  protected long timePartition;
  protected TsFileResourceList sequenceFileList;
  protected TsFileResourceList unsequenceFileList;
  protected ICompactionTaskFactory taskFactory;

  public AbstractCrossSpaceCompactionSelector(
      String storageGroupName,
      String virtualGroupId,
      String storageGroupDir,
      long timePartition,
      TsFileResourceList sequenceFileList,
      TsFileResourceList unsequenceFileList,
      ICompactionTaskFactory taskFactory) {
    this.storageGroupName = storageGroupName;
    this.virtualGroupId = virtualGroupId;
    this.storageGroupDir = storageGroupDir;
    this.timePartition = timePartition;
    this.sequenceFileList = sequenceFileList;
    this.unsequenceFileList = unsequenceFileList;
    this.taskFactory = taskFactory;
  }

  public abstract boolean selectAndSubmit();
}
