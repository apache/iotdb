package org.apache.iotdb.db.engine.compaction.inner;

import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionSelector;
import org.apache.iotdb.db.engine.compaction.task.ICompactionTaskFactory;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;

public abstract class AbstractInnerSpaceCompactionSelector extends AbstractCompactionSelector {
  protected String storageGroupName;
  protected String virtualStorageGroupName;
  protected long timePartition;
  protected TsFileResourceList tsFileResources;
  protected boolean sequence;
  protected ICompactionTaskFactory taskFactory;

  public AbstractInnerSpaceCompactionSelector(
      String storageGroupName,
      String virtualStorageGroupName,
      long timePartition,
      TsFileResourceList tsFileResources,
      boolean sequence,
      ICompactionTaskFactory taskFactory) {
    this.storageGroupName = storageGroupName;
    this.virtualStorageGroupName = virtualStorageGroupName;
    this.timePartition = timePartition;
    this.tsFileResources = tsFileResources;
    this.sequence = sequence;
    this.taskFactory = taskFactory;
  }

  public abstract boolean selectAndSubmit();
}
