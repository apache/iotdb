package org.apache.iotdb.db.engine.compaction.inner;

import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionSelector;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceManager;

public abstract class AbstractInnerSpaceCompactionSelector extends AbstractCompactionSelector {
  protected String storageGroupName;
  protected String virtualStorageGroupName;
  protected long timePartition;
  protected TsFileResourceList tsFileResources;
  protected boolean sequence;
  protected InnerSpaceCompactionTaskFactory taskFactory;
  protected TsFileResourceManager tsFileResourceManager;

  public AbstractInnerSpaceCompactionSelector(
      String storageGroupName,
      String virtualStorageGroupName,
      long timePartition,
      TsFileResourceManager tsFileResourceManager,
      TsFileResourceList tsFileResources,
      boolean sequence,
      InnerSpaceCompactionTaskFactory taskFactory) {
    this.storageGroupName = storageGroupName;
    this.virtualStorageGroupName = virtualStorageGroupName;
    this.timePartition = timePartition;
    this.tsFileResources = tsFileResources;
    this.tsFileResourceManager = tsFileResourceManager;
    this.sequence = sequence;
    this.taskFactory = taskFactory;
  }

  public abstract boolean selectAndSubmit();
}
