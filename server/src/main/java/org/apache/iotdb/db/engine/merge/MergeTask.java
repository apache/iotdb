package org.apache.iotdb.db.engine.merge;

import java.util.concurrent.Callable;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;

public abstract class MergeTask implements Callable<Void> {
  protected MergeResource resource;
  protected String storageGroupSysDir;
  protected String storageGroupName;
  protected MergeLogger mergeLogger;
  protected MergeContext mergeContext = new MergeContext();

  protected MergeCallback callback;
  protected String taskName;
  protected boolean fullMerge;
  protected States states = States.START;

  public MergeTask(MergeResource mergeResource, String storageGroupSysDir,
      MergeCallback callback, String taskName, boolean fullMerge, String storageGroupName) {
    this.resource = mergeResource;
    this.storageGroupSysDir = storageGroupSysDir;
    this.callback = callback;
    this.taskName = taskName;
    this.fullMerge = fullMerge;
    this.storageGroupName = storageGroupName;
  }

  public String getStorageGroupName() {
    return storageGroupName;
  }

  protected enum States {
    START,
    MERGE_CHUNKS,
    MERGE_FILES,
    CLEAN_UP,
    ABORTED
  }

  public String getProgress() {
    switch (states) {
      case ABORTED:
        return "Aborted";
      case CLEAN_UP:
        return "Cleaning up";
      case MERGE_FILES:
        return "Merging files";
      case MERGE_CHUNKS:
        return "Merging series";
      case START:
      default:
        return "Just started";
    }
  }

  public String getTaskName() {
    return taskName;
  }
}
