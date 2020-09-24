package org.apache.iotdb.db.engine.merge.strategy.overlapped.squeeze.task;

import org.apache.iotdb.db.engine.merge.MergeCallback;
import org.apache.iotdb.db.engine.merge.MergeTask;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;

public class SqueezeAppendMergeTask extends MergeTask {

  public static final String MERGE_SUFFIX = ".merge.squeeze";

  public SqueezeAppendMergeTask(
      MergeResource mergeResource, String storageGroupSysDir, MergeCallback callback,
      String taskName, String storageGroupName) {
    super(mergeResource, storageGroupSysDir, callback, taskName, false, storageGroupName);
  }

  @Override
  public Void call() {
    return null;
  }

  @Override
  protected void cleanUp(boolean executeCallback) {

  }
}
