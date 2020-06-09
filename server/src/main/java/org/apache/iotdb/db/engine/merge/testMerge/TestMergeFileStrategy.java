package org.apache.iotdb.db.engine.merge.testMerge;

import java.util.concurrent.Callable;
import org.apache.iotdb.db.engine.merge.MergeCallback;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.testMerge.level.task.LevelMergeTask;

public enum TestMergeFileStrategy {

  LEVEL;

  public Callable<Void> getMergeTask(MergeResource mergeResource, String storageGroupSysDir,
      MergeCallback callback, String taskName, String storageGroupName) {
    switch (this) {
      case LEVEL:
      default:
        return new LevelMergeTask(mergeResource, storageGroupSysDir, callback, taskName,
            storageGroupName);
    }
  }

}
