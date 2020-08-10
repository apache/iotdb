package org.apache.iotdb.db.engine.merge;

import java.util.concurrent.Callable;

public abstract class MergeChunkHeapTask implements Callable<Void> {

  public abstract String getStorageGroupName();

  public abstract String getTaskName();

  public abstract String getProgress();
}
