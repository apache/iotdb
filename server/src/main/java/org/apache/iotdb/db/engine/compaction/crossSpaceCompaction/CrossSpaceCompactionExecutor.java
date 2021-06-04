package org.apache.iotdb.db.engine.compaction.crossSpaceCompaction;

import org.apache.iotdb.db.engine.compaction.TsFileManagement;

public abstract class CrossSpaceCompactionExecutor {

  protected TsFileManagement tsFileManagement;

  public CrossSpaceCompactionExecutor(TsFileManagement tsFileManagement) {
    this.tsFileManagement = tsFileManagement;
  }

  public abstract void recover();

  public abstract void doCrossSpaceCompaction(boolean fullMerge, long timePartition);
}
