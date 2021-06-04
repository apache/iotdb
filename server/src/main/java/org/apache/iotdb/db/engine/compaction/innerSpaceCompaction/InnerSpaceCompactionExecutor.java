package org.apache.iotdb.db.engine.compaction.innerSpaceCompaction;

import org.apache.iotdb.db.engine.compaction.TsFileManagement;

public abstract class InnerSpaceCompactionExecutor {

  protected TsFileManagement tsFileManagement;

  public InnerSpaceCompactionExecutor(TsFileManagement tsFileManagement) {
    this.tsFileManagement = tsFileManagement;
  }

  public abstract void recover();

  public abstract void doInnerSpaceCompaction(boolean sequence, long timePartition);
}
