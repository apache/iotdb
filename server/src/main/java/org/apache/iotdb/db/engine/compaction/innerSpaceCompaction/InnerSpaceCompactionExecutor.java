package org.apache.iotdb.db.engine.compaction.innerSpaceCompaction;

import java.util.List;
import org.apache.iotdb.db.engine.compaction.TsFileManagement;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

public abstract class InnerSpaceCompactionExecutor {

  protected TsFileManagement tsFileManagement;

  public InnerSpaceCompactionExecutor(TsFileManagement tsFileManagement) {
    this.tsFileManagement = tsFileManagement;
  }

  public abstract void recover();

  public abstract void doInnerSpaceCompaction(
      List<List<TsFileResource>> mergeResources, boolean sequence, long timePartition);
}
