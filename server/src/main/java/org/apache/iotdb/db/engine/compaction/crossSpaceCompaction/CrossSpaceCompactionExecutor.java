package org.apache.iotdb.db.engine.compaction.crossSpaceCompaction;

import java.util.List;
import org.apache.iotdb.db.engine.compaction.TsFileManagement;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

public abstract class CrossSpaceCompactionExecutor {

  protected TsFileManagement tsFileManagement;

  public CrossSpaceCompactionExecutor(TsFileManagement tsFileManagement) {
    this.tsFileManagement = tsFileManagement;
  }

  public abstract void recover();

  public abstract void doCrossSpaceCompaction(
      boolean fullMerge, List<TsFileResource> seqMergeList, List<TsFileResource> unSeqMergeList);
}
