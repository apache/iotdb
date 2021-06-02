package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.CloseCompactionMergeCallBack;

public class CompactionRecoverTask extends AbstractCompactionTask {
  public CompactionRecoverTask(
      TsFileManagement tsFileManagement,
      CloseCompactionMergeCallBack closeCompactionMergeCallBack) {
    super(tsFileManagement, closeCompactionMergeCallBack);
  }

  @Override
  public Void call() {
    recover();
    closeCompactionMergeCallBack.call();
    return null;
  }

  private void recover() {
    crossSpaceCompactionExecutor.recover();
    innerSpaceCompactionExecutor.recover();
  }
}
