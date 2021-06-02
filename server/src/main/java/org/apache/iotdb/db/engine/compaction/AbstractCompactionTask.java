package org.apache.iotdb.db.engine.compaction;

import java.util.concurrent.Callable;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.crossSpaceCompaction.CrossSpaceCompactionExecutor;
import org.apache.iotdb.db.engine.compaction.crossSpaceCompaction.inplace.InplaceCompactionExecutor;
import org.apache.iotdb.db.engine.compaction.innerSpaceCompaction.InnerSpaceCompactionExecutor;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.CloseCompactionMergeCallBack;

public abstract class AbstractCompactionTask implements Callable<Void> {
  protected TsFileManagement tsFileManagement;
  protected CloseCompactionMergeCallBack closeCompactionMergeCallBack;
  protected CrossSpaceCompactionExecutor crossSpaceCompactionExecutor;
  protected InnerSpaceCompactionExecutor innerSpaceCompactionExecutor;

  public AbstractCompactionTask(
      TsFileManagement tsFileManagement,
      CloseCompactionMergeCallBack closeCompactionMergeCallBack) {
    this.tsFileManagement = tsFileManagement;
    this.closeCompactionMergeCallBack = closeCompactionMergeCallBack;
    this.crossSpaceCompactionExecutor = new InplaceCompactionExecutor(tsFileManagement);
    this.innerSpaceCompactionExecutor =
        IoTDBDescriptor.getInstance()
            .getConfig()
            .getCompactionStrategy()
            .getInnerSpaceCompactionExecutor(tsFileManagement);
  }
}
