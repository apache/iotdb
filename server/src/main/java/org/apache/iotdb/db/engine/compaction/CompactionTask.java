package org.apache.iotdb.db.engine.compaction;

import java.util.List;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.CloseCompactionMergeCallBack;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.utils.Pair;

public class CompactionTask extends AbstractCompactionTask {
  private final boolean enableUnseqCompaction =
      IoTDBDescriptor.getInstance().getConfig().isEnableUnseqCompaction();

  private long timePartition;
  private boolean isFullMerge;

  public CompactionTask(
      TsFileManagement tsFileManagement,
      long timePartition,
      boolean isFullMerge,
      CloseCompactionMergeCallBack closeCompactionMergeCallBack) {
    super(tsFileManagement, closeCompactionMergeCallBack);
    this.timePartition = timePartition;
    this.isFullMerge = isFullMerge;
  }

  @Override
  public Void call() {
    merge(timePartition, isFullMerge);
    closeCompactionMergeCallBack.call(tsFileManagement.storageGroupName);
    return null;
  }

  private void merge(long timePartition, boolean isFullMerge) {
    List<List<TsFileResource>> sequenceTsFileResources = tsFileListPair.left;
    List<List<TsFileResource>> unSequenceTsFileResources = tsFileListPair.right;
    if (enableUnseqCompaction) {
      crossSpaceCompactionExecutor.doCrossSpaceCompaction(isFullMerge, timePartition);
    }
    innerSpaceCompactionExecutor.doInnerSpaceCompaction(true, timePartition);
    innerSpaceCompactionExecutor.doInnerSpaceCompaction(false, timePartition);
  }
}
