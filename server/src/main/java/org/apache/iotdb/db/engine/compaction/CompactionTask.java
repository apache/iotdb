package org.apache.iotdb.db.engine.compaction;

import java.util.List;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.CloseCompactionMergeCallBack;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.utils.Pair;

public class CompactionTask extends AbstractCompactionTask {
  private final int unseqLevelNum =
      Math.max(IoTDBDescriptor.getInstance().getConfig().getUnseqLevelNum(), 1);
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
    Pair<List<List<TsFileResource>>, List<List<TsFileResource>>> tsFileListPair =
        tsFileManagement.forkCurrentFileList(timePartition);
    List<List<TsFileResource>> sequenceTsFileResources = tsFileListPair.left;
    List<List<TsFileResource>> unSequenceTsFileResources = tsFileListPair.right;
    // do unseq compaction if has an unseq file in max unseq level
    if (enableUnseqCompaction && unSequenceTsFileResources.get(unseqLevelNum - 1).size() > 0) {
      crossSpaceCompactionExecutor.doCrossSpaceCompaction(
          isFullMerge,
          tsFileManagement.getTsFileListByTimePartition(true, timePartition),
          unSequenceTsFileResources.get(unseqLevelNum - 1));
    }

    innerSpaceCompactionExecutor.doInnerSpaceCompaction(
        sequenceTsFileResources, true, timePartition);

    innerSpaceCompactionExecutor.doInnerSpaceCompaction(
        unSequenceTsFileResources, false, timePartition);
  }
}
