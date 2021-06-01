package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.crossSpaceCompaction.CrossSpaceCompactionExecutor;
import org.apache.iotdb.db.engine.compaction.innerSpaceCompaction.InnerSpaceCompactionExecutor;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.CloseCompactionMergeCallBack;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import java.util.List;
import java.util.concurrent.Callable;

public class CompactionScheduler {
  private final int unseqLevelNum =
      Math.max(IoTDBDescriptor.getInstance().getConfig().getUnseqLevelNum(), 1);
  private final boolean enableUnseqCompaction =
      IoTDBDescriptor.getInstance().getConfig().isEnableUnseqCompaction();

  private TsFileManagement tsFileManagement;
  private CrossSpaceCompactionExecutor crossSpaceCompactionExecutor;
  private InnerSpaceCompactionExecutor innerSpaceCompactionExecutor;

  public CompactionScheduler(
      TsFileManagement tsFileManagement,
      CrossSpaceCompactionExecutor crossSpaceCompactionExecutor,
      InnerSpaceCompactionExecutor innerSpaceCompactionExecutor) {
    this.tsFileManagement = tsFileManagement;
    this.crossSpaceCompactionExecutor = crossSpaceCompactionExecutor;
    this.innerSpaceCompactionExecutor = innerSpaceCompactionExecutor;
  }

  public void recover() {
    crossSpaceCompactionExecutor.recover();
    innerSpaceCompactionExecutor.recover();
  }

  public void merge(
      long timePartition,
      List<List<TsFileResource>> sequenceTsFileResources,
      List<List<TsFileResource>> unSequenceTsFileResources,
      boolean isFullMerge) {
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

  public class CompactionMergeTask implements Callable<Void> {

    private long timePartition;
    private List<List<TsFileResource>> sequenceTsFileResources;
    private List<List<TsFileResource>> unSequenceTsFileResources;
    private boolean isFullMerge;
    private CloseCompactionMergeCallBack closeCompactionMergeCallBack;

    public CompactionMergeTask(
        long timePartition,
        List<List<TsFileResource>> sequenceTsFileResources,
        List<List<TsFileResource>> unSequenceTsFileResources,
        boolean isFullMerge,
        CloseCompactionMergeCallBack closeCompactionMergeCallBack) {
      this.timePartition = timePartition;
      this.sequenceTsFileResources = sequenceTsFileResources;
      this.unSequenceTsFileResources = unSequenceTsFileResources;
      this.isFullMerge = isFullMerge;
      this.closeCompactionMergeCallBack = closeCompactionMergeCallBack;
    }

    @Override
    public Void call() {
      merge(timePartition, sequenceTsFileResources, unSequenceTsFileResources, isFullMerge);
      closeCompactionMergeCallBack.call();
      return null;
    }
  }

  public class CompactionRecoverTask implements Callable<Void> {

    private CloseCompactionMergeCallBack closeCompactionMergeCallBack;

    public CompactionRecoverTask(CloseCompactionMergeCallBack closeCompactionMergeCallBack) {
      this.closeCompactionMergeCallBack = closeCompactionMergeCallBack;
    }

    @Override
    public Void call() {
      recover();
      closeCompactionMergeCallBack.call();
      return null;
    }
  }
}
