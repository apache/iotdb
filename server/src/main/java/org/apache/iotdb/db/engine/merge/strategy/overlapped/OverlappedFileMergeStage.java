package org.apache.iotdb.db.engine.merge.strategy.overlapped;

import java.io.File;
import java.util.Collection;
import java.util.List;
import org.apache.iotdb.db.engine.merge.BaseMergeSchedulerTask;
import org.apache.iotdb.db.engine.merge.FileMergeStrategy;
import org.apache.iotdb.db.engine.merge.FileRecoverStrategy;
import org.apache.iotdb.db.engine.merge.FileSelectorStrategy;
import org.apache.iotdb.db.engine.merge.IMergeFileSelector;
import org.apache.iotdb.db.engine.merge.IRecoverMergeTask;
import org.apache.iotdb.db.engine.merge.MergeCallback;
import org.apache.iotdb.db.engine.merge.MergeLogger;
import org.apache.iotdb.db.engine.merge.MergeStage;
import org.apache.iotdb.db.engine.merge.MergeTask;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.PartialPath;

public class OverlappedFileMergeStage extends MergeStage {

  private FileSelectorStrategy mergeSizeSelectorStrategy;
  private FileMergeStrategy mergeOverlappedFilesStrategy;
  private FileRecoverStrategy mergeRecoverStrategy;

  public OverlappedFileMergeStage(
      FileSelectorStrategy mergeSizeSelectorStrategy,
      FileMergeStrategy mergeOverlappedFilesStrategy,
      FileRecoverStrategy mergeRecoverStrategy) {
    this.mergeSizeSelectorStrategy = mergeSizeSelectorStrategy;
    this.mergeOverlappedFilesStrategy = mergeOverlappedFilesStrategy;
    this.mergeRecoverStrategy = mergeRecoverStrategy;
  }

  @Override
  public IMergeFileSelector getFileSelector(Collection<TsFileResource> seqFiles,
      Collection<TsFileResource> unseqFiles, long dataTTL, String storageGroupName,
      File storageGroupSysDir) {
    return mergeSizeSelectorStrategy
        .getFileSelector(seqFiles, unseqFiles, dataTTL, storageGroupName, storageGroupSysDir);
  }

  @Override
  public MergeTask getMergeTask(MergeResource mergeResource, String storageGroupSysDir,
      MergeCallback callback, String taskName, String storageGroupName) {
    return mergeOverlappedFilesStrategy
        .getMergeTask(mergeResource, storageGroupSysDir, callback, taskName, storageGroupName);
  }

  @Override
  public BaseMergeSchedulerTask getMergeSchedulerTask(MergeContext context, String taskName,
      MergeLogger mergeLogger, MergeResource mergeResource, List<PartialPath> unmergedSeries,
      String storageGroupName) {
    return null;
  }

  @Override
  public IRecoverMergeTask getRecoverMergeTask(Collection<TsFileResource> seqTsFiles,
      Collection<TsFileResource> unseqTsFiles, String storageGroupSysDir, MergeCallback callback,
      String taskName, String storageGroupName) {
    return mergeRecoverStrategy
        .getRecoverMergeTask(seqTsFiles, unseqTsFiles, storageGroupSysDir, callback, taskName,
            storageGroupName);
  }

  @Override
  public String toString() {
    return mergeOverlappedFilesStrategy.toString().replaceFirst("_", "-");
  }

  public static OverlappedFileMergeStage valueOf(String input) {
    String[] result = input.split("-");
    String mergeTypeStr = result[0] + "_" + result[1];
    OverlappedBasedFileSelectorStrategy overlappedBasedFileSelectorStrategy = OverlappedBasedFileSelectorStrategy
        .valueOf(result[0]);
    MergeOverlappedFilesStrategy mergeOverlappedFilesStrategy = MergeOverlappedFilesStrategy
        .valueOf(mergeTypeStr);
    OverlappedBasedFileRecoverStrategy overlappedBasedFileRecoverStrategy = OverlappedBasedFileRecoverStrategy
        .valueOf(mergeTypeStr);
    return new OverlappedFileMergeStage(overlappedBasedFileSelectorStrategy,
        mergeOverlappedFilesStrategy, overlappedBasedFileRecoverStrategy);
  }
}
