package org.apache.iotdb.db.engine.merge.strategy.small;

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

public class SmallFileMergeStage extends MergeStage {

  // how to select small file
  private FileSelectorStrategy mergeSizeSelectorStrategy;
  // how to merge
  private FileMergeStrategy mergeSmallFilesStrategy;
  // how to recover
  private FileRecoverStrategy mergeRecoverStrategy;

  public SmallFileMergeStage(
      MergeSmallFilesStrategy mergeSmallFilesStrategy,
      SizeBasedFileSelectorStrategy mergeSizeSelectorStrategy,
      SizeBasedFileRecoverStrategy mergeRecoverStrategy) {
    this.mergeSmallFilesStrategy = mergeSmallFilesStrategy;
    this.mergeSizeSelectorStrategy = mergeSizeSelectorStrategy;
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
    return mergeSmallFilesStrategy
        .getMergeTask(mergeResource, storageGroupSysDir, callback, taskName, storageGroupName);
  }

  @Override
  public BaseMergeSchedulerTask getMergeSchedulerTask(MergeContext context, String taskName,
      MergeLogger mergeLogger,
      MergeResource mergeResource, List<PartialPath> unmergedSeries, String storageGroupName) {
    return mergeSmallFilesStrategy
        .getMergeSchedulerTask(context, taskName, mergeLogger, mergeResource, unmergedSeries,
            storageGroupName);
  }

  @Override
  public IRecoverMergeTask getRecoverMergeTask(Collection<TsFileResource> seqTsFiles,
      Collection<TsFileResource> unseqTsFiles, String storageGroupSysDir, MergeCallback callback,
      String taskName, String storageGroupName) {
    return mergeRecoverStrategy
        .getRecoverMergeTask(seqTsFiles, unseqTsFiles, storageGroupSysDir, callback, taskName,
            storageGroupName);
  }

  public FileSelectorStrategy getMergeSizeSelectorStrategy() {
    return mergeSizeSelectorStrategy;
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(mergeSmallFilesStrategy.toString());
    result.append("-");
    result.append(mergeSizeSelectorStrategy.toString());
    return new String(result);
  }

  public static SmallFileMergeStage valueOf(String input) {
    String[] result = input.split("-");
    MergeSmallFilesStrategy mergeSmallFilesStrategy = MergeSmallFilesStrategy.valueOf(result[0]);
    SizeBasedFileSelectorStrategy mergeSizeSelectorStrategy = SizeBasedFileSelectorStrategy
        .valueOf(result[1]);
    SizeBasedFileRecoverStrategy mergeRecoverStrategy = SizeBasedFileRecoverStrategy
        .valueOf(result[0]);
    return new SmallFileMergeStage(mergeSmallFilesStrategy, mergeSizeSelectorStrategy,
        mergeRecoverStrategy);
  }
}
