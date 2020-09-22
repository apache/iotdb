package org.apache.iotdb.db.engine.merge.sizeMerge;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.iotdb.db.engine.merge.IMergeFileSelector;
import org.apache.iotdb.db.engine.merge.IRecoverMergeTask;
import org.apache.iotdb.db.engine.merge.MergeCallback;
import org.apache.iotdb.db.engine.merge.MergeFileStrategyFactory;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.sizeMerge.regularization.selector.RegularizationMaxFileSelector;
import org.apache.iotdb.db.engine.merge.sizeMerge.regularization.task.RecoverRegularizationMergeTask;
import org.apache.iotdb.db.engine.merge.sizeMerge.regularization.task.RegularizationMergeTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

public class MergeSmallFileStrategyFactory extends MergeFileStrategyFactory {
  private MergeSmallFilesStrategy mergeSmallFilesStrategy;
  private MergeSizeSelectorStrategy mergeSizeSelectorStrategy;

  public MergeSmallFileStrategyFactory(
      MergeSmallFilesStrategy mergeSmallFilesStrategy, MergeSizeSelectorStrategy mergeSizeSelectorStrategy) {
    this.mergeSmallFilesStrategy = mergeSmallFilesStrategy;
    this.mergeSizeSelectorStrategy = mergeSizeSelectorStrategy;
  }

  @Override
  public IMergeFileSelector getFileSelector(Collection<TsFileResource> seqFiles,
      Collection<TsFileResource> unseqFiles, long dataTTL, String storageGroupName,
      String storageGroupSysDir) {
    switch (mergeSmallFilesStrategy) {
      case REGULARIZATION:
      default:
        return new RegularizationMaxFileSelector(seqFiles, dataTTL, timeLowerBound);
    }
  }
  @Override
  public Callable<Void> getMergeTask(MergeResource mergeResource, String storageGroupSysDir,
      MergeCallback callback, String taskName, String storageGroupName) {
    switch (mergeSmallFilesStrategy) {
      case REGULARIZATION:
      default:
        return new RegularizationMergeTask(mergeResource, storageGroupSysDir, callback, taskName,
            storageGroupName);
    }
  }

  @Override
  public IRecoverMergeTask getRecoverMergeTask(List<TsFileResource> seqTsFiles,
      List<TsFileResource> unseqTsFiles, String storageGroupSysDir, MergeCallback callback,
      String taskName, String storageGroupName) {
    switch (mergeSmallFilesStrategy) {
      case REGULARIZATION:
      default:
        return new RecoverRegularizationMergeTask(seqTsFiles,
            unseqTsFiles, storageGroupSysDir, callback, taskName, storageGroupName);
    }
  }

  public MergeSmallFilesStrategy getMergeSmallFilesStrategy() {
    return mergeSmallFilesStrategy;
  }

  public MergeSizeSelectorStrategy getMergeSizeSelectorStrategy() {
    return mergeSizeSelectorStrategy;
  }

  @Override
  public String toString(){
    StringBuilder result = new StringBuilder();
    result.append(mergeSmallFilesStrategy.toString());
    result.append("-");
    result.append(mergeSizeSelectorStrategy.toString());
    return new String(result);
  }

  public static MergeSmallFileStrategyFactory valueOf(String input){
    String[] result = input.split("-");
    MergeSmallFilesStrategy mergeSmallFilesStrategy = MergeSmallFilesStrategy.valueOf(result[0]);
    MergeSizeSelectorStrategy mergeSizeSelectorStrategy = MergeSizeSelectorStrategy.valueOf(result[1]);
    return new MergeSmallFileStrategyFactory(mergeSmallFilesStrategy,mergeSizeSelectorStrategy);
  }
}
