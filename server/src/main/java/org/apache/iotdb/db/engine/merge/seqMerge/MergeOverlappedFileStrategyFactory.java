package org.apache.iotdb.db.engine.merge.seqMerge;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.iotdb.db.engine.merge.IMergeFileSelector;
import org.apache.iotdb.db.engine.merge.IRecoverMergeTask;
import org.apache.iotdb.db.engine.merge.MergeCallback;
import org.apache.iotdb.db.engine.merge.MergeFileStrategyFactory;
import org.apache.iotdb.db.engine.merge.MergeTask;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.seqMerge.inplace.selector.InplaceMaxOverLappedFileSelector;
import org.apache.iotdb.db.engine.merge.seqMerge.inplace.task.InplaceMergeTask;
import org.apache.iotdb.db.engine.merge.seqMerge.inplace.task.RecoverInplaceMergeTask;
import org.apache.iotdb.db.engine.merge.seqMerge.squeeze.selector.SqueezeMaxOverLappedFileSelector;
import org.apache.iotdb.db.engine.merge.seqMerge.squeeze.task.RecoverSqueezeMergeTask;
import org.apache.iotdb.db.engine.merge.seqMerge.squeeze.task.SqueezeMergeTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

public class MergeOverlappedFileStrategyFactory extends MergeFileStrategyFactory {
  private MergeOverlappedFilesStrategy mergeOverlappedFilesStrategy;
  private boolean isFullMerge;
  private static final String FULL_MERGE = "FULL_MERGE";
  private static final String APPEND_MERGE = "APPEND_MEGRE";

  public MergeOverlappedFileStrategyFactory(
      MergeOverlappedFilesStrategy mergeOverlappedFilesStrategy, boolean isFullMerge) {
    this.mergeOverlappedFilesStrategy = mergeOverlappedFilesStrategy;
    this.isFullMerge = isFullMerge;
  }

  @Override
  public IMergeFileSelector getFileSelector(Collection<TsFileResource> seqFiles,
      Collection<TsFileResource> unseqFiles, long dataTTL, String storageGroupName,
      String storageGroupSysDir) {
    switch (mergeOverlappedFilesStrategy) {
      case INPLACE:
        return new InplaceMaxOverLappedFileSelector(seqFiles, unseqFiles, dataTTL);
      case SQUEEZE:
      default:
        return new SqueezeMaxOverLappedFileSelector(seqFiles, unseqFiles, dataTTL);
    }
  }

  @Override
  public MergeTask getMergeTask(MergeResource mergeResource, String storageGroupSysDir,
      MergeCallback callback, String taskName, String storageGroupName) {
    switch (mergeOverlappedFilesStrategy) {
      case INPLACE:
        return new InplaceMergeTask(mergeResource, storageGroupSysDir, callback, taskName,
            isFullMerge, storageGroupName);
      case SQUEEZE:
        return new SqueezeMergeTask(mergeResource, storageGroupSysDir, callback, taskName,
            storageGroupName);
    }
    return null;
  }

  @Override
  public IRecoverMergeTask getRecoverMergeTask(List<TsFileResource> seqTsFiles,
      List<TsFileResource> unseqTsFiles, String storageGroupSysDir, MergeCallback callback,
      String taskName, String storageGroupName) {
    switch (mergeOverlappedFilesStrategy) {
      case SQUEEZE:
        return new RecoverSqueezeMergeTask(seqTsFiles,
            unseqTsFiles, storageGroupSysDir, callback, taskName, storageGroupName);
      case INPLACE:
      default:
        return new RecoverInplaceMergeTask(seqTsFiles,
            unseqTsFiles, storageGroupSysDir, callback, taskName,
            isFullMerge, storageGroupName);
    }
  }

  public MergeOverlappedFilesStrategy getMergeOverlappedFilesStrategy() {
    return mergeOverlappedFilesStrategy;
  }

  public boolean isFullMerge() {
    return isFullMerge;
  }

  @Override
  public String toString(){
    StringBuilder result = new StringBuilder();
    result.append(mergeOverlappedFilesStrategy.toString());
    result.append("-");
    result.append(isFullMerge?FULL_MERGE:APPEND_MERGE);
    return new String(result);
  }

  public static MergeOverlappedFileStrategyFactory valueOf(String input){
    String[] result = input.split("-");
    MergeOverlappedFilesStrategy mergeOverlappedFilesStrategy = MergeOverlappedFilesStrategy.valueOf(result[0]);
    boolean isFullMerge;
    switch (result[1]){
      case FULL_MERGE:
        isFullMerge=true;
        break;
      case APPEND_MERGE:
      default:
        isFullMerge=false;
    }
    return new MergeOverlappedFileStrategyFactory(mergeOverlappedFilesStrategy,isFullMerge);
  }
}
