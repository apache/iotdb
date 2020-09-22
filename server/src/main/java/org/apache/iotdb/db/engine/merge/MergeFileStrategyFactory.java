package org.apache.iotdb.db.engine.merge;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

public abstract class MergeFileStrategyFactory {
  @Override
  public abstract String toString();

  public abstract IMergeFileSelector getFileSelector(Collection<TsFileResource> seqFiles,
      Collection<TsFileResource> unseqFiles, long dataTTL, String storageGroupName,
      String storageGroupSysDir);

  public abstract Callable<Void> getMergeTask(MergeResource mergeResource, String storageGroupSysDir,
      MergeCallback callback, String taskName, String storageGroupName);

  public abstract IRecoverMergeTask getRecoverMergeTask(List<TsFileResource> seqTsFiles,
      List<TsFileResource> unseqTsFiles, String storageGroupSysDir, MergeCallback callback,
      String taskName, String storageGroupName);
}
