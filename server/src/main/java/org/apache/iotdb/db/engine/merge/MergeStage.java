package org.apache.iotdb.db.engine.merge;

import java.io.File;
import java.util.Collection;
import java.util.List;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.PartialPath;

public abstract class MergeStage {

  @Override
  public abstract String toString();

  public abstract IMergeFileSelector getFileSelector(Collection<TsFileResource> seqFiles,
      Collection<TsFileResource> unseqFiles, long dataTTL, String storageGroupName,
      File storageGroupSysDir);

  public abstract MergeTask getMergeTask(MergeResource mergeResource, String storageGroupSysDir,
      MergeCallback callback, String taskName, String storageGroupName);

  public abstract BaseMergeSchedulerTask getMergeSchedulerTask(MergeContext context,
      String taskName,
      MergeLogger mergeLogger,
      MergeResource mergeResource, List<PartialPath> unmergedSeries, String storageGroupName);

  public abstract IRecoverMergeTask getRecoverMergeTask(Collection<TsFileResource> seqTsFiles,
      Collection<TsFileResource> unseqTsFiles, String storageGroupSysDir, MergeCallback callback,
      String taskName, String storageGroupName);
}
