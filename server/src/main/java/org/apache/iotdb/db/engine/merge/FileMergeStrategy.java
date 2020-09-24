package org.apache.iotdb.db.engine.merge;

import java.util.List;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.metadata.PartialPath;

public interface FileMergeStrategy {

  MergeTask getMergeTask(MergeResource mergeResource, String storageGroupSysDir,
      MergeCallback callback, String taskName, String storageGroupName);

  BaseMergeSchedulerTask getMergeSchedulerTask(MergeContext context, String taskName,
      MergeLogger mergeLogger,
      MergeResource mergeResource, List<PartialPath> unmergedSeries, String storageGroupName);
}
