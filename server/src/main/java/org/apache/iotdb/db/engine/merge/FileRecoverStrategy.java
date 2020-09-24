package org.apache.iotdb.db.engine.merge;

import java.util.Collection;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

public interface FileRecoverStrategy {

  IRecoverMergeTask getRecoverMergeTask(Collection<TsFileResource> seqTsFiles,
      Collection<TsFileResource> unseqTsFiles, String storageGroupSysDir, MergeCallback callback,
      String taskName, String storageGroupName);
}
