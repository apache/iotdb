package org.apache.iotdb.db.engine.merge.strategy.small;

import java.util.Collection;
import org.apache.iotdb.db.engine.merge.FileRecoverStrategy;
import org.apache.iotdb.db.engine.merge.IRecoverMergeTask;
import org.apache.iotdb.db.engine.merge.MergeCallback;
import org.apache.iotdb.db.engine.merge.strategy.small.regularization.recover.RecoverRegularizationMergeTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

public enum SizeBasedFileRecoverStrategy implements FileRecoverStrategy {
  REGULARIZATION;

  @Override
  public IRecoverMergeTask getRecoverMergeTask(Collection<TsFileResource> seqTsFiles,
      Collection<TsFileResource> unseqTsFiles, String storageGroupSysDir, MergeCallback callback,
      String taskName, String storageGroupName) {
    switch (this) {
      case REGULARIZATION:
      default:
        return new RecoverRegularizationMergeTask(seqTsFiles,
            unseqTsFiles, storageGroupSysDir, callback, taskName, storageGroupName);
    }
  }
}
