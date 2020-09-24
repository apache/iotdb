package org.apache.iotdb.db.engine.merge.strategy.overlapped;

import java.util.Collection;
import org.apache.iotdb.db.engine.merge.FileRecoverStrategy;
import org.apache.iotdb.db.engine.merge.IRecoverMergeTask;
import org.apache.iotdb.db.engine.merge.MergeCallback;
import org.apache.iotdb.db.engine.merge.strategy.overlapped.inplace.recover.RecoverInplaceAppendMergeTask;
import org.apache.iotdb.db.engine.merge.strategy.overlapped.inplace.recover.RecoverInplaceFullMergeTask;
import org.apache.iotdb.db.engine.merge.strategy.overlapped.squeeze.recover.RecoverSqueezeAppendMergeTask;
import org.apache.iotdb.db.engine.merge.strategy.overlapped.squeeze.recover.RecoverSqueezeFullMergeTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

public enum OverlappedBasedFileRecoverStrategy implements FileRecoverStrategy {
  INPLACE_FULL_MERGE,
  INPLACE_APPEND_MERGE,
  SQUEEZE_FULL_MERGE,
  SQUEEZE_APPEND_MERGE;

  @Override
  public IRecoverMergeTask getRecoverMergeTask(Collection<TsFileResource> seqTsFiles,
      Collection<TsFileResource> unseqTsFiles, String storageGroupSysDir, MergeCallback callback,
      String taskName, String storageGroupName) {
    switch (this) {
      case SQUEEZE_FULL_MERGE:
        return new RecoverSqueezeFullMergeTask(seqTsFiles,
            unseqTsFiles, storageGroupSysDir, callback, taskName, storageGroupName);
      case SQUEEZE_APPEND_MERGE:
        return new RecoverSqueezeAppendMergeTask(seqTsFiles,
            unseqTsFiles, storageGroupSysDir, callback, taskName, storageGroupName);
      case INPLACE_FULL_MERGE:
        return new RecoverInplaceFullMergeTask(seqTsFiles, unseqTsFiles, storageGroupSysDir,
            callback, taskName, storageGroupName);
      case INPLACE_APPEND_MERGE:
      default:
        return new RecoverInplaceAppendMergeTask(seqTsFiles,
            unseqTsFiles, storageGroupSysDir, callback, taskName, storageGroupName);
    }
  }
}
