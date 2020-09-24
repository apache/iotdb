package org.apache.iotdb.db.engine.merge.strategy.overlapped;

import java.io.File;
import java.util.Collection;
import org.apache.iotdb.db.engine.merge.FileSelectorStrategy;
import org.apache.iotdb.db.engine.merge.IMergeFileSelector;
import org.apache.iotdb.db.engine.merge.strategy.overlapped.inplace.selector.InplaceMaxOverLappedFileSelector;
import org.apache.iotdb.db.engine.merge.strategy.overlapped.squeeze.selector.SqueezeMaxOverLappedFileSelector;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

public enum OverlappedBasedFileSelectorStrategy implements FileSelectorStrategy {
  INPLACE,
  SQUEEZE;

  @Override
  public IMergeFileSelector getFileSelector(Collection<TsFileResource> seqFiles,
      Collection<TsFileResource> unseqFiles, long dataTTL, String storageGroupName,
      File storageGroupSysDir) {
    switch (this) {
      case INPLACE:
        return new InplaceMaxOverLappedFileSelector(seqFiles, unseqFiles, dataTTL, storageGroupName,
            storageGroupSysDir);
      case SQUEEZE:
      default:
        return new SqueezeMaxOverLappedFileSelector(seqFiles, unseqFiles, dataTTL, storageGroupName,
            storageGroupSysDir);
    }
  }
}
