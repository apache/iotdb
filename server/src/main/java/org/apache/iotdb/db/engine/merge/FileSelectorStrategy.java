package org.apache.iotdb.db.engine.merge;

import java.io.File;
import java.util.Collection;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

public interface FileSelectorStrategy {

  IMergeFileSelector getFileSelector(Collection<TsFileResource> seqFiles,
      Collection<TsFileResource> unseqFiles, long dataTTL, String storageGroupName,
      File storageGroupSysDir);
}
