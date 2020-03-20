package org.apache.iotdb.db.engine.merge;

import java.util.List;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;

public interface IMergeFileSelector {

  MergeResource selectMergedFiles(List<TsFileResource> seqFiles, List<TsFileResource> unseqfiles,
      long budget, long timeLowerBound) throws MergeException;
}
