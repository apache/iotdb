package org.apache.iotdb.db.engine.merge.seqMerge.inplace.selector;

import java.util.Collection;
import org.apache.iotdb.db.engine.merge.BaseFileSeriesSelector;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.utils.SelectorContext;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.tsfile.utils.Pair;

public class InplaceMaxSeriesMergeFileSelector extends BaseFileSeriesSelector {

  @Override
  public Pair<MergeResource, SelectorContext> selectMergedFiles() throws MergeException {
    return select();
  }

  public InplaceMaxSeriesMergeFileSelector(Collection<TsFileResource> seqFiles,
      Collection<TsFileResource> unseqFiles, long budget) {
    this(seqFiles, unseqFiles, budget, Long.MIN_VALUE);
  }

  public InplaceMaxSeriesMergeFileSelector(Collection<TsFileResource> seqFiles,
      Collection<TsFileResource> unseqFiles, long budget, long timeLowerBound) {
    this.selectorContext = new SelectorContext();
    this.maxFileMergeFileSelector = new InplaceMaxFileSelector(seqFiles, unseqFiles,
        budget, timeLowerBound);
  }
}
