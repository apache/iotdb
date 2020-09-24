package org.apache.iotdb.db.engine.merge.strategy.small.regularization.selector;

import java.io.File;
import java.util.Collection;
import org.apache.iotdb.db.engine.merge.strategy.small.BaseSizeFileSelector;
import org.apache.iotdb.db.engine.merge.strategy.small.SizeBasedFileSelectorStrategy;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

public class PointRangeFileSelector extends BaseSizeFileSelector {

  public PointRangeFileSelector(Collection<TsFileResource> seqFiles, long dataTTL,
      String storageGroupName, File storageGroupSysDir) {
    super(seqFiles, dataTTL, storageGroupName, storageGroupSysDir,
        SizeBasedFileSelectorStrategy.POINT_RANGE);
  }
}
