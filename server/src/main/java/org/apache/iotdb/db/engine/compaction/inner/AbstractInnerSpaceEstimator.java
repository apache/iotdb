package org.apache.iotdb.db.engine.compaction.inner;

import org.apache.iotdb.db.engine.compaction.cross.utils.AbstractCompactionEstimator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import java.util.List;

public abstract class AbstractInnerSpaceEstimator extends AbstractCompactionEstimator {
    public abstract long estimateInnerCompactionMemory(List<TsFileResource> resources);
}
