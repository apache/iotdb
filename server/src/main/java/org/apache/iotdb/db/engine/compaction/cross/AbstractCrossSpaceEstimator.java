package org.apache.iotdb.db.engine.compaction.cross;

import org.apache.iotdb.db.engine.compaction.cross.utils.AbstractCompactionEstimator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import java.io.IOException;
import java.util.List;

public interface AbstractCrossSpaceEstimator extends AbstractCompactionEstimator {
    public abstract long estimateCrossCompactionMemory(
            List<TsFileResource> seqResources, TsFileResource unseqResource) throws IOException;
}
