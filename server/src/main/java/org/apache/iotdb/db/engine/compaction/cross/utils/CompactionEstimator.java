package org.apache.iotdb.db.engine.compaction.cross.utils;

import java.io.IOException;
import java.util.List;

public interface CompactionEstimator {

     long estimateMemory(int unseqIndex, List<Integer> seqIndexes) throws IOException;

}
