package org.apache.iotdb.db.engine.merge;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.utils.Pair;

@FunctionalInterface
public interface IFileSelector {

  Pair<List<TsFileResource>, List<TsFileResource>> select(boolean useTightBound) throws IOException;
}
