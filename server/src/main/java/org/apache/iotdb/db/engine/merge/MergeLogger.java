package org.apache.iotdb.db.engine.merge;

import java.io.IOException;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

public interface MergeLogger {

  void logAllTsEnd() throws IOException;

  void logNewFile(TsFileResource resource) throws IOException;
}
