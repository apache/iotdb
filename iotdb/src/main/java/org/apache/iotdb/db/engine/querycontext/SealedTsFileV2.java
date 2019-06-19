package org.apache.iotdb.db.engine.querycontext;

import java.io.File;
import java.util.Map;
import org.apache.iotdb.db.engine.filenodeV2.TsFileResourceV2;

public class SealedTsFileV2 extends TsFileResourceV2 {

  public SealedTsFileV2(File file) {
    super(file);
  }

  public SealedTsFileV2(File file, Map<String, Long> startTimeMap,
      Map<String, Long> endTimeMap) {
    super(file, startTimeMap, endTimeMap);
  }

  @Override
  public TSFILE_TYPE getTsFileType() {
    return TSFILE_TYPE.SEALED;
  }
}
