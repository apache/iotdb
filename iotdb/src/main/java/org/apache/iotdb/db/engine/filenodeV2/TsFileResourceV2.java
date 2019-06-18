package org.apache.iotdb.db.engine.filenodeV2;

import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.engine.filenode.TsFileResource;
import org.apache.iotdb.db.engine.modification.ModificationFile;

public class TsFileResourceV2 {

  private File file;
//  private Map<String, Long> startTimeMap;
//  private Map<String, Long> endTimeMap;

  private boolean sealed;
  private transient ModificationFile modFile;

  public long getFileSize() {
    return file.length();
  }

}
