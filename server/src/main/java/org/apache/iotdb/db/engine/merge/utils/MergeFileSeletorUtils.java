package org.apache.iotdb.db.engine.merge.utils;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

public class MergeFileSeletorUtils {

  public static boolean filterResource(TsFileResource res, long timeLowerBound) {
    return res.isClosed() && !res.isDeleted() && res.stillLives(timeLowerBound);
  }

}
