package org.apache.iotdb.db.engine.merge.utils;

import java.util.List;
import java.util.Set;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.utils.UpgradeUtils;

public class MergeFileSelectorUtils {

  public static boolean filterResource(TsFileResource res, long timeLowerBound) {
    return res.isClosed() && !res.isDeleted() && res.stillLives(timeLowerBound);
  }

  public static boolean checkForUpgrade(TsFileResource unseqFile,
      Set<Integer> tmpSelectedSeqFiles, List<TsFileResource> seqFiles) {
    // reject the selection if it contains files that should be upgraded
    boolean isNeedUpgrade = false;
    if (UpgradeUtils.isNeedUpgrade(unseqFile)) {
      isNeedUpgrade = true;
    }
    for (Integer seqFileIdx : tmpSelectedSeqFiles) {
      if (UpgradeUtils.isNeedUpgrade(seqFiles.get(seqFileIdx))) {
        isNeedUpgrade = true;
        break;
      }
    }
    return isNeedUpgrade;
  }
}
