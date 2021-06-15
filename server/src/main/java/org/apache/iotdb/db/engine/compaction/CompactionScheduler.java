package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionScheduler {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompactionScheduler.class);

  public static void compactionSchedule(TsFileManagement tsFileManagement, long timePartition) {}

  private static void doInnerSpaceCompaction(TsFileResourceList srcFiles, boolean isSequence) {}

  private static void doCrossSpaceCompaction(
      TsFileResourceList sequenceFiles, TsFileResourceList unsequenceFiles) {}

  private static boolean selectInSpaceFiles(
      TsFileResourceList tsFileResources, boolean isSequence, TsFileResourceList selectedFiles) {
    return false;
  }

  private static boolean selectCrossSpaceFiles(TsFileResourceList unsequenceFiles) {
    return false;
  }
}
