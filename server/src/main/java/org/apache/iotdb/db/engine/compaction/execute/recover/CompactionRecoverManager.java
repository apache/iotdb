/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.compaction.execute.recover;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.regex.Pattern;

import static org.apache.iotdb.db.engine.compaction.execute.utils.log.CompactionLogger.CROSS_COMPACTION_LOG_NAME_FROM_OLD;
import static org.apache.iotdb.db.engine.compaction.execute.utils.log.CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX_FROM_OLD;

/**
 * CompactionRecoverManager searches compaction log and call {@link CompactionRecoverTask} to
 * execute the recover process for all compaction task sequentially, including InnerCompactionTask
 * in sequence/unsequence space, CrossSpaceCompaction.
 */
public class CompactionRecoverManager {
  private static final Logger logger =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private final TsFileManager tsFileManager;
  private final String logicalStorageGroupName;
  private final String dataRegionId;

  public CompactionRecoverManager(
      TsFileManager tsFileManager, String logicalStorageGroupName, String dataRegionId) {
    this.tsFileManager = tsFileManager;
    this.logicalStorageGroupName = logicalStorageGroupName;
    this.dataRegionId = dataRegionId;
  }

  public void recoverInnerSpaceCompaction(boolean isSequence) {
    logger.info("recovering inner compaction");
    recoverCompactionBefore013(true);
    recoverCompaction(true, isSequence);
  }

  public void recoverCrossSpaceCompaction() {
    logger.info("recovering cross compaction");
    recoverCompactionBefore013(false);
    recoverCompaction(false, true);
  }

  private void recoverCompaction(boolean isInnerSpace, boolean isLogSequence) {
    List<String> dirs;
    if (isLogSequence) {
      dirs = DirectoryManager.getInstance().getAllSequenceFileFolders();
    } else {
      dirs = DirectoryManager.getInstance().getAllUnSequenceFileFolders();
    }
    for (String dir : dirs) {
      File storageGroupDir =
          new File(dir + File.separator + logicalStorageGroupName + File.separator + dataRegionId);
      if (!storageGroupDir.exists()) {
        return;
      }
      File[] timePartitionDirs = storageGroupDir.listFiles();
      if (timePartitionDirs == null) {
        return;
      }
      for (File timePartitionDir : timePartitionDirs) {
        if (!timePartitionDir.isDirectory()
            || !Pattern.compile("[0-9]*").matcher(timePartitionDir.getName()).matches()) {
          continue;
        }
        File[] compactionLogs =
            CompactionLogger.findCompactionLogs(isInnerSpace, timePartitionDir.getPath());
        for (File compactionLog : compactionLogs) {
          logger.info("Calling compaction recover task.");
          new CompactionRecoverTask(
                  logicalStorageGroupName, dataRegionId, tsFileManager, compactionLog, isInnerSpace)
              .doCompaction();
        }
      }
    }
  }

  /** Check whether there is old compaction log from previous version (<0.13) and recover it. */
  private void recoverCompactionBefore013(boolean isInnerSpace) {
    String oldLogName =
        isInnerSpace
            ? logicalStorageGroupName + INNER_COMPACTION_LOG_NAME_SUFFIX_FROM_OLD
            : CROSS_COMPACTION_LOG_NAME_FROM_OLD;
    File logFileFromOld =
        FSFactoryProducer.getFSFactory().getFile(tsFileManager.getStorageGroupDir(), oldLogName);

    if (logFileFromOld.exists()) {
      logger.info("Calling compaction task to recover from previous version.");
      new CompactionRecoverTask(
              logicalStorageGroupName, dataRegionId, tsFileManager, logFileFromOld, isInnerSpace)
          .doCompaction();
    }
  }
}
