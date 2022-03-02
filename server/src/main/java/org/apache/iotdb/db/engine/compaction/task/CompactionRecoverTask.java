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
package org.apache.iotdb.db.engine.compaction.task;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.compaction.utils.log.CompactionLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.regex.Pattern;

/**
 * CompactionRecoverTask execute the recover process for all compaction task sequentially, including
 * InnerCompactionTask in sequence/unsequence space, CrossSpaceCompaction.
 */
public class CompactionRecoverTask {
  private static final Logger logger =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private TsFileManager tsFileManager;
  private String logicalStorageGroupName;
  private String virtualStorageGroupId;

  public CompactionRecoverTask(
      TsFileManager tsFileManager, String logicalStorageGroupName, String virtualStorageGroupId) {
    this.tsFileManager = tsFileManager;
    this.logicalStorageGroupName = logicalStorageGroupName;
    this.virtualStorageGroupId = virtualStorageGroupId;
  }

  public void recoverCrossSpaceCompaction() throws Exception {
    logger.info("recovering cross compaction");
    recoverCrossCompactionFromOldVersion();
    recoverCrossCompaction();
    logger.info("try to synchronize CompactionScheduler");
  }

  private void recoverCrossCompaction() throws Exception {
    List<String> sequenceDirs = DirectoryManager.getInstance().getAllSequenceFileFolders();
    for (String dir : sequenceDirs) {
      File storageGroupDir =
          new File(
              dir
                  + File.separator
                  + logicalStorageGroupName
                  + File.separator
                  + virtualStorageGroupId);
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
            CompactionLogger.findCrossSpaceCompactionLogs(timePartitionDir.getPath());
        for (File compactionLog : compactionLogs) {
          logger.info("calling cross compaction task");
          IoTDBDescriptor.getInstance()
              .getConfig()
              .getCrossCompactionStrategy()
              .getCompactionRecoverTask(
                  logicalStorageGroupName,
                  virtualStorageGroupId,
                  Long.parseLong(timePartitionDir.getName()),
                  compactionLog,
                  tsFileManager)
              .call();
        }
      }
    }
  }

  private void recoverCrossCompactionFromOldVersion() throws Exception {
    // check whether there is old compaction log from previous version (<0.13)
    File mergeLogFromOldVersion =
        new File(
            tsFileManager.getStorageGroupDir()
                + File.separator
                + CompactionLogger.CROSS_COMPACTION_LOG_NAME_FEOM_OLD);
    if (mergeLogFromOldVersion.exists()) {
      logger.info("calling cross compaction task to recover from previous version.");
      IoTDBDescriptor.getInstance()
          .getConfig()
          .getCrossCompactionStrategy()
          .getCompactionRecoverTask(
              logicalStorageGroupName,
              virtualStorageGroupId,
              0L,
              mergeLogFromOldVersion,
              tsFileManager)
          .call();
    }
  }
}
