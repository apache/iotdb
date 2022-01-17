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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.compaction.CompactionScheduler;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Set;

/**
 * CompactionRecoverTask execute the recover process for all compaction task sequentially, including
 * InnerCompactionTask in sequence/unsequence space, CrossSpaceCompaction.
 */
public class CompactionRecoverTask {
  private static final Logger logger = LoggerFactory.getLogger(CompactionRecoverTask.class);
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
    recoverCrossCompaction();
    logger.info("try to synchronize CompactionScheduler");
    CompactionScheduler.decPartitionCompaction(
        logicalStorageGroupName + "-" + virtualStorageGroupId, 0);
  }

  private void recoverCrossCompaction() throws Exception {
    Set<Long> timePartitions = tsFileManager.getTimePartitions();
    List<String> sequenceDirs = DirectoryManager.getInstance().getAllSequenceFileFolders();
    for (String dir : sequenceDirs) {
      String storageGroupDir =
          dir + File.separator + logicalStorageGroupName + File.separator + virtualStorageGroupId;
      for (Long timePartition : timePartitions) {
        String timePartitionDir = storageGroupDir + File.separator + timePartition;
        File[] compactionLogs =
            RewriteCrossSpaceCompactionLogger.findCrossSpaceCompactionLogs(timePartitionDir);
        for (File compactionLog : compactionLogs) {
          logger.info("calling cross compaction task");
          IoTDBDescriptor.getInstance()
              .getConfig()
              .getCrossCompactionStrategy()
              .getCompactionRecoverTask(
                  logicalStorageGroupName,
                  virtualStorageGroupId,
                  timePartition,
                  storageGroupDir,
                  compactionLog,
                  tsFileManager)
              .call();
        }
      }
    }
  }
}
