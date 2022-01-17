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
package org.apache.iotdb.db.engine.compaction.cross;

import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.RewriteCrossSpaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.task.RewriteCrossCompactionRecoverTask;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.task.RewriteCrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;

import java.io.File;
import java.util.List;

public enum CrossCompactionStrategy {
  INPLACE_COMPACTION;

  public static CrossCompactionStrategy getCrossCompactionStrategy(String name) {
    if ("INPLACE_COMPACTION".equalsIgnoreCase(name)) {
      return INPLACE_COMPACTION;
    }
    throw new RuntimeException("Illegal Cross Compaction Strategy " + name);
  }

  public AbstractCrossSpaceCompactionTask getCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartitionId,
      String storageGroupDir,
      TsFileManager tsFileManager,
      TsFileResourceList seqTsFileResourceList,
      TsFileResourceList UNseqTsFileResourceList,
      List<TsFileResource> selectedSeqTsFileResourceList,
      List<TsFileResource> selectedUnSeqTsFileResourceList) {
    switch (this) {
      case INPLACE_COMPACTION:
      default:
        return new RewriteCrossSpaceCompactionTask(
            logicalStorageGroupName,
            virtualStorageGroupName,
            timePartitionId,
            storageGroupDir,
            tsFileManager,
            seqTsFileResourceList,
            UNseqTsFileResourceList,
            selectedSeqTsFileResourceList,
            selectedUnSeqTsFileResourceList,
            CompactionTaskManager.currentTaskNum);
    }
  }

  public AbstractCrossSpaceCompactionTask getCompactionRecoverTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartitionId,
      String storageGroupDir,
      TsFileResourceList seqTsFileResourceList,
      TsFileResourceList unSeqTsFileResourceList,
      File logFile,
      TsFileManager tsFileManager) {
    switch (this) {
      case INPLACE_COMPACTION:
      default:
        return new RewriteCrossCompactionRecoverTask(
            logicalStorageGroupName,
            virtualStorageGroupName,
            timePartitionId,
            storageGroupDir,
            seqTsFileResourceList,
            unSeqTsFileResourceList,
            logFile,
            CompactionTaskManager.currentTaskNum,
            tsFileManager);
    }
  }

  public AbstractCrossSpaceCompactionSelector getCompactionSelector(
      String logicalStorageGroupName,
      String virtualGroupId,
      String storageGroupDir,
      long timePartition,
      TsFileManager tsFileManager,
      TsFileResourceList sequenceFileList,
      TsFileResourceList unsequenceFileList,
      CrossSpaceCompactionTaskFactory taskFactory) {
    switch (this) {
      case INPLACE_COMPACTION:
      default:
        return new RewriteCrossSpaceCompactionSelector(
            logicalStorageGroupName,
            virtualGroupId,
            storageGroupDir,
            timePartition,
            tsFileManager,
            sequenceFileList,
            unsequenceFileList,
            taskFactory);
    }
  }
}
