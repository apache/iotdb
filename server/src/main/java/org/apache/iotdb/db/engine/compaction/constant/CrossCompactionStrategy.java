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
package org.apache.iotdb.db.engine.compaction.constant;

import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.cross.CrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.cross.ICrossSpaceSelector;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.RewriteCrossSpaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.performer.impl.ReadPointCompactionPerformer;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import java.util.List;

public enum CrossCompactionStrategy {
  REWRITE_COMPACTION;

  public static CrossCompactionStrategy getCrossCompactionStrategy(String name) {
    if ("REWRITE_COMPACTION".equalsIgnoreCase(name)) {
      return REWRITE_COMPACTION;
    }
    throw new RuntimeException("Illegal Cross Compaction Strategy " + name);
  }

  public CrossSpaceCompactionTask getCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartitionId,
      TsFileManager tsFileManager,
      List<TsFileResource> selectedSeqTsFileResourceList,
      List<TsFileResource> selectedUnSeqTsFileResourceList) {
    switch (this) {
      case REWRITE_COMPACTION:
      default:
        return new CrossSpaceCompactionTask(
            logicalStorageGroupName,
            virtualStorageGroupName,
            timePartitionId,
            tsFileManager,
            selectedSeqTsFileResourceList,
            selectedUnSeqTsFileResourceList,
            new ReadPointCompactionPerformer(),
            CompactionTaskManager.currentTaskNum);
    }
  }

  public ICrossSpaceSelector getCompactionSelector(
      String logicalStorageGroupName,
      String virtualGroupId,
      long timePartition,
      TsFileManager tsFileManager) {
    switch (this) {
      case REWRITE_COMPACTION:
      default:
        return new RewriteCrossSpaceCompactionSelector(
            logicalStorageGroupName, virtualGroupId, timePartition, tsFileManager);
    }
  }
}
