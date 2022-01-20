/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.engine.compaction.task;

import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.task.RewriteCrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.FakedTsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import java.util.List;

public class FakedCrossSpaceCompactionTask extends RewriteCrossSpaceCompactionTask {
  public FakedCrossSpaceCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartitionId,
      String storageGroupDir,
      TsFileManager tsFileManager,
      List<TsFileResource> selectedSeqTsFileResourceList,
      List<TsFileResource> selectedUnSeqTsFileResourceList) {
    super(
        logicalStorageGroupName,
        virtualStorageGroupName,
        timePartitionId,
        storageGroupDir,
        tsFileManager,
        selectedSeqTsFileResourceList,
        selectedUnSeqTsFileResourceList,
        CompactionTaskManager.currentTaskNum);
  }

  @Override
  protected void doCompaction() {
    long totalUnseqFileSize = 0;
    for (TsFileResource resource : selectedUnSeqTsFileResourceList) {
      totalUnseqFileSize += resource.getTsFileSize();
    }
    long avgSizeAddToSeqFile = totalUnseqFileSize / selectedSeqTsFileResourceList.size();
    for (TsFileResource resource : selectedSeqTsFileResourceList) {
      ((FakedTsFileResource) resource)
          .setTsFileSize(resource.getTsFileSize() + avgSizeAddToSeqFile);
    }
    selectedSeqTsFileResourceList.clear();
    selectedUnSeqTsFileResourceList.clear();
  }
}
