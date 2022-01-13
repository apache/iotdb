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
package org.apache.iotdb.db.engine.compaction.cross.inplace;

import org.apache.iotdb.db.engine.compaction.cross.AbstractCrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.cross.inplace.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class InplaceCompactionTask extends AbstractCrossSpaceCompactionTask {

  private static final Logger LOGGER = LoggerFactory.getLogger("COMPACTION");
  protected String storageGroupDir;
  protected List<TsFileResource> selectedSeqTsFileResourceList;
  protected List<TsFileResource> selectedUnSeqTsFileResourceList;
  protected String logicalStorageGroupName;
  protected String virtualStorageGroupName;

  public InplaceCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartitionId,
      String storageGroupDir,
      List<TsFileResource> selectedSeqTsFileResourceList,
      List<TsFileResource> selectedUnSeqTsFileResourceList,
      AtomicInteger currentTaskNum) {
    super(
        logicalStorageGroupName + "-" + virtualStorageGroupName,
        timePartitionId,
        currentTaskNum,
        selectedSeqTsFileResourceList,
        selectedUnSeqTsFileResourceList);
    this.logicalStorageGroupName = logicalStorageGroupName;
    this.virtualStorageGroupName = virtualStorageGroupName;
    this.storageGroupDir = storageGroupDir;
    this.selectedSeqTsFileResourceList = selectedSeqTsFileResourceList;
    this.selectedUnSeqTsFileResourceList = selectedUnSeqTsFileResourceList;
  }

  @Override
  protected void doCompaction() throws Exception {
    String taskName = fullStorageGroupName + "-" + System.currentTimeMillis();
    CrossSpaceCompactionTask mergeTask =
        new CrossSpaceCompactionTask(
            selectedSeqTsFileResourceList,
            selectedUnSeqTsFileResourceList,
            storageGroupDir,
            taskName,
            logicalStorageGroupName);
    mergeTask.call();
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask other) {
    if (other instanceof InplaceCompactionTask) {
      InplaceCompactionTask otherTask = (InplaceCompactionTask) other;
      if (!otherTask.selectedSeqTsFileResourceList.equals(selectedSeqTsFileResourceList)
          || !otherTask.selectedUnSeqTsFileResourceList.equals(selectedUnSeqTsFileResourceList)) {
        return false;
      }
      return true;
    }
    return false;
  }
}
