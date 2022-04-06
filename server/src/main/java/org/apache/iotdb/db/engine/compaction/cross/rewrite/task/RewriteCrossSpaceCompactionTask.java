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
package org.apache.iotdb.db.engine.compaction.cross.rewrite.task;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.compaction.cross.AbstractCrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.performer.AbstractCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.performer.ReadPointCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RewriteCrossSpaceCompactionTask extends AbstractCrossSpaceCompactionTask {
  private static final Logger logger =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  protected List<TsFileResource> selectedSeqTsFileResourceList;
  protected List<TsFileResource> selectedUnSeqTsFileResourceList;
  protected TsFileResourceList seqTsFileResourceList;
  protected TsFileResourceList unseqTsFileResourceList;
  private AbstractCompactionPerformer performer;

  public RewriteCrossSpaceCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartitionId,
      TsFileManager tsFileManager,
      List<TsFileResource> selectedSeqTsFileResourceList,
      List<TsFileResource> selectedUnSeqTsFileResourceList,
      AtomicInteger currentTaskNum) {
    super(
        logicalStorageGroupName + "-" + virtualStorageGroupName,
        timePartitionId,
        currentTaskNum,
        selectedSeqTsFileResourceList,
        selectedUnSeqTsFileResourceList,
        tsFileManager);
  }

  @Override
  protected void performCompaction() throws Exception {
    performer =
        new ReadPointCompactionPerformer(
            selectedSequenceFiles, selectedUnsequenceFiles, targetTsfileResourceList);
    performer.perform();
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask other) {
    if (other instanceof RewriteCrossSpaceCompactionTask) {
      RewriteCrossSpaceCompactionTask otherTask = (RewriteCrossSpaceCompactionTask) other;
      return otherTask.selectedSeqTsFileResourceList.equals(selectedSeqTsFileResourceList)
          && otherTask.selectedUnSeqTsFileResourceList.equals(selectedUnSeqTsFileResourceList);
    }
    return false;
  }
}
