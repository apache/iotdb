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

package org.apache.iotdb.db.storageengine.dataregion.compaction.schedule;

import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.SettleCompactionTask;

public class CompactionScheduleSummary {
  private int submitSeqInnerSpaceCompactionTaskNum = 0;
  private int submitUnseqInnerSpaceCompactionTaskNum = 0;
  private int submitCrossSpaceCompactionTaskNum = 0;
  private int submitInsertionCrossSpaceCompactionTaskNum = 0;
  private int submitSettleCompactionTaskNum = 0;

  // region TTL info
  private int allDeletedFileNum = 0;

  private int partialDeletedFileNum = 0;
  // end region

  public void incrementSubmitTaskNum(CompactionTaskType taskType, int num) {
    switch (taskType) {
      case INNER_SEQ:
        submitSeqInnerSpaceCompactionTaskNum += num;
        break;
      case INNER_UNSEQ:
        submitUnseqInnerSpaceCompactionTaskNum += num;
        break;
      case CROSS:
        submitCrossSpaceCompactionTaskNum += num;
        break;
      case INSERTION:
        submitInsertionCrossSpaceCompactionTaskNum += num;
        break;
      case SETTLE:
        submitSettleCompactionTaskNum += num;
        break;
      default:
        break;
    }
  }

  public void updateTTLInfo(AbstractCompactionTask task) {
    switch (task.getCompactionTaskType()) {
      case INNER_SEQ:
        submitSeqInnerSpaceCompactionTaskNum += 1;
        partialDeletedFileNum += task.getProcessedFileNum();
        break;
      case INNER_UNSEQ:
        submitUnseqInnerSpaceCompactionTaskNum += 1;
        partialDeletedFileNum += task.getProcessedFileNum();
        break;
      case SETTLE:
        submitSettleCompactionTaskNum += 1;
        partialDeletedFileNum += ((SettleCompactionTask) task).getPartialDeletedFiles().size();
        allDeletedFileNum += ((SettleCompactionTask) task).getFullyDeletedFiles().size();
        break;
      default:
        break;
    }
  }

  public int getSubmitCrossSpaceCompactionTaskNum() {
    return submitCrossSpaceCompactionTaskNum;
  }

  public int getSubmitInsertionCrossSpaceCompactionTaskNum() {
    return submitInsertionCrossSpaceCompactionTaskNum;
  }

  public int getSubmitSeqInnerSpaceCompactionTaskNum() {
    return submitSeqInnerSpaceCompactionTaskNum;
  }

  public int getSubmitUnseqInnerSpaceCompactionTaskNum() {
    return submitUnseqInnerSpaceCompactionTaskNum;
  }

  public int getSubmitSettleCompactionTaskNum() {
    return submitSettleCompactionTaskNum;
  }

  public boolean hasSubmitTask() {
    return submitCrossSpaceCompactionTaskNum
            + submitInsertionCrossSpaceCompactionTaskNum
            + submitSeqInnerSpaceCompactionTaskNum
            + submitUnseqInnerSpaceCompactionTaskNum
            + submitSettleCompactionTaskNum
        > 0;
  }

  public int getAllDeletedFileNum() {
    return allDeletedFileNum;
  }

  public int getPartialDeletedFileNum() {
    return partialDeletedFileNum;
  }
}
