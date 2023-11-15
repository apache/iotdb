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

public class CompactionScheduleSummary {
  private int submitSeqInnerSpaceCompactionTaskNum = 0;
  private int submitUnseqInnerSpaceCompactionTaskNum = 0;
  private int submitCrossSpaceCompactionTaskNum = 0;
  private int submitInsertionCrossSpaceCompactionTaskNum = 0;

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

  public boolean hasSubmitTask() {
    return submitCrossSpaceCompactionTaskNum
            + submitInsertionCrossSpaceCompactionTaskNum
            + submitSeqInnerSpaceCompactionTaskNum
            + submitUnseqInnerSpaceCompactionTaskNum
        > 0;
  }
}
