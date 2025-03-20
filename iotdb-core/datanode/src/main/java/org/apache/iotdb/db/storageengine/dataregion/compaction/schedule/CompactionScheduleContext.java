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

import org.apache.iotdb.db.service.metrics.CompactionMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.SettleCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CompactionScheduleContext {
  private int submitSeqInnerSpaceCompactionTaskNum = 0;
  private int submitUnseqInnerSpaceCompactionTaskNum = 0;
  private int submitCrossSpaceCompactionTaskNum = 0;
  private int submitInsertionCrossSpaceCompactionTaskNum = 0;
  private int submitSettleCompactionTaskNum = 0;

  // region TTL info
  private int fullyDirtyFileNum = 0;

  private int partiallyDirtyFileNum = 0;

  // end region

  private final Map<TsFileResource, DeviceTimeIndex> partitionFileDeviceInfoCache;
  private long cachedDeviceInfoSize = 0;

  private final Set<Long> timePartitionsDelayInsertionSelection;

  public CompactionScheduleContext() {
    this.partitionFileDeviceInfoCache = new HashMap<>();
    this.timePartitionsDelayInsertionSelection = new HashSet<>();
  }

  public void delayInsertionSelection(long timePartitionId) {
    timePartitionsDelayInsertionSelection.add(timePartitionId);
  }

  public boolean isInsertionSelectionDelayed(long timePartitionId) {
    return timePartitionsDelayInsertionSelection.remove(timePartitionId);
  }

  public void addResourceDeviceTimeIndex(
      TsFileResource tsFileResource, DeviceTimeIndex deviceTimeIndex) {
    partitionFileDeviceInfoCache.put(tsFileResource, deviceTimeIndex);
    long deviceTimeIndexSize =
        tsFileResource.getDeviceTimeIndexRamSize().orElse(deviceTimeIndex.calculateRamSize());
    cachedDeviceInfoSize += deviceTimeIndexSize;
    CompactionMetrics.getInstance().addSelectionCachedDeviceTimeIndexSize(deviceTimeIndexSize);
  }

  public DeviceTimeIndex getResourceDeviceInfo(TsFileResource resource) {
    return partitionFileDeviceInfoCache.get(resource);
  }

  public void clearTimePartitionDeviceInfoCache() {
    partitionFileDeviceInfoCache.clear();
    CompactionMetrics.getInstance()
        .decreaseSelectionCachedDeviceTimeIndexSize(cachedDeviceInfoSize);
    cachedDeviceInfoSize = 0;
  }

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
        partiallyDirtyFileNum += task.getProcessedFileNum();
        break;
      case INNER_UNSEQ:
        submitUnseqInnerSpaceCompactionTaskNum += 1;
        partiallyDirtyFileNum += task.getProcessedFileNum();
        break;
      case SETTLE:
        submitSettleCompactionTaskNum += 1;
        partiallyDirtyFileNum += ((SettleCompactionTask) task).getPartiallyDirtyFiles().size();
        fullyDirtyFileNum += ((SettleCompactionTask) task).getFullyDirtyFiles().size();
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

  public int getSubmitCompactionTaskNum() {
    return submitSeqInnerSpaceCompactionTaskNum
        + submitUnseqInnerSpaceCompactionTaskNum
        + submitCrossSpaceCompactionTaskNum
        + submitInsertionCrossSpaceCompactionTaskNum
        + submitSettleCompactionTaskNum;
  }

  public boolean hasSubmitTask() {
    return submitCrossSpaceCompactionTaskNum
            + submitInsertionCrossSpaceCompactionTaskNum
            + submitSeqInnerSpaceCompactionTaskNum
            + submitUnseqInnerSpaceCompactionTaskNum
            + submitSettleCompactionTaskNum
        > 0;
  }

  public int getFullyDirtyFileNum() {
    return fullyDirtyFileNum;
  }

  public int getPartiallyDirtyFileNum() {
    return partiallyDirtyFileNum;
  }
}
