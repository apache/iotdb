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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.metrics.CompactionMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICrossCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ISeqCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.IUnseqCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.SettleCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ArrayDeviceTimeIndex;
import org.apache.iotdb.db.utils.EncryptDBUtils;

import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.file.metadata.IDeviceID;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
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

  // region info
  private int fullyDirtyFileNum = 0;

  private int partiallyDirtyFileNum = 0;
  // end region

  private final Map<TsFileResource, ArrayDeviceTimeIndex> partitionFileDeviceInfoCache;
  private final Map<IDeviceID, IDeviceID> deviceIdCache;
  private long cachedDeviceInfoSize = 0;

  private final Set<Long> timePartitionsDelayInsertionSelection;

  private EncryptParameter encryptParameter;

  @TestOnly
  public CompactionScheduleContext() {
    this.partitionFileDeviceInfoCache = new HashMap<>();
    this.timePartitionsDelayInsertionSelection = new HashSet<>();
    this.deviceIdCache = new HashMap<>();
    this.encryptParameter = EncryptDBUtils.getDefaultFirstEncryptParam();
  }

  public CompactionScheduleContext(EncryptParameter encryptParameter) {
    this.partitionFileDeviceInfoCache = new HashMap<>();
    this.timePartitionsDelayInsertionSelection = new HashSet<>();
    this.deviceIdCache = new HashMap<>();
    this.encryptParameter = encryptParameter;
  }

  public void delayInsertionSelection(long timePartitionId) {
    timePartitionsDelayInsertionSelection.add(timePartitionId);
  }

  public boolean isInsertionSelectionDelayed(long timePartitionId) {
    return timePartitionsDelayInsertionSelection.remove(timePartitionId);
  }

  public void addResourceDeviceTimeIndex(
      TsFileResource tsFileResource, ArrayDeviceTimeIndex deviceTimeIndex) {
    partitionFileDeviceInfoCache.put(tsFileResource, deviceTimeIndex);
    long deviceTimeIndexSize =
        tsFileResource.getDeviceTimeIndexRamSize().orElse(deviceTimeIndex.calculateRamSize());
    cachedDeviceInfoSize += deviceTimeIndexSize;
    CompactionMetrics.getInstance().addSelectionCachedDeviceTimeIndexSize(deviceTimeIndexSize);
  }

  public ArrayDeviceTimeIndex getResourceDeviceInfo(TsFileResource resource) {
    return partitionFileDeviceInfoCache.get(resource);
  }

  public void clearTimePartitionDeviceInfoCache() {
    partitionFileDeviceInfoCache.clear();
    deviceIdCache.clear();
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

  public ISeqCompactionPerformer getSeqCompactionPerformer() {
    return IoTDBDescriptor.getInstance()
        .getConfig()
        .getInnerSeqCompactionPerformer()
        .createInstance(encryptParameter);
  }

  public IUnseqCompactionPerformer getUnseqCompactionPerformer() {
    IUnseqCompactionPerformer unseqCompactionPerformer =
        IoTDBDescriptor.getInstance()
            .getConfig()
            .getInnerUnseqCompactionPerformer()
            .createInstance(encryptParameter);
    return unseqCompactionPerformer;
  }

  public ICrossCompactionPerformer getCrossCompactionPerformer() {
    return IoTDBDescriptor.getInstance()
        .getConfig()
        .getCrossCompactionPerformer()
        .createInstance(encryptParameter);
  }

  public IDeviceID.Deserializer getCachedDeviceIdDeserializer() {
    return new CachedIDeviceIdDeserializer();
  }

  private class CachedIDeviceIdDeserializer implements IDeviceID.Deserializer {

    @Override
    public IDeviceID deserializeFrom(ByteBuffer byteBuffer) {
      IDeviceID deviceId = IDeviceID.Deserializer.DEFAULT_DESERIALIZER.deserializeFrom(byteBuffer);
      return deviceIdCache.computeIfAbsent(deviceId, k -> deviceId);
    }

    @Override
    public IDeviceID deserializeFrom(InputStream inputStream) throws IOException {
      IDeviceID deviceId = IDeviceID.Deserializer.DEFAULT_DESERIALIZER.deserializeFrom(inputStream);
      return deviceIdCache.computeIfAbsent(deviceId, k -> deviceId);
    }
  }
}
