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

package org.apache.iotdb.db.storageengine.dataregion.read.filescan.impl;

import org.apache.iotdb.db.utils.ModificationUtils;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.BitMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.ARRAY_SIZE;

public class MemAlignedChunkHandleImpl extends MemChunkHandleImpl {

  private final List<BitMap> bitMapOfValue;
  private final List<TimeRange> deletionList;
  // start time and end time of the chunk according to bitMap
  private final long[] startEndTime;

  public MemAlignedChunkHandleImpl(
      IDeviceID deviceID,
      String measurement,
      long[] dataOfTimestamp,
      List<BitMap> bitMapOfValue,
      List<TimeRange> deletionList,
      long[] startEndTime) {
    super(deviceID, measurement, dataOfTimestamp);
    this.bitMapOfValue = bitMapOfValue;
    this.deletionList = deletionList;
    this.startEndTime = startEndTime;
  }

  @Override
  public long[] getPageStatisticsTime() {
    return startEndTime;
  }

  @Override
  public long[] getDataTime() throws IOException {
    List<Long> timeList = new ArrayList<>();
    int[] deletionCursor = {0};
    for (int i = 0; i < dataOfTimestamp.length; i++) {
      if (!bitMapOfValue.isEmpty()) {
        int arrayIndex = i / ARRAY_SIZE;
        int elementIndex = i % ARRAY_SIZE;
        if (bitMapOfValue.get(arrayIndex).isMarked(elementIndex)) {
          continue;
        }
      }
      if (!ModificationUtils.isPointDeleted(dataOfTimestamp[i], deletionList, deletionCursor)
          && (i == dataOfTimestamp.length - 1 || dataOfTimestamp[i] != dataOfTimestamp[i + 1])) {
        timeList.add(dataOfTimestamp[i]);
      }
    }
    hasRead = true;
    return timeList.stream().mapToLong(Long::longValue).toArray();
  }
}
