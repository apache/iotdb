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

package org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeDevicePathCache;
import org.apache.iotdb.tsfile.file.metadata.IDeviceID;
import org.apache.iotdb.tsfile.file.metadata.PlainDeviceID;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PlainDeviceTimeIndex extends ArrayDeviceTimeIndex implements ITimeIndex {

  private static final Logger logger = LoggerFactory.getLogger(PlainDeviceTimeIndex.class);

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ArrayDeviceTimeIndex.class);

  @Override
  public void serialize(OutputStream outputStream) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PlainDeviceTimeIndex deserialize(InputStream inputStream) throws IOException {
    int deviceNum = ReadWriteIOUtils.readInt(inputStream);

    startTimes = new long[deviceNum];
    endTimes = new long[deviceNum];

    for (int i = 0; i < deviceNum; i++) {
      startTimes[i] = ReadWriteIOUtils.readLong(inputStream);
      endTimes[i] = ReadWriteIOUtils.readLong(inputStream);
      minStartTime = Math.min(minStartTime, startTimes[i]);
      maxEndTime = Math.max(maxEndTime, endTimes[i]);
    }

    for (int i = 0; i < deviceNum; i++) {
      String path =
          DataNodeDevicePathCache.getInstance()
              .getDeviceId(ReadWriteIOUtils.readString(inputStream));
      int index = ReadWriteIOUtils.readInt(inputStream);
      deviceToIndex.put(IDeviceID.Factory.DEFAULT_FACTORY.create(path), index);
    }
    return this;
  }

  @Override
  public PlainDeviceTimeIndex deserialize(ByteBuffer buffer) {
    int deviceNum = buffer.getInt();
    startTimes = new long[deviceNum];
    endTimes = new long[deviceNum];

    for (int i = 0; i < deviceNum; i++) {
      startTimes[i] = buffer.getLong();
      endTimes[i] = buffer.getLong();
      minStartTime = Math.min(minStartTime, startTimes[i]);
      maxEndTime = Math.max(maxEndTime, endTimes[i]);
    }

    for (int i = 0; i < deviceNum; i++) {
      String path =
          DataNodeDevicePathCache.getInstance().getDeviceId(ReadWriteIOUtils.readString(buffer));
      int index = buffer.getInt();
      deviceToIndex.put(IDeviceID.Factory.DEFAULT_FACTORY.create(path), index);
    }
    return this;
  }

  /**
   * Deserialize TimeIndex and get devices only.
   *
   * @param inputStream inputStream
   * @return device name
   */
  public static Set<IDeviceID> getDevices(InputStream inputStream) throws IOException {
    int deviceNum = ReadWriteIOUtils.readInt(inputStream);
    ReadWriteIOUtils.skip(inputStream, 2L * deviceNum * ReadWriteIOUtils.LONG_LEN);
    Set<IDeviceID> devices = new HashSet<>();
    for (int i = 0; i < deviceNum; i++) {
      String path =
          DataNodeDevicePathCache.getInstance()
              .getDeviceId(ReadWriteIOUtils.readString(inputStream));
      ReadWriteIOUtils.skip(inputStream, ReadWriteIOUtils.INT_LEN);
      devices.add(IDeviceID.Factory.DEFAULT_FACTORY.create(path));
    }
    return devices;
  }

  @Override
  public Pair<Long, Long> getPossibleStartTimeAndEndTime(
      PartialPath devicePattern, Set<IDeviceID> deviceMatchInfo) {
    boolean hasMatchedDevice = false;
    long startTime = Long.MAX_VALUE;
    long endTime = Long.MIN_VALUE;
    for (Map.Entry<IDeviceID, Integer> entry : deviceToIndex.entrySet()) {
      try {
        if (deviceMatchInfo.contains(entry.getKey())) {
          hasMatchedDevice = true;
          if (startTimes[entry.getValue()] < startTime) {
            startTime = startTimes[entry.getValue()];
          }
          if (endTimes[entry.getValue()] > endTime) {
            endTime = endTimes[entry.getValue()];
          }
        } else {
          if (devicePattern.matchFullPath(
              DataNodeDevicePathCache.getInstance()
                  .getPartialPath(((PlainDeviceID) entry.getKey()).toStringID()))) {
            deviceMatchInfo.add(entry.getKey());
            hasMatchedDevice = true;
            if (startTimes[entry.getValue()] < startTime) {
              startTime = startTimes[entry.getValue()];
            }
            if (endTimes[entry.getValue()] > endTime) {
              endTime = endTimes[entry.getValue()];
            }
          }
        }
      } catch (IllegalPathException e) {
        // won't reach here
      }
    }

    return hasMatchedDevice ? new Pair<>(startTime, endTime) : null;
  }

  @Override
  public int compareDegradePriority(ITimeIndex timeIndex) {
    if (timeIndex instanceof ArrayDeviceTimeIndex) {
      return Long.compare(getMinStartTime(), timeIndex.getMinStartTime());
    } else if (timeIndex instanceof FileTimeIndex) {
      return -1;
    } else {
      logger.error("Wrong timeIndex type {}", timeIndex.getClass().getName());
      throw new RuntimeException("Wrong timeIndex type " + timeIndex.getClass().getName());
    }
  }

  @Override
  public long calculateRamSize() {
    return INSTANCE_SIZE
        + RamUsageEstimator.sizeOfMap(
            deviceToIndex, RamUsageEstimator.shallowSizeOfInstance(Integer.class))
        + RamUsageEstimator.sizeOf(startTimes)
        + RamUsageEstimator.sizeOf(endTimes);
  }

  @Override
  public byte getTimeIndexType() {
    return PLAIN_DEVICE_TIME_INDEX_TYPE;
  }
}
