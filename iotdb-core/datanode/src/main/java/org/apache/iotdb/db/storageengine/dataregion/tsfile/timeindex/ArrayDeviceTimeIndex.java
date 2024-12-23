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
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.exception.PartitionViolationException;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Deserializer;
import org.apache.tsfile.utils.FilePathUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ArrayDeviceTimeIndex implements ITimeIndex {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ArrayDeviceTimeIndex.class);

  private static final Logger logger = LoggerFactory.getLogger(ArrayDeviceTimeIndex.class);

  public static final int INIT_ARRAY_SIZE = 64;

  /** start times array. */
  protected long[] startTimes;

  /**
   * end times array. The values in this array are Long.MIN_VALUE if it's an unsealed sequence
   * tsfile
   */
  protected long[] endTimes;

  /** min start time */
  protected long minStartTime = Long.MAX_VALUE;

  /** max end time */
  protected long maxEndTime = Long.MIN_VALUE;

  /** device -> index of start times array and end times array */
  protected Map<IDeviceID, Integer> deviceToIndex;

  public ArrayDeviceTimeIndex() {
    this.deviceToIndex = new ConcurrentHashMap<>();
    this.startTimes = new long[INIT_ARRAY_SIZE];
    this.endTimes = new long[INIT_ARRAY_SIZE];
    initTimes(startTimes, Long.MAX_VALUE);
    initTimes(endTimes, Long.MIN_VALUE);
  }

  public ArrayDeviceTimeIndex(
      Map<IDeviceID, Integer> deviceToIndex, long[] startTimes, long[] endTimes) {
    this.startTimes = startTimes;
    this.endTimes = endTimes;
    this.deviceToIndex = deviceToIndex;
  }

  @Override
  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(getTimeIndexType(), outputStream);
    int deviceNum = deviceToIndex.size();

    ReadWriteIOUtils.write(deviceNum, outputStream);
    for (int i = 0; i < deviceNum; i++) {
      ReadWriteIOUtils.write(startTimes[i], outputStream);
      ReadWriteIOUtils.write(endTimes[i], outputStream);
    }

    for (Entry<IDeviceID, Integer> deviceIdIntegerEntry : deviceToIndex.entrySet()) {
      IDeviceID device = deviceIdIntegerEntry.getKey();
      int index = deviceIdIntegerEntry.getValue();
      device.serialize(outputStream);
      ReadWriteIOUtils.write(index, outputStream);
    }
  }

  @Override
  public ArrayDeviceTimeIndex deserialize(InputStream inputStream) throws IOException {
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
      IDeviceID deviceID = Deserializer.DEFAULT_DESERIALIZER.deserializeFrom(inputStream);
      int index = ReadWriteIOUtils.readInt(inputStream);
      deviceToIndex.put(deviceID, index);
    }
    return this;
  }

  @Override
  public ArrayDeviceTimeIndex deserialize(ByteBuffer buffer) {
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
      IDeviceID deviceID = Deserializer.DEFAULT_DESERIALIZER.deserializeFrom(buffer);
      int index = buffer.getInt();
      deviceToIndex.put(deviceID, index);
    }
    return this;
  }

  @Override
  public void close() {
    startTimes = Arrays.copyOfRange(startTimes, 0, deviceToIndex.size());
    endTimes = Arrays.copyOfRange(endTimes, 0, deviceToIndex.size());
  }

  public Set<IDeviceID> getDevices() {
    return deviceToIndex.keySet();
  }

  @Override
  public Set<IDeviceID> getDevices(String tsFilePath, TsFileResource tsFileResource) {
    return deviceToIndex.keySet();
  }

  public Map<IDeviceID, Integer> getDeviceToIndex() {
    return deviceToIndex;
  }

  public long[] getEndTimes() {
    return endTimes;
  }

  public long[] getStartTimes() {
    return startTimes;
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
      IDeviceID deviceID = Deserializer.DEFAULT_DESERIALIZER.deserializeFrom(inputStream);
      ReadWriteIOUtils.skip(inputStream, ReadWriteIOUtils.INT_LEN);
      devices.add(deviceID);
    }
    return devices;
  }

  @Override
  public boolean endTimeEmpty() {
    for (long endTime : endTimes) {
      if (endTime != Long.MIN_VALUE) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean stillLives(long ttlLowerBound) {
    if (ttlLowerBound == Long.MAX_VALUE) {
      return true;
    }
    for (long endTime : endTimes) {
      // the file cannot be deleted if any device still lives
      if (endTime >= ttlLowerBound) {
        return true;
      }
    }
    return false;
  }

  @Override
  public long calculateRamSize() {
    return INSTANCE_SIZE
        + RamUsageEstimator.sizeOfMap(
            deviceToIndex, RamUsageEstimator.shallowSizeOfInstance(Integer.class))
        + RamUsageEstimator.sizeOf(startTimes)
        + RamUsageEstimator.sizeOf(endTimes);
  }

  private int getDeviceIndex(IDeviceID deviceId) {
    int index;
    if (deviceToIndex.containsKey(deviceId)) {
      index = deviceToIndex.get(deviceId);
    } else {
      index = deviceToIndex.size();
      if (startTimes.length <= index) {
        startTimes = enLargeArray(startTimes, Long.MAX_VALUE);
        endTimes = enLargeArray(endTimes, Long.MIN_VALUE);
      }
      deviceToIndex.put(deviceId, index);
    }
    return index;
  }

  private void initTimes(long[] times, long defaultTime) {
    Arrays.fill(times, defaultTime);
  }

  private long[] enLargeArray(long[] array, long defaultValue) {
    long[] tmp = new long[(int) (array.length * 2)];
    initTimes(tmp, defaultValue);
    System.arraycopy(array, 0, tmp, 0, array.length);
    return tmp;
  }

  @Override
  public long getTimePartition(String tsFilePath) {
    try {
      if (deviceToIndex != null && !deviceToIndex.isEmpty()) {
        return TimePartitionUtils.getTimePartitionId(
            startTimes[deviceToIndex.values().iterator().next()]);
      }
      String[] filePathSplits = FilePathUtils.splitTsFilePath(tsFilePath);
      return Long.parseLong(filePathSplits[filePathSplits.length - 2]);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  @Override
  public long getTimePartitionWithCheck(String tsFilePath) throws PartitionViolationException {
    try {
      return getTimePartitionWithCheck();
    } catch (PartitionViolationException e) {
      throw new PartitionViolationException(tsFilePath);
    }
  }

  @Override
  public boolean isSpanMultiTimePartitions() {
    try {
      getTimePartitionWithCheck();
      return false;
    } catch (PartitionViolationException e) {
      return true;
    }
  }

  private long getTimePartitionWithCheck() throws PartitionViolationException {
    Long partitionId = null;

    for (final int index : deviceToIndex.values()) {
      final long startTimePartitionId = TimePartitionUtils.getTimePartitionId(startTimes[index]);
      final long endTimePartitionId = TimePartitionUtils.getTimePartitionId(endTimes[index]);

      if (startTimePartitionId != endTimePartitionId) {
        throw new PartitionViolationException();
      }

      if (partitionId == null) {
        partitionId = startTimePartitionId;
        continue;
      }

      if (partitionId != startTimePartitionId) {
        throw new PartitionViolationException();
      }
    }

    // Just in case
    if (partitionId == null) {
      throw new PartitionViolationException();
    }

    return partitionId;
  }

  @Override
  public void updateStartTime(IDeviceID deviceId, long time) {
    long startTime = getStartTime(deviceId);
    if (time < startTime) {
      int index = getDeviceIndex(deviceId);
      startTimes[index] = time;
    }
    minStartTime = Math.min(minStartTime, time);
  }

  @Override
  public void updateEndTime(IDeviceID deviceId, long time) {
    long endTime = getEndTime(deviceId);
    if (time > endTime) {
      int index = getDeviceIndex(deviceId);
      endTimes[index] = time;
    }
    maxEndTime = Math.max(maxEndTime, time);
  }

  @Override
  public void putStartTime(IDeviceID deviceId, long time) {
    int index = getDeviceIndex(deviceId);
    startTimes[index] = time;
    minStartTime = Math.min(minStartTime, time);
  }

  @Override
  public void putEndTime(IDeviceID deviceId, long time) {
    int index = getDeviceIndex(deviceId);
    endTimes[index] = time;
    maxEndTime = Math.max(maxEndTime, time);
  }

  @Override
  public long getStartTime(IDeviceID deviceId) {
    if (!deviceToIndex.containsKey(deviceId)) {
      return Long.MAX_VALUE;
    }
    return startTimes[deviceToIndex.get(deviceId)];
  }

  @Override
  public long getEndTime(IDeviceID deviceId) {
    if (!deviceToIndex.containsKey(deviceId)) {
      return Long.MIN_VALUE;
    }
    return endTimes[deviceToIndex.get(deviceId)];
  }

  @Override
  public boolean checkDeviceIdExist(IDeviceID deviceId) {
    return deviceToIndex.containsKey(deviceId);
  }

  @Override
  public long getMinStartTime() {
    return minStartTime;
  }

  @Override
  public long getMaxEndTime() {
    return maxEndTime;
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
  public boolean definitelyNotContains(IDeviceID device) {
    return !deviceToIndex.containsKey(device);
  }

  @Override
  public boolean isDeviceAlive(IDeviceID device, long ttl) {
    return endTimes[deviceToIndex.get(device)] >= CommonDateTimeUtils.currentTime() - ttl;
  }

  @Override
  public long[] getStartAndEndTime(IDeviceID deviceId) {
    Integer index = deviceToIndex.get(deviceId);
    if (index == null) {
      return null;
    } else {
      int i = index;
      return new long[] {startTimes[i], endTimes[i]};
    }
  }

  @Override
  public Pair<Long, Long> getPossibleStartTimeAndEndTime(
      PartialPath devicePattern, Set<IDeviceID> deviceMatchInfo) {
    boolean hasMatchedDevice = false;
    long startTime = Long.MAX_VALUE;
    long endTime = Long.MIN_VALUE;
    for (Entry<IDeviceID, Integer> entry : deviceToIndex.entrySet()) {
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
          if (devicePattern.matchFullPath(new PartialPath(entry.getKey()))) {
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
  public byte getTimeIndexType() {
    return ARRAY_DEVICE_TIME_INDEX_TYPE;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(" DeviceIndexMapSize = ").append(deviceToIndex.size());
    builder.append(" startTimeLength = ").append(startTimes.length);
    builder.append(" endTimeLength = ").append(endTimes.length);
    builder.append(" DeviceIndexMap = [");
    deviceToIndex.forEach(
        (key, value) ->
            builder.append(" device = ").append(key).append(", index = ").append(value));
    builder.append("]");
    builder.append(" StartTimes = ").append(Arrays.toString(startTimes));
    builder.append(" EndTimes = ").append(Arrays.toString(endTimes));
    return builder.toString();
  }
}
