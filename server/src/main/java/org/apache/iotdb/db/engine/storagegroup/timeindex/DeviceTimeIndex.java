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

package org.apache.iotdb.db.engine.storagegroup.timeindex;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.PartitionViolationException;
import org.apache.iotdb.db.rescon.CachedStringPool;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class DeviceTimeIndex implements ITimeIndex {

  protected static final int INIT_ARRAY_SIZE = 64;

  public static final Map<String, String> cachedDevicePool = CachedStringPool.getInstance()
      .getCachedPool();

  /**
   * start times array.
   */
  protected long[] startTimes;

  /**
   * end times array. The values in this array are Long.MIN_VALUE if it's an unsealed sequence
   * tsfile
   */
  protected long[] endTimes;

  /**
   * device -> index of start times array and end times array
   */
  protected Map<String, Integer> deviceToIndex;

  public DeviceTimeIndex() {
    init();
  }

  public DeviceTimeIndex(Map<String, Integer> deviceToIndex, long[] startTimes, long[] endTimes) {
    this.startTimes = startTimes;
    this.endTimes = endTimes;
    this.deviceToIndex = deviceToIndex;
  }

  @Override
  public void init() {
    this.deviceToIndex = new ConcurrentHashMap<>();
    this.startTimes = new long[INIT_ARRAY_SIZE];
    this.endTimes = new long[INIT_ARRAY_SIZE];
    initTimes(startTimes, Long.MAX_VALUE);
    initTimes(endTimes, Long.MIN_VALUE);
  }

  @Override
  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(deviceToIndex.size(), outputStream);
    for (Entry<String, Integer> entry : deviceToIndex.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(startTimes[entry.getValue()], outputStream);
      ReadWriteIOUtils.write(endTimes[entry.getValue()], outputStream);
    }
  }

  @Override
  public DeviceTimeIndex deserialize(InputStream inputStream) throws IOException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    Map<String, Integer> deviceMap = new HashMap<>();
    long[] startTimesArray = new long[size];
    long[] endTimesArray = new long[size];
    for (int i = 0; i < size; i++) {
      String path = ReadWriteIOUtils.readString(inputStream);
      // To reduce the String number in memory,
      // use the deviceId from memory instead of the deviceId read from disk
      String cachedPath = cachedDevicePool.computeIfAbsent(path, k -> k);
      deviceMap.put(cachedPath, i);

      startTimesArray[i] = ReadWriteIOUtils.readLong(inputStream);
      endTimesArray[i] = ReadWriteIOUtils.readLong(inputStream);
    }
    return new DeviceTimeIndex(deviceMap, startTimesArray, endTimesArray);
  }

  @Override
  public DeviceTimeIndex deserialize(ByteBuffer buffer) {
    int size = buffer.getInt();
    long[] startTimesArray = new long[size];
    long[] endTimesArray = new long[size];
    Map<String, Integer> deviceMap = new ConcurrentHashMap<>(size);

    for (int i = 0; i < size; i++) {
      String path = SerializeUtils.deserializeString(buffer);
      // To reduce the String number in memory,
      // use the deviceId from memory instead of the deviceId read from disk
      String cachedPath = cachedDevicePool.computeIfAbsent(path, k -> k);
      deviceMap.put(cachedPath, i);

      startTimesArray[i] = buffer.getLong();
      endTimesArray[i] = buffer.getLong();
    }

    return new DeviceTimeIndex(deviceMap, startTimesArray, endTimesArray);
  }

  @Override
  public void close() {
    trimStartEndTimes();
  }

  @Override
  public Set<String> getDevices() {
    return deviceToIndex.keySet();
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
  public boolean stillLives(long timeLowerBound) {
    if (timeLowerBound == Long.MAX_VALUE) {
      return true;
    }
    for (long endTime : endTimes) {
      // the file cannot be deleted if any device still lives
      if (endTime >= timeLowerBound) {
        return true;
      }
    }
    return false;
  }

  @Override
  public long calculateRamSize() {
    return RamUsageEstimator.sizeOf(deviceToIndex) + RamUsageEstimator.sizeOf(startTimes) +
        RamUsageEstimator.sizeOf(endTimes);
  }

  @Override
  public long estimateRamIncrement(String deviceToBeChecked) {
    long ramIncrement = 0L;
    if (!deviceToIndex.containsKey(deviceToBeChecked)) {
      // 80 is the Map.Entry header ram size
      if (deviceToIndex.isEmpty()) {
        ramIncrement += 80;
      }
      // Map.Entry ram size
      ramIncrement += RamUsageEstimator.sizeOf(deviceToBeChecked) + 16;
      // if needs to extend the startTimes and endTimes arrays
      if (deviceToIndex.size() >= startTimes.length) {
        ramIncrement += startTimes.length * Long.BYTES;
      }
    }
    return ramIncrement;
  }

  private void trimStartEndTimes() {
    startTimes = Arrays.copyOfRange(startTimes, 0, deviceToIndex.size());
    endTimes = Arrays.copyOfRange(endTimes, 0, deviceToIndex.size());
  }

  private int getDeviceIndex(String deviceId) {
    int index;
    if (deviceToIndex.containsKey(deviceId)) {
      index = deviceToIndex.get(deviceId);
    } else {
      index = deviceToIndex.size();
      deviceToIndex.put(deviceId, index);
      if (startTimes.length <= index) {
        startTimes = enLargeArray(startTimes, Long.MAX_VALUE);
        endTimes = enLargeArray(endTimes, Long.MIN_VALUE);
      }
    }
    return index;
  }

  private void initTimes(long[] times, long defaultTime) {
    Arrays.fill(times, defaultTime);
  }

  private long[] enLargeArray(long[] array, long defaultValue) {
    long[] tmp = new long[(int) (array.length * 1.5)];
    initTimes(tmp, defaultValue);
    System.arraycopy(array, 0, tmp, 0, array.length);
    return tmp;
  }

  @Override
  public long getTimePartition(String file) {
    try {
      if (deviceToIndex != null && !deviceToIndex.isEmpty()) {
        return StorageEngine.getTimePartition(startTimes[deviceToIndex.values().iterator().next()]);
      }
      String[] filePathSplits = FilePathUtils.splitTsFilePath(file);
      return Long.parseLong(filePathSplits[filePathSplits.length - 2]);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  @Override
  public long getTimePartitionWithCheck(String file) throws PartitionViolationException {
    long partitionId = -1;
    for (Long startTime : startTimes) {
      long p = StorageEngine.getTimePartition(startTime);
      if (partitionId == -1) {
        partitionId = p;
      } else {
        if (partitionId != p) {
          throw new PartitionViolationException(file);
        }
      }
    }
    for (Long endTime : endTimes) {
      long p = StorageEngine.getTimePartition(endTime);
      if (partitionId == -1) {
        partitionId = p;
      } else {
        if (partitionId != p) {
          throw new PartitionViolationException(file);
        }
      }
    }
    if (partitionId == -1) {
      throw new PartitionViolationException(file);
    }
    return partitionId;
  }

  @Override
  public void updateStartTime(String deviceId, long time) {
    long startTime = getStartTime(deviceId);
    if (time < startTime) {
      startTimes[getDeviceIndex(deviceId)] = time;
    }
  }

  @Override
  public void updateEndTime(String deviceId, long time) {
    long endTime = getEndTime(deviceId);
    if (time > endTime) {
      endTimes[getDeviceIndex(deviceId)] = time;
    }
  }

  @Override
  public long getStartTime(String deviceId) {
    if (!deviceToIndex.containsKey(deviceId)) {
      return Long.MAX_VALUE;
    }
    return startTimes[deviceToIndex.get(deviceId)];
  }

  @Override
  public long getEndTime(String deviceId) {
    if (!deviceToIndex.containsKey(deviceId)) {
      return Long.MIN_VALUE;
    }
    return endTimes[deviceToIndex.get(deviceId)];
  }
}
