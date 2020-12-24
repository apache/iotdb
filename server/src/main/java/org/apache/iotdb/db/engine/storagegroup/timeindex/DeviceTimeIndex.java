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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.rescon.CachedStringPool;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class DeviceTimeIndex {

  private static final Map<String, String> cachedDevicePool = CachedStringPool.getInstance()
      .getCachedPool();

  protected static final int INIT_ARRAY_SIZE = 64;

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

  public void init() {
    this.deviceToIndex = new ConcurrentHashMap<>();
    this.startTimes = new long[INIT_ARRAY_SIZE];
    this.endTimes = new long[INIT_ARRAY_SIZE];
    initTimes(startTimes, Long.MAX_VALUE);
    initTimes(endTimes, Long.MIN_VALUE);
  }

  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(deviceToIndex.size(), outputStream);
    for (Entry<String, Integer> entry : deviceToIndex.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(startTimes[entry.getValue()], outputStream);
      ReadWriteIOUtils.write(endTimes[entry.getValue()], outputStream);
    }
  }

  public static DeviceTimeIndex deserialize(InputStream inputStream) throws IOException {
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
      endTimesArray[i] =ReadWriteIOUtils.readLong(inputStream);
    }
    return new DeviceTimeIndex(deviceMap, startTimesArray, endTimesArray);
  }

  public long[] getStartTimes() {
    return startTimes;
  }

  public void setStartTimes(long[] startTimes) {
    this.startTimes = startTimes;
  }

  public long[] getEndTimes() {
    return endTimes;
  }

  public void setEndTimes(long[] endTimes) {
    this.endTimes = endTimes;
  }

  public Map<String, Integer> getDeviceToIndex() {
    return deviceToIndex;
  }

  public long calculateRamSize() {
    return RamUsageEstimator.sizeOf(deviceToIndex) + RamUsageEstimator.sizeOf(startTimes) +
        RamUsageEstimator.sizeOf(endTimes);
  }

  public long estimateRamIncrement(String deviceToBeChecked) {
    long ramIncrement = 0L;
    if (!containsDevice(deviceToBeChecked)) {
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

  public boolean containsDevice(String deviceId) {
    return deviceToIndex.containsKey(deviceId);
  }

  public void trimStartEndTimes() {
    startTimes = Arrays.copyOfRange(startTimes, 0, deviceToIndex.size());
    endTimes = Arrays.copyOfRange(endTimes, 0, deviceToIndex.size());
  }

  public int getDeviceIndex(String deviceId) {
    int index;
    if (containsDevice(deviceId)) {
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

  public long getTimePartition(TsFileResource resource) {
    try {
      if (deviceToIndex != null && !deviceToIndex.isEmpty()) {
        return StorageEngine.getTimePartition(startTimes[deviceToIndex.values().iterator().next()]);
      }
      String[] splits = FilePathUtils.splitTsFilePath(resource);
      return Long.parseLong(splits[splits.length - 2]);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  public void putStartTime(String deviceId, long startTime) {
    startTimes[getDeviceIndex(deviceId)] = startTime;
  }

  public void putEndTime(String deviceId, long endTime) {
    endTimes[getDeviceIndex(deviceId)] = endTime;
  }

  public long getStartTime(String deviceId) {
    if (!deviceToIndex.containsKey(deviceId)) {
      return Long.MAX_VALUE;
    }
    return startTimes[deviceToIndex.get(deviceId)];
  }

  public long getEndTime(String deviceId) {
    if (!deviceToIndex.containsKey(deviceId)) {
      return Long.MIN_VALUE;
    }
    return endTimes[deviceToIndex.get(deviceId)];
  }
}
