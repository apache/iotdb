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

import io.netty.util.internal.ConcurrentSet;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.PartitionViolationException;
import org.apache.iotdb.db.rescon.CachedStringPool;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class FileTimeIndex implements ITimeIndex {

  protected static final Map<String, String> cachedDevicePool = CachedStringPool.getInstance()
      .getCachedPool();

  /**
   * start time
   */
  protected long startTime;

  /**
   * end times. The value is Long.MIN_VALUE if it's an unsealed sequence tsfile
   */
  protected long endTime;

  /**
   * devices
   */
  protected Set<String> devices;

  public FileTimeIndex() {
    this.devices = new ConcurrentSet<>();
    this.startTime = Long.MAX_VALUE;
    this.endTime = Long.MIN_VALUE;
  }

  public FileTimeIndex(Set<String> devices, long startTime, long endTime) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.devices = devices;
  }

  @Override
  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(devices.size(), outputStream);
    for (String device : devices) {
      ReadWriteIOUtils.write(device, outputStream);
    }
    ReadWriteIOUtils.write(startTime, outputStream);
    ReadWriteIOUtils.write(endTime, outputStream);
  }

  @Override
  public FileTimeIndex deserialize(InputStream inputStream) throws IOException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    Set<String> deviceSet = new HashSet<>();
    for (int i = 0; i < size; i++) {
      String path = ReadWriteIOUtils.readString(inputStream);
      // To reduce the String number in memory,
      // use the deviceId from memory instead of the deviceId read from disk
      String cachedPath = cachedDevicePool.computeIfAbsent(path, k -> k);
      deviceSet.add(cachedPath);
    }
    return new FileTimeIndex(deviceSet, ReadWriteIOUtils.readLong(inputStream),
        ReadWriteIOUtils.readLong(inputStream));
  }

  @Override
  public FileTimeIndex deserialize(ByteBuffer buffer) {
    int size = buffer.getInt();
    Set<String> deviceSet = new HashSet<>(size);

    for (int i = 0; i < size; i++) {
      String path = SerializeUtils.deserializeString(buffer);
      // To reduce the String number in memory,
      // use the deviceId from memory instead of the deviceId read from disk
      String cachedPath = cachedDevicePool.computeIfAbsent(path, k -> k);
      deviceSet.add(cachedPath);
    }
    return new FileTimeIndex(deviceSet, buffer.getLong(), buffer.getLong());
  }

  @Override
  public void close() {
    // allowed to be null
  }

  @Override
  public Set<String> getDevices() {
    return devices;
  }

  @Override
  public boolean endTimeEmpty() {
    return endTime == Long.MIN_VALUE;
  }

  @Override
  public boolean stillLives(long timeLowerBound) {
    if (timeLowerBound == Long.MAX_VALUE) {
      return true;
    }
    // the file cannot be deleted if any device still lives
    return endTime >= timeLowerBound;
  }

  @Override
  public long calculateRamSize() {
    return RamUsageEstimator.sizeOf(devices) + RamUsageEstimator.sizeOf(startTime) +
        RamUsageEstimator.sizeOf(endTime);
  }

  @Override
  public long estimateRamIncrement(String deviceToBeChecked) {
    return devices.contains(deviceToBeChecked) ? 0L : RamUsageEstimator.sizeOf(deviceToBeChecked);
  }

  @Override
  public long getTimePartition(String tsfilePath) {
    try {
      if (devices != null && !devices.isEmpty()) {
        return StorageEngine.getTimePartition(startTime);
      }
      String[] filePathSplits = FilePathUtils.splitTsFilePath(tsfilePath);
      return Long.parseLong(filePathSplits[filePathSplits.length - 2]);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  @Override
  public long getTimePartitionWithCheck(String tsfilePath) throws PartitionViolationException {
    long startPartitionId = StorageEngine.getTimePartition(startTime);
    long endPartitionId = StorageEngine.getTimePartition(endTime);
    if (startPartitionId != endPartitionId) {
      throw new PartitionViolationException(tsfilePath);
    }
    return startPartitionId;
  }

  @Override
  public void updateStartTime(String deviceId, long time) {
    devices.add(deviceId);
    if (this.startTime > time) {
      this.startTime = time;
    }
  }

  @Override
  public void updateEndTime(String deviceId, long time) {
    devices.add(deviceId);
    if (this.endTime < time) {
      this.endTime = time;
    }
  }

  @Override
  public long getStartTime(String deviceId) {
    return startTime;
  }

  @Override
  public long getEndTime(String deviceId) {
    return endTime;
  }
}
