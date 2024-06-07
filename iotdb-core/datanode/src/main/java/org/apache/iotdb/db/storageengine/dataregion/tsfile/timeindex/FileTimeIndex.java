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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.exception.PartitionViolationException;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
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
import java.nio.file.NoSuchFileException;
import java.util.Collections;
import java.util.Set;

public class FileTimeIndex implements ITimeIndex {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(FileTimeIndex.class);

  private static final Logger logger = LoggerFactory.getLogger(FileTimeIndex.class);

  /** start time */
  protected long startTime;

  /** end times. The value is Long.MIN_VALUE if it's an unsealed sequence tsfile */
  protected long endTime;

  public FileTimeIndex() {
    this.startTime = Long.MAX_VALUE;
    this.endTime = Long.MIN_VALUE;
  }

  public FileTimeIndex(long startTime, long endTime) {
    this.startTime = startTime;
    this.endTime = endTime;
  }

  @Override
  public void serialize(OutputStream outputStream) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileTimeIndex deserialize(InputStream inputStream) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileTimeIndex deserialize(ByteBuffer buffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    // allowed to be null
  }

  @Override
  public Set<IDeviceID> getDevices(String tsFilePath, TsFileResource tsFileResource) {
    tsFileResource.readLock();
    try (InputStream inputStream =
        FSFactoryProducer.getFSFactory()
            .getBufferedInputStream(tsFilePath + TsFileResource.RESOURCE_SUFFIX)) {
      // The first byte is VERSION_NUMBER, second byte is timeIndexType.
      byte[] bytes = ReadWriteIOUtils.readBytes(inputStream, 2);
      if (bytes[1] == ARRAY_DEVICE_TIME_INDEX_TYPE) {
        return ArrayDeviceTimeIndex.getDevices(inputStream);
      } else {
        return PlainDeviceTimeIndex.getDevices(inputStream);
      }
    } catch (NoSuchFileException e) {
      // deleted by ttl
      if (tsFileResource.isDeleted()) {
        return Collections.emptySet();
      } else {
        logger.error(
            "Can't read file {} from disk ", tsFilePath + TsFileResource.RESOURCE_SUFFIX, e);
        throw new RuntimeException(
            "Can't read file " + tsFilePath + TsFileResource.RESOURCE_SUFFIX + " from disk");
      }
    } catch (Exception e) {
      logger.error(
          "Failed to get devices from tsfile: {}", tsFilePath + TsFileResource.RESOURCE_SUFFIX, e);
      throw new RuntimeException(
          "Failed to get devices from tsfile: " + tsFilePath + TsFileResource.RESOURCE_SUFFIX);
    } finally {
      tsFileResource.readUnlock();
    }
  }

  @Override
  public boolean endTimeEmpty() {
    return endTime == Long.MIN_VALUE;
  }

  @Override
  public boolean stillLives(long ttlLowerBound) {
    if (ttlLowerBound == Long.MAX_VALUE) {
      return true;
    }
    // the file cannot be deleted if any device still lives
    return endTime >= ttlLowerBound;
  }

  @Override
  public long calculateRamSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public long getTimePartition(String tsFilePath) {
    try {
      String[] filePathSplits = FilePathUtils.splitTsFilePath(tsFilePath);
      return Long.parseLong(filePathSplits[filePathSplits.length - 2]);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  @Override
  public long getTimePartitionWithCheck(String tsFilePath) throws PartitionViolationException {
    final long startPartitionId = TimePartitionUtils.getTimePartitionId(startTime);
    final long endPartitionId = TimePartitionUtils.getTimePartitionId(endTime);

    if (startPartitionId == endPartitionId) {
      return startPartitionId;
    }

    throw new PartitionViolationException(tsFilePath);
  }

  @Override
  public boolean isSpanMultiTimePartitions() {
    return TimePartitionUtils.getTimePartitionId(startTime)
        != TimePartitionUtils.getTimePartitionId(endTime);
  }

  @Override
  public void updateStartTime(IDeviceID deviceId, long time) {
    if (this.startTime > time) {
      this.startTime = time;
    }
  }

  @Override
  public void updateEndTime(IDeviceID deviceId, long time) {
    if (this.endTime < time) {
      this.endTime = time;
    }
  }

  @Override
  public void putStartTime(IDeviceID deviceId, long time) {
    this.startTime = time;
  }

  @Override
  public void putEndTime(IDeviceID deviceId, long time) {
    this.endTime = time;
  }

  @Override
  public long getStartTime(IDeviceID deviceId) {
    return startTime;
  }

  @Override
  public long getMinStartTime() {
    return startTime;
  }

  @Override
  public long getEndTime(IDeviceID deviceId) {
    return endTime;
  }

  @Override
  public long getMaxEndTime() {
    return endTime;
  }

  @Override
  public boolean checkDeviceIdExist(IDeviceID deviceId) {
    return true;
  }

  @Override
  public int compareDegradePriority(ITimeIndex timeIndex) {
    if (timeIndex instanceof ArrayDeviceTimeIndex) {
      return 1;
    } else if (timeIndex instanceof FileTimeIndex) {
      return Long.compare(startTime, timeIndex.getMinStartTime());
    } else {
      logger.error("Wrong timeIndex type {}", timeIndex.getClass().getName());
      throw new RuntimeException("Wrong timeIndex type " + timeIndex.getClass().getName());
    }
  }

  @Override
  public boolean definitelyNotContains(IDeviceID device) {
    return false;
  }

  @Override
  public boolean isDeviceAlive(IDeviceID device, long ttl) {
    return endTime >= CommonDateTimeUtils.currentTime() - ttl;
  }

  @Override
  public long[] getStartAndEndTime(IDeviceID deviceId) {
    return new long[] {startTime, endTime};
  }

  @Override
  public Pair<Long, Long> getPossibleStartTimeAndEndTime(
      PartialPath devicePattern, Set<IDeviceID> deviceMatchInfo) {
    return new Pair<>(startTime, endTime);
  }

  @Override
  public byte getTimeIndexType() {
    return FILE_TIME_INDEX_TYPE;
  }

  @Override
  public String toString() {
    return " StartTime = " + startTime + " EndTime = " + endTime;
  }
}
