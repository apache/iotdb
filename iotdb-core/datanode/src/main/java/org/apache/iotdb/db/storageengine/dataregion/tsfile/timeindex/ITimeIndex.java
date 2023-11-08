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
import org.apache.iotdb.db.exception.PartitionViolationException;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Set;

public interface ITimeIndex {

  byte DEVICE_TIME_INDEX_TYPE = 1;
  byte FILE_TIME_INDEX_TYPE = 2;

  int SPANS_MULTI_TIME_PARTITIONS_FLAG_ID = -1;

  /**
   * serialize to outputStream
   *
   * @param outputStream outputStream
   */
  void serialize(OutputStream outputStream) throws IOException;

  /**
   * deserialize from inputStream
   *
   * @param inputStream inputStream
   * @return TimeIndex
   */
  ITimeIndex deserialize(InputStream inputStream) throws IOException;

  /**
   * deserialize from byte buffer
   *
   * @param buffer byte buffer
   * @return TimeIndex
   */
  ITimeIndex deserialize(ByteBuffer buffer);

  /** do something when TsFileResource is closing (may be empty method) */
  void close();

  /**
   * get devices in TimeIndex
   *
   * @return device names
   */
  Set<String> getDevices(String tsFilePath, TsFileResource tsFileResource);

  /** @return whether end time is empty (Long.MIN_VALUE) */
  boolean endTimeEmpty();

  /**
   * @param ttlLowerBound time lower bound
   * @return whether any of the device lives over the given time bound
   */
  boolean stillLives(long ttlLowerBound);

  /** @return Calculate file index ram size */
  long calculateRamSize();

  /**
   * get time partition
   *
   * @param tsFilePath tsFile absolute path
   * @return partition
   */
  long getTimePartition(String tsFilePath);

  /**
   * get time partition with check. If data of tsFile spans partitions, an exception will be thrown
   *
   * @param tsFilePath tsFile path
   * @return partition
   * @throws PartitionViolationException data of tsFile spans partitions
   */
  long getTimePartitionWithCheck(String tsFilePath) throws PartitionViolationException;

  /**
   * Check whether the tsFile spans multiple time partitions.
   *
   * @return true if the tsFile spans multiple time partitions, otherwise false.
   */
  boolean isSpanMultiTimePartitions();

  /**
   * update start time
   *
   * @param deviceId device name
   * @param time start time
   */
  void updateStartTime(String deviceId, long time);

  /**
   * update end time
   *
   * @param deviceId device name
   * @param time end time
   */
  void updateEndTime(String deviceId, long time);

  /**
   * put start time
   *
   * @param deviceId device name
   * @param time start time
   */
  void putStartTime(String deviceId, long time);

  /**
   * put end time
   *
   * @param deviceId device name
   * @param time end time
   */
  void putEndTime(String deviceId, long time);

  /**
   * get start time of device
   *
   * @param deviceId device name
   * @return start time
   */
  long getStartTime(String deviceId);

  /**
   * get end time of device
   *
   * @param deviceId device name
   * @return end time
   */
  long getEndTime(String deviceId);

  /**
   * check whether deviceId exists in TsFile
   *
   * @param deviceId device name
   * @return true if the deviceId may exist in TsFile, otherwise false.
   */
  boolean checkDeviceIdExist(String deviceId);

  /**
   * get min start time of device
   *
   * @return min start time
   */
  long getMinStartTime();

  /**
   * get max end time of device
   *
   * @return max end time
   */
  long getMaxEndTime();

  /**
   * compare the priority of two ITimeIndex
   *
   * @param timeIndex another timeIndex
   * @return value is less than 0 if the priority of this timeIndex is higher than the argument,
   *     value is equal to 0 if the priority of this timeIndex is equal to the argument, value is
   *     larger than 0 if the priority of this timeIndex is less than the argument
   */
  int compareDegradePriority(ITimeIndex timeIndex);

  /**
   * Whether this TsFile definitely not contains this device, if ture, it must not contain this
   * device, if false, it may or may not contain this device Notice: using method be CAREFULLY and
   * you really understand the meaning!!!!!
   */
  boolean definitelyNotContains(String device);

  /**
   * @return null if the deviceId doesn't exist, otherwise index 0 is startTime, index 1 is endTime
   */
  long[] getStartAndEndTime(String deviceId);

  Pair<Long, Long> getPossibleStartTimeAndEndTime(
      PartialPath devicePattern, Set<String> deviceMatchInfo);

  /**
   * Get TimeIndex Type
   *
   * @return V012FileTimeIndex = 0, deviceTimeIndex = 1, fileTimeIndex = 2
   */
  byte getTimeIndexType();

  static ITimeIndex createTimeIndex(InputStream inputStream) throws IOException {
    byte timeIndexType = ReadWriteIOUtils.readByte(inputStream);
    if (timeIndexType == -1) {
      throw new IOException("The end of stream has been reached");
    }
    return TimeIndexLevel.valueOf(timeIndexType).getTimeIndex().deserialize(inputStream);
  }
}
