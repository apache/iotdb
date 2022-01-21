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

import org.apache.iotdb.db.exception.PartitionViolationException;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Set;

public class V012FileTimeIndex implements ITimeIndex {

  /** devices */
  public V012FileTimeIndex() {}

  @Override
  public void serialize(OutputStream outputStream) throws IOException {
    throw new UnsupportedOperationException(
        "V012FileTimeIndex should be rewritten while upgrading and serialize() method should not be called any more.");
  }

  @Override
  public FileTimeIndex deserialize(InputStream inputStream) throws IOException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      ReadWriteIOUtils.readString(inputStream);
    }
    return new FileTimeIndex(
        ReadWriteIOUtils.readLong(inputStream), ReadWriteIOUtils.readLong(inputStream));
  }

  @Override
  public FileTimeIndex deserialize(ByteBuffer buffer) {
    int size = buffer.getInt();
    for (int i = 0; i < size; i++) {
      SerializeUtils.deserializeString(buffer);
    }
    return new FileTimeIndex(buffer.getLong(), buffer.getLong());
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException(
        "V012FileTimeIndex should be rewritten while upgrading and close() method should not be called any more.");
  }

  @Override
  public Set<String> getDevices(String tsFilePath) {
    throw new UnsupportedOperationException(
        "V012FileTimeIndex should be rewritten while upgrading and getDevices() method should not be called any more.");
  }

  @Override
  public boolean endTimeEmpty() {
    throw new UnsupportedOperationException(
        "V012FileTimeIndex should be rewritten while upgrading and endTimeEmpty() method should not be called any more.");
  }

  @Override
  public boolean stillLives(long timeLowerBound) {
    throw new UnsupportedOperationException(
        "V012FileTimeIndex should be rewritten while upgrading and stillLives() method should not be called any more.");
  }

  @Override
  public long calculateRamSize() {
    throw new UnsupportedOperationException(
        "V012FileTimeIndex should be rewritten while upgrading and calculateRamSize() method should not be called any more.");
  }

  @Override
  public long getTimePartition(String tsFilePath) {
    throw new UnsupportedOperationException(
        "V012FileTimeIndex should be rewritten while upgrading and getTimePartition() method should not be called any more.");
  }

  @Override
  public long getTimePartitionWithCheck(String tsFilePath) throws PartitionViolationException {
    throw new UnsupportedOperationException(
        "V012FileTimeIndex should be rewritten while upgrading and getTimePartitionWithCheck() method should not be called any more.");
  }

  @Override
  public boolean isSpanMultiTimePartitions() {
    throw new UnsupportedOperationException(
        "V012FileTimeIndex should be rewritten while upgrading and isSpanMultiTimePartitions() method should not be called any more.");
  }

  @Override
  public void updateStartTime(String deviceId, long time) {
    throw new UnsupportedOperationException(
        "V012FileTimeIndex should be rewritten while upgrading and updateStartTime() method should not be called any more.");
  }

  @Override
  public void updateEndTime(String deviceId, long time) {
    throw new UnsupportedOperationException(
        "V012FileTimeIndex should be rewritten while upgrading and updateEndTime() method should not be called any more.");
  }

  @Override
  public void putStartTime(String deviceId, long time) {
    throw new UnsupportedOperationException(
        "V012FileTimeIndex should be rewritten while upgrading and putStartTime() method should not be called any more.");
  }

  @Override
  public void putEndTime(String deviceId, long time) {
    throw new UnsupportedOperationException(
        "V012FileTimeIndex should be rewritten while upgrading and putEndTime() method should not be called any more.");
  }

  @Override
  public long getStartTime(String deviceId) {
    throw new UnsupportedOperationException(
        "V012FileTimeIndex should be rewritten while upgrading and getStartTime() method should not be called any more.");
  }

  @Override
  public long getEndTime(String deviceId) {
    throw new UnsupportedOperationException(
        "V012FileTimeIndex should be rewritten while upgrading and getEndTime() method should not be called any more.");
  }

  @Override
  public boolean checkDeviceIdExist(String deviceId) {
    throw new UnsupportedOperationException(
        "V012FileTimeIndex should be rewritten while upgrading and checkDeviceIdExist() method should not be called any more.");
  }

  @Override
  public long getMinStartTime() {
    throw new UnsupportedOperationException(
        "V012FileTimeIndex should be rewritten while upgrading and getMinStartTime() method should not be called any more.");
  }

  @Override
  public long getMaxEndTime() {
    throw new UnsupportedOperationException(
        "V012FileTimeIndex should be rewritten while upgrading and getMaxEndTime() method should not be called any more.");
  }

  @Override
  public int compareDegradePriority(ITimeIndex timeIndex) {
    throw new UnsupportedOperationException(
        "V012FileTimeIndex should be rewritten while upgrading.");
  }
}
