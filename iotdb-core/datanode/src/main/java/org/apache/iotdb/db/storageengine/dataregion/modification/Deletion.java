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

package org.apache.iotdb.db.storageengine.dataregion.modification;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;

import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

/** Deletion is a delete operation on a timeseries. */
public class Deletion extends Modification implements Cloneable {

  /** data within the interval [startTime, endTime] are to be deleted. */
  private TimeRange timeRange;

  /**
   * constructor of Deletion, the start time is set to Long.MIN_VALUE
   *
   * @param endTime end time of delete interval
   * @param path time series path
   */
  public Deletion(MeasurementPath path, long fileOffset, long endTime) {
    super(Type.DELETION, path, fileOffset);
    this.timeRange = new TimeRange(Long.MIN_VALUE, endTime);
    this.timeRange.setLeftClose(false);
    if (endTime == Long.MAX_VALUE) {
      this.timeRange.setRightClose(false);
    }
  }

  /**
   * constructor of Deletion.
   *
   * @param startTime start time of delete interval
   * @param endTime end time of delete interval
   * @param path time series path
   */
  public Deletion(MeasurementPath path, long fileOffset, long startTime, long endTime) {
    super(Type.DELETION, path, fileOffset);
    this.timeRange = new TimeRange(startTime, endTime);
    if (startTime == Long.MIN_VALUE) {
      this.timeRange.setLeftClose(false);
    }
    if (endTime == Long.MAX_VALUE) {
      this.timeRange.setRightClose(false);
    }
  }

  public long getStartTime() {
    return this.timeRange.getMin();
  }

  public void setStartTime(long timestamp) {
    this.timeRange.setMin(timestamp);
  }

  public long getEndTime() {
    return this.timeRange.getMax();
  }

  public void setEndTime(long timestamp) {
    this.timeRange.setMax(timestamp);
  }

  public TimeRange getTimeRange() {
    return timeRange;
  }

  public boolean intersects(Deletion deletion) {
    if (super.equals(deletion)) {
      return this.timeRange.intersects(deletion.getTimeRange());
    } else {
      return false;
    }
  }

  public void merge(Deletion deletion) {
    this.timeRange.merge(deletion.getTimeRange());
  }

  public long serializeWithoutFileOffset(DataOutputStream stream) throws IOException {
    long serializeSize = 0;
    stream.writeLong(getStartTime());
    serializeSize += Long.BYTES;
    stream.writeLong(getEndTime());
    serializeSize += Long.BYTES;
    serializeSize += ReadWriteIOUtils.write(getPathString(), stream);
    return serializeSize;
  }

  public static Deletion deserializeWithoutFileOffset(DataInputStream stream)
      throws IOException, IllegalPathException {
    long startTime = stream.readLong();
    long endTime = stream.readLong();
    return new Deletion(
        new MeasurementPath(ReadWriteIOUtils.readString(stream)), 0, startTime, endTime);
  }

  public long getSerializedSize() {
    return Long.BYTES * 2 + Integer.BYTES + (long) getPathString().length() * Character.BYTES;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Deletion)) {
      return false;
    }
    Deletion del = (Deletion) obj;
    return super.equals(obj) && del.timeRange.equals(this.timeRange);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), this.timeRange);
  }

  @Override
  public String toString() {
    return "Deletion{"
        + "startTime="
        + getStartTime()
        + ", endTime="
        + getEndTime()
        + ", type="
        + type
        + ", path="
        + path
        + ", fileOffset="
        + fileOffset
        + '}';
  }

  @Override
  public Deletion clone() {
    return new Deletion(getPath(), getFileOffset(), getStartTime(), getEndTime());
  }
}
