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

package org.apache.iotdb.db.engine.modification;

import org.apache.iotdb.db.metadata.PartialPath;

import java.util.Objects;

/** Deletion is a delete operation on a timeseries. */
public class Deletion extends Modification {

  /** data within the interval [startTime, endTime] are to be deleted. */
  private long startTime;

  private long endTime;

  /**
   * constructor of Deletion, the start time is set to Long.MIN_VALUE
   *
   * @param endTime end time of delete interval
   * @param path time series path
   */
  public Deletion(PartialPath path, long fileOffset, long endTime) {
    super(Type.DELETION, path, fileOffset);
    this.startTime = Long.MIN_VALUE;
    this.endTime = endTime;
  }

  /**
   * constructor of Deletion
   *
   * @param startTime start time of delete interval
   * @param endTime end time of delete interval
   * @param path time series path
   */
  public Deletion(PartialPath path, long fileOffset, long startTime, long endTime) {
    super(Type.DELETION, path, fileOffset);
    this.startTime = startTime;
    this.endTime = endTime;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long timestamp) {
    this.startTime = timestamp;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long timestamp) {
    this.endTime = timestamp;
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
    return super.equals(obj) && del.startTime == this.startTime && del.endTime == this.endTime;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), startTime, endTime);
  }

  @Override
  public String toString() {
    return "Deletion{"
        + "startTime="
        + startTime
        + ", endTime="
        + endTime
        + ", type="
        + type
        + ", path="
        + path
        + ", fileOffset="
        + fileOffset
        + '}';
  }
}
