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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TimeRange extends Node {

  // [startTime, endTime)
  private long startTime;
  private long endTime;
  // if it is left close and right open interval
  private final boolean leftCRightO;

  public TimeRange(long startTime, long endTime, boolean leftCRightO) {
    super(null);
    this.startTime = startTime;
    this.endTime = endTime;
    this.leftCRightO = leftCRightO;
  }

  public TimeRange(NodeLocation location, long startTime, long endTime, boolean leftCRightO) {
    super(location);
    this.startTime = startTime;
    this.endTime = endTime;
    this.leftCRightO = leftCRightO;
  }

  @Override
  public List<? extends Node> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TimeRange timeRange = (TimeRange) o;
    return startTime == timeRange.startTime
        && endTime == timeRange.endTime
        && leftCRightO == timeRange.leftCRightO;
  }

  @Override
  public int hashCode() {
    return Objects.hash(startTime, endTime, leftCRightO);
  }

  @Override
  public String toString() {
    return "TimeRange{"
        + "startTime="
        + startTime
        + ", endTime="
        + endTime
        + ", leftCRightO="
        + leftCRightO
        + '}';
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public boolean isLeftCRightO() {
    return leftCRightO;
  }

  public org.apache.tsfile.read.common.TimeRange toTsFileTimeRange() {
    return new org.apache.tsfile.read.common.TimeRange(this.startTime, this.endTime);
  }
}
