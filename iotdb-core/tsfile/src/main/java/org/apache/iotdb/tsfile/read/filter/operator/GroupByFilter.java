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

package org.apache.iotdb.tsfile.read.filter.operator;

import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.basic.OperatorType;
import org.apache.iotdb.tsfile.read.filter.basic.TimeFilter;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class GroupByFilter extends TimeFilter {

  // [startTime, endTime]
  protected long startTime;
  protected long endTime;

  protected long interval;
  protected long slidingStep;

  public GroupByFilter(long startTime, long endTime, long interval, long slidingStep) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.interval = interval;
    this.slidingStep = slidingStep;
  }

  protected GroupByFilter(long startTime, long endTime) {
    this.startTime = startTime;
    this.endTime = endTime;
  }

  public GroupByFilter(ByteBuffer buffer) {
    this.startTime = ReadWriteIOUtils.readLong(buffer);
    this.endTime = ReadWriteIOUtils.readLong(buffer);
    this.interval = ReadWriteIOUtils.readLong(buffer);
    this.slidingStep = ReadWriteIOUtils.readLong(buffer);
  }

  @Override
  public boolean timeSatisfy(long time) {
    if (time < startTime || time >= endTime) {
      return false;
    } else {
      return (time - startTime) % slidingStep < interval;
    }
  }

  @Override
  public boolean satisfyStartEndTime(long startTime, long endTime) {
    if (endTime < this.startTime || startTime >= this.endTime) {
      return false;
    } else if (startTime <= this.startTime) {
      return true;
    } else {
      long minTime = startTime - this.startTime;
      long count = minTime / slidingStep;
      if (minTime <= interval + count * slidingStep) {
        return true;
      } else {
        if (this.endTime <= (count + 1) * slidingStep + this.startTime) {
          return false;
        } else {
          return endTime >= (count + 1) * slidingStep + this.startTime;
        }
      }
    }
  }

  @Override
  public boolean containStartEndTime(long startTime, long endTime) {
    if (startTime >= this.startTime && endTime <= this.endTime) {
      long minTime = startTime - this.startTime;
      long maxTime = endTime - this.startTime;
      long count = minTime / slidingStep;
      return minTime <= interval + count * slidingStep && maxTime <= interval + count * slidingStep;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format(
        "GroupByFilter{[%d, %d], %d, %d}", startTime, endTime, interval, slidingStep);
  }

  @Override
  public List<TimeRange> getTimeRanges() {
    return startTime >= endTime
        ? Collections.emptyList()
        : Collections.singletonList(new TimeRange(startTime, endTime - 1));
  }

  @Override
  public Filter reverse() {
    throw new UnsupportedOperationException();
  }

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.GROUP_BY_TIME;
  }

  @Override
  public void serialize(DataOutputStream outputStream) throws IOException {
    super.serialize(outputStream);
    ReadWriteIOUtils.write(startTime, outputStream);
    ReadWriteIOUtils.write(endTime, outputStream);
    ReadWriteIOUtils.write(interval, outputStream);
    ReadWriteIOUtils.write(slidingStep, outputStream);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GroupByFilter that = (GroupByFilter) o;
    return interval == that.interval
        && slidingStep == that.slidingStep
        && startTime == that.startTime
        && endTime == that.endTime;
  }

  @Override
  public int hashCode() {
    return Objects.hash(interval, slidingStep, startTime, endTime);
  }
}
