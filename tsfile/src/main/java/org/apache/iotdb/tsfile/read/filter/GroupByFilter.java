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
package org.apache.iotdb.tsfile.read.filter;

import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterSerializeId;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class GroupByFilter implements Filter, Serializable {

  private static final long serialVersionUID = -1211805021419281440L;
  protected long interval;
  protected long slidingStep;
  protected long startTime;
  protected long endTime;

  public GroupByFilter(long interval, long slidingStep, long startTime, long endTime) {
    this.interval = interval;
    this.slidingStep = slidingStep;
    this.startTime = startTime;
    this.endTime = endTime;
  }

  public GroupByFilter() {}

  @Override
  public boolean satisfy(Statistics statistics) {
    return satisfyStartEndTime(statistics.getStartTime(), statistics.getEndTime());
  }

  @Override
  public boolean satisfy(long time, Object value) {
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
  public Filter copy() {
    return new GroupByFilter(interval, slidingStep, startTime, endTime);
  }

  @Override
  public String toString() {
    return "GroupByFilter{}";
  }

  @Override
  public void serialize(DataOutputStream outputStream) {
    try {
      outputStream.write(getSerializeId().ordinal());
      outputStream.writeLong(interval);
      outputStream.writeLong(slidingStep);
      outputStream.writeLong(startTime);
      outputStream.writeLong(endTime);
    } catch (IOException ignored) {
      // ignored
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    interval = buffer.getLong();
    slidingStep = buffer.getLong();
    startTime = buffer.getLong();
    endTime = buffer.getLong();
  }

  @Override
  public FilterSerializeId getSerializeId() {
    return FilterSerializeId.GROUP_BY;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof GroupByFilter)) {
      return false;
    }
    GroupByFilter other = ((GroupByFilter) obj);
    return this.interval == other.interval
        && this.slidingStep == other.slidingStep
        && this.startTime == other.startTime
        && this.endTime == other.endTime;
  }

  @Override
  public int hashCode() {
    return Objects.hash(interval, slidingStep, startTime, endTime);
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  @Override
  public List<TimeRange> getTimeRanges() {
    return startTime >= endTime
        ? Collections.emptyList()
        : Collections.singletonList(new TimeRange(startTime, endTime - 1));
  }
}
