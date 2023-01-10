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

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterSerializeId;
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Between<T extends Comparable<T>> implements Filter {

  private static final long serialVersionUID = -537390606419370764L;

  protected T value1;

  protected T value2;

  protected boolean not;

  protected FilterType filterType;

  public Between() {}

  public Between(T value1, T value2, FilterType filterType, boolean not) {
    this.value1 = value1;
    this.value2 = value2;
    this.filterType = filterType;
    this.not = not;
  }

  @Override
  public void serialize(DataOutputStream outputStream) {
    try {
      outputStream.write(getSerializeId().ordinal());
      outputStream.write(filterType.ordinal());
      ReadWriteIOUtils.write(not, outputStream);
      ReadWriteIOUtils.writeObject(value1, outputStream);
      ReadWriteIOUtils.writeObject(value2, outputStream);
    } catch (IOException ignored) {
      // ignored
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    filterType = FilterType.values()[buffer.get()];
    not = ReadWriteIOUtils.readBool(buffer);
    value1 = (T) ReadWriteIOUtils.readObject(buffer);
    value2 = (T) ReadWriteIOUtils.readObject(buffer);
  }

  @Override
  public FilterSerializeId getSerializeId() {
    return FilterSerializeId.BETWEEN;
  }

  @Override
  public boolean satisfy(Statistics statistics) {
    if (filterType == FilterType.TIME_FILTER) {
      long time1 = (Long) value1, time2 = (Long) value2;
      if (not) {
        return statistics.getStartTime() < time1 || statistics.getEndTime() > time2;
      } else {
        return statistics.getEndTime() >= time1 || statistics.getStartTime() <= time2;
      }
    } else {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return true;
      }
      if (not) {
        return ((T) statistics.getMinValue()).compareTo(value1) < 0
            || ((T) statistics.getMaxValue()).compareTo(value2) > 0;
      } else {
        return ((T) statistics.getMaxValue()).compareTo(value1) >= 0
            && ((T) statistics.getMinValue()).compareTo(value2) <= 0;
      }
    }
  }

  @Override
  public boolean satisfy(long time, Object value) {
    Object v = filterType == FilterType.TIME_FILTER ? time : value;
    return (value1.compareTo((T) v) <= 0 && ((T) v).compareTo(value2) <= 0) ^ not;
  }

  @Override
  public boolean satisfyStartEndTime(long startTime, long endTime) {
    if (filterType == FilterType.TIME_FILTER) {
      long time1 = (Long) value1, time2 = (Long) value2;
      if (not) {
        return startTime < time1 || endTime > time2;
      } else {
        return endTime >= time1 && startTime <= time2;
      }
    } else {
      return true;
    }
  }

  @Override
  public boolean containStartEndTime(long startTime, long endTime) {
    if (filterType == FilterType.TIME_FILTER) {
      long time1 = (Long) value1, time2 = (Long) value2;
      if (not) {
        return endTime < time1 || startTime > time2;
      } else {
        return startTime >= time1 && endTime <= time2;
      }
    } else {
      return true;
    }
  }

  @Override
  public Filter copy() {
    return new Between(value1, value2, filterType, not);
  }
}
