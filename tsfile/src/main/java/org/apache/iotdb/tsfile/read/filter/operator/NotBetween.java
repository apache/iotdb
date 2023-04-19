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
import java.io.Serializable;
import java.nio.ByteBuffer;

public class NotBetween<T extends Comparable<T>> implements Filter, Serializable {

  private static final long serialVersionUID = 5939421238701173620L;

  protected T value1;

  protected T value2;

  protected FilterType filterType;

  public NotBetween() {}

  public NotBetween(T value1, T value2, FilterType filterType) {
    this.value1 = value1;
    this.value2 = value2;
    this.filterType = filterType;
  }

  @Override
  public void serialize(DataOutputStream outputStream) {
    try {
      outputStream.write(getSerializeId().ordinal());
      outputStream.write(filterType.ordinal());
      ReadWriteIOUtils.writeObject(value1, outputStream);
      ReadWriteIOUtils.writeObject(value2, outputStream);
    } catch (IOException ignored) {
      // ignored
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    filterType = FilterType.values()[buffer.get()];
    value1 = (T) ReadWriteIOUtils.readObject(buffer);
    value2 = (T) ReadWriteIOUtils.readObject(buffer);
  }

  @Override
  public FilterSerializeId getSerializeId() {
    return FilterSerializeId.NOT_BETWEEN;
  }

  @Override
  public boolean satisfy(Statistics statistics) {
    if (filterType == FilterType.TIME_FILTER) {
      return statistics.getStartTime() < (Long) value1 || statistics.getEndTime() > (Long) value2;
    } else {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return true;
      }
      return ((T) statistics.getMinValue()).compareTo(value1) < 0
          || ((T) statistics.getMaxValue()).compareTo(value2) > 0;
    }
  }

  @Override
  public boolean allSatisfy(Statistics statistics) {
    if (filterType == FilterType.TIME_FILTER) {
      return statistics.getStartTime() > (Long) value1 || statistics.getEndTime() < (Long) value2;

    } else {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return false;
      }
      return ((T) statistics.getMinValue()).compareTo(value2) > 0
          || ((T) statistics.getMaxValue()).compareTo(value1) < 0;
    }
  }

  @Override
  public boolean satisfy(long time, Object value) {
    Object v = filterType == FilterType.TIME_FILTER ? time : value;
    return value1.compareTo((T) v) > 0 || ((T) v).compareTo(value2) > 0;
  }

  @Override
  public boolean satisfyStartEndTime(long startTime, long endTime) {
    if (filterType == FilterType.TIME_FILTER) {
      return startTime < (Long) value1 || endTime > (Long) value2;
    } else {
      return true;
    }
  }

  @Override
  public boolean containStartEndTime(long startTime, long endTime) {
    if (filterType == FilterType.TIME_FILTER) {
      return endTime < (Long) value1 || startTime > (Long) value2;
    } else {
      return true;
    }
  }

  @Override
  public Filter copy() {
    return new NotBetween(value1, value2, filterType);
  }
}
