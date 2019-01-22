/**
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

import org.apache.iotdb.tsfile.read.filter.DigestForFilter;
import org.apache.iotdb.tsfile.read.filter.basic.UnaryFilter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;

/**
 * Greater than filter.
 *
 * @param <T>
 *            comparable data type
 */
public class Gt<T extends Comparable<T>> extends UnaryFilter<T> {

  private static final long serialVersionUID = -2088181659871608986L;

  public Gt(T value, FilterType filterType) {
    super(value, filterType);
  }

  @Override
  public boolean satisfy(DigestForFilter digest) {
    if (filterType == FilterType.TIME_FILTER) {
      return ((Long) value) < digest.getMaxTime();
    } else {
      return value.compareTo(digest.getMaxValue()) < 0;
    }
  }

  @Override
  public boolean satisfy(long time, Object value) {
    Object v = filterType == FilterType.TIME_FILTER ? time : value;
    return this.value.compareTo((T) v) < 0;
  }

  @Override
  public boolean satisfyStartEndTime(long startTime, long endTime) {
    if (filterType == FilterType.TIME_FILTER) {
      long time = (Long) value;
      if (time >= endTime) {
        return false;
      }
      return true;
    } else {
      return true;
    }
  }

  @Override
  public String toString() {
    return getFilterType() + " > " + value;
  }
}
