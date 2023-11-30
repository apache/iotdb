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

package org.apache.iotdb.tsfile.read.filter.basic;

import org.apache.iotdb.tsfile.file.metadata.IStatisticsProvider;
import org.apache.iotdb.tsfile.read.common.TimeRange;

import java.util.List;

public interface IValueFilter extends Filter {

  int getMeasurementIndex();

  default boolean satisfyRow(long time, Object[] values) {
    Object value = values[getMeasurementIndex()];
    if (value == null) {
      // null not satisfy any filter, except IS NULL/IS NOT NULL
      return false;
    }
    return satisfy(time, values[getMeasurementIndex()]);
  }

  default boolean canSkip(IStatisticsProvider statisticsProvider) {
    return canSkip(statisticsProvider.getMeasurementStatistics(getMeasurementIndex()));
  }

  default boolean allSatisfy(IStatisticsProvider statisticsProvider) {
    return allSatisfy(statisticsProvider.getMeasurementStatistics(getMeasurementIndex()));
  }

  default boolean satisfyStartEndTime(long startTime, long endTime) {
    return true;
  }

  default boolean containStartEndTime(long startTime, long endTime) {
    return false;
  }

  @Override
  default List<TimeRange> getTimeRanges() {
    throw new UnsupportedOperationException("Value filter does not support getTimeRanges()");
  }
}
