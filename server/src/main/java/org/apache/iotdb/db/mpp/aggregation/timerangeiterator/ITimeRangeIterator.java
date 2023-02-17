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

package org.apache.iotdb.db.mpp.aggregation.timerangeiterator;

import org.apache.iotdb.tsfile.read.common.TimeRange;

/**
 * This interface used for iteratively generating aggregated time windows in GROUP BY query.
 *
 * <p>It will return a leftCloseRightClose time window, by decreasing maxTime if leftCloseRightOpen
 * and increasing minTime if leftOpenRightClose.
 */
public interface ITimeRangeIterator {

  /** return the first time range by sorting order */
  TimeRange getFirstTimeRange();

  /** @return whether current iterator has next time range */
  boolean hasNextTimeRange();
  /**
   * return the next time range according to curStartTime (the start time of the last returned time
   * range)
   */
  TimeRange nextTimeRange();

  boolean isAscending();

  default TimeRange getFinalTimeRange(TimeRange timeRange, boolean leftCRightO) {
    return leftCRightO
        ? new TimeRange(timeRange.getMin(), timeRange.getMax() - 1)
        : new TimeRange(timeRange.getMin() + 1, timeRange.getMax());
  }

  /**
   * As there is only one timestamp can be output for a time range, this method will return the
   * output time based on leftCloseRightOpen or not.
   *
   * @return minTime if leftCloseRightOpen, else maxTime.
   */
  long currentOutputTime();

  long getTotalIntervalNum();
}
