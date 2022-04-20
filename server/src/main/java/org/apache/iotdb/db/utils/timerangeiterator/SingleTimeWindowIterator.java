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

package org.apache.iotdb.db.utils.timerangeiterator;

import org.apache.iotdb.tsfile.read.common.TimeRange;

/** Used for aggregation with only one time window. i.e. Aggregation without group by. */
public class SingleTimeWindowIterator implements ITimeRangeIterator {

  // total query [startTime, endTime)
  private final long startTime;
  private final long endTime;

  private TimeRange curTimeRange;

  public SingleTimeWindowIterator(long startTime, long endTime) {
    this.startTime = startTime;
    this.endTime = endTime;
  }

  @Override
  public TimeRange getFirstTimeRange() {
    return new TimeRange(startTime, endTime);
  }

  @Override
  public boolean hasNextTimeRange() {
    if (curTimeRange == null) {
      curTimeRange = getFirstTimeRange();
      return true;
    } else {
      return false;
    }
  }

  @Override
  public TimeRange nextTimeRange() {
    if (curTimeRange != null || hasNextTimeRange()) {
      return curTimeRange;
    }
    return null;
  }

  @Override
  public boolean isAscending() {
    return false;
  }
}
