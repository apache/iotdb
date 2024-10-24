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

package org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator;

import org.apache.tsfile.read.common.TimeRange;

public class TableSingleTimeWindowIterator implements ITableTimeRangeIterator {

  // when all devices are consumed up, finished = true
  boolean finished = false;

  private TimeRange curTimeRange;

  public TableSingleTimeWindowIterator(TimeRange curTimeRange) {
    this.curTimeRange = curTimeRange;
  }

  public TableSingleTimeWindowIterator() {}

  @Override
  public TimeIteratorType getType() {
    return TimeIteratorType.SINGLE_TIME_ITERATOR;
  }

  @Override
  public boolean hasNextTimeRange() {
    return !finished;
  }

  @Override
  public boolean hasCachedTimeRange() {
    return curTimeRange != null;
  }

  @Override
  public TimeRange getCurTimeRange() {
    return curTimeRange;
  }

  @Override
  public boolean canFinishCurrentTimeRange(long startTime) {
    return false;
  }

  @Override
  public void resetCurTimeRange() {
    this.curTimeRange = null;
  }

  @Override
  public void updateCurTimeRange(long startTime) {
    // only meets real data, init the curTimeRange
    if (curTimeRange == null) {
      curTimeRange = new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE);
    }
  }

  @Override
  public void setFinished() {
    this.curTimeRange = null;
    this.finished = true;
  }
}
