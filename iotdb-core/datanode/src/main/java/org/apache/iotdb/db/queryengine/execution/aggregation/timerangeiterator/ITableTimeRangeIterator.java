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

public interface ITableTimeRangeIterator {

  TimeIteratorType getType();

  /**
   * @return whether current iterator has next time range.
   */
  boolean hasNextTimeRange();

  boolean hasCachedTimeRange();

  TimeRange getCurTimeRange();

  boolean canFinishCurrentTimeRange(long startTime);

  void resetCurTimeRange();

  void updateCurTimeRange(long startTime);

  void setFinished();

  enum TimeIteratorType {
    DATE_BIN_TIME_ITERATOR,
    SINGLE_TIME_ITERATOR
  }
}
