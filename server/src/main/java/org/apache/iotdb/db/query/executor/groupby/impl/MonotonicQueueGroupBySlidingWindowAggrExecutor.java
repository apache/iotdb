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

package org.apache.iotdb.db.query.executor.groupby.impl;

import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.executor.groupby.GroupBySlidingWindowAggrExecutor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.Comparator;

public class MonotonicQueueGroupBySlidingWindowAggrExecutor
    extends GroupBySlidingWindowAggrExecutor {

  private final Comparator<Object> comparator;

  public MonotonicQueueGroupBySlidingWindowAggrExecutor(
      TSDataType dataType, String aggrFuncName, boolean ascending, Comparator<Object> comparator) {
    super(dataType, aggrFuncName, ascending);
    this.comparator = comparator;
  }

  @Override
  public void update(AggregateResult aggregateResult) {
    Object res = aggregateResult.getResult();
    if (res == null) {
      return;
    }

    while (!deque.isEmpty() && comparator.compare(res, deque.getLast().getResult()) > 0) {
      deque.removeLast();
    }
    deque.addLast(aggregateResult);
    if (!deque.isEmpty()) {
      this.aggregateResult = deque.getFirst();
    } else {
      this.aggregateResult.reset();
    }
  }

  @Override
  protected void evictingExpiredValue() {
    while (!deque.isEmpty() && !inTimeRange(deque.getFirst().getTime())) {
      deque.removeFirst();
    }
    if (!deque.isEmpty()) {
      this.aggregateResult = deque.getFirst();
    } else {
      this.aggregateResult.reset();
    }
  }
}
