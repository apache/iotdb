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

package org.apache.iotdb.db.query.dataset.groupby.queue;

import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class MinValueSlidingWindowAggrQueue extends SlidingWindowAggrQueue {

  public MinValueSlidingWindowAggrQueue(
      TSDataType dataType, String aggrFuncName, boolean ascending) {
    super(dataType, aggrFuncName, ascending);
  }

  @Override
  public void update(AggregateResult aggregateResult) {
    Comparable<Object> minVal = (Comparable<Object>) aggregateResult.getResult();
    if (minVal == null) {
      return;
    }

    while (!deque.isEmpty() && minVal.compareTo(deque.getLast().getResult()) < 0) {
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
