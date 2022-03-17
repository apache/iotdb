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
import org.apache.iotdb.db.query.executor.groupby.SlidingWindowGroupByExecutor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/**
 * When calculating MAX_TIME and LAST_VALUE (the MIN_TIME and FIRST_VALUE in descending order), the
 * aggregation result always appears in the most recent pre-aggregation result. So, we do not need
 * to cache the previous pre-aggregated results in the queue.
 */
public class EmptyQueueSlidingWindowGroupByExecutor extends SlidingWindowGroupByExecutor {

  public EmptyQueueSlidingWindowGroupByExecutor(
      TSDataType dataType, String aggrFuncName, boolean ascending) {
    super(dataType, aggrFuncName, ascending);
  }

  @Override
  public void update(AggregateResult aggregateResult) {
    if (aggregateResult.getResult() != null) {
      this.aggregateResult = aggregateResult;
    }
  }

  @Override
  protected void evictingExpiredValue() {
    if (!inTimeRange(this.aggregateResult.getTime())) {
      this.aggregateResult.reset();
    }
  }
}
