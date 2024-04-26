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

package org.apache.iotdb.db.pipe.processor.twostage.operator;

import org.apache.iotdb.db.pipe.processor.twostage.state.CountState;
import org.apache.iotdb.db.pipe.processor.twostage.state.State;

import org.apache.tsfile.utils.Pair;

import java.util.Queue;

public class CountOperator implements Operator {

  private final long onCompletionTimestamp;
  private long globalCount;

  private final Queue<Pair<Long, Long>> globalCountQueue;

  public CountOperator(String combineId, Queue<Pair<Long, Long>> globalCountQueue) {
    onCompletionTimestamp = Long.parseLong(combineId);
    globalCount = 0;

    this.globalCountQueue = globalCountQueue;
  }

  @Override
  public void combine(State state) {
    globalCount += ((CountState) state).getCount();
  }

  @Override
  public void onComplete() {
    globalCountQueue.add(new Pair<>(onCompletionTimestamp, globalCount));
  }
}
