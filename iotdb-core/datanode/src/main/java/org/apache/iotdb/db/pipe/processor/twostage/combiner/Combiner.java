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

package org.apache.iotdb.db.pipe.processor.twostage.combiner;

import org.apache.iotdb.db.pipe.processor.twostage.operator.Operator;
import org.apache.iotdb.db.pipe.processor.twostage.state.State;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class Combiner {

  private static final int MAX_COMBINER_LIVE_TIME_IN_MS = 1000 * 60 * 30; // 30 minutes
  private final long creationTimeInMs;

  private final Operator operator;

  private final Set<Integer> expectedRegionIdSet;
  private final Set<Integer> receivedRegionIdSet;

  private final AtomicBoolean isComplete = new AtomicBoolean(false);

  public Combiner(Operator operator, Set<Integer> expectedRegionIdSet) {
    this.creationTimeInMs = System.currentTimeMillis();

    this.operator = operator;

    this.expectedRegionIdSet = expectedRegionIdSet;
    this.receivedRegionIdSet = new HashSet<>();
  }

  public void combine(int regionId, State state) {
    if (expectedRegionIdSet.contains(regionId)) {
      return;
    }

    receivedRegionIdSet.add(regionId);
    operator.combine(state);

    if (receivedRegionIdSet.equals(expectedRegionIdSet)) {
      operator.onComplete();
    }

    isComplete.set(true);
  }

  public boolean isOutdated() {
    return System.currentTimeMillis() - creationTimeInMs > MAX_COMBINER_LIVE_TIME_IN_MS;
  }

  public boolean isComplete() {
    return isComplete.get();
  }
}
