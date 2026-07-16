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

package org.apache.iotdb.calc.execution.filter;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A dynamic time threshold shared between TopK and Scan operators.
 *
 * <p>When TopK heap is full, the worst row in the heap becomes the threshold. Scan operators can
 * skip rows (and even files) that cannot possibly enter the final TopK result.
 */
public class TopKRuntimeFilter {
  private final boolean ascending;
  private final AtomicLong threshold;

  public TopKRuntimeFilter(boolean ascending) {
    this.ascending = ascending;
    threshold = new AtomicLong(ascending ? Long.MAX_VALUE : Long.MIN_VALUE);
  }

  public boolean isAscending() {
    return ascending;
  }

  /** Update threshold with the current heap-top time. Only tightens the bound. */
  public void updateThreshold(long time) {
    if (ascending) {
      // ASC TopK: keep smallest K rows, threshold is the largest time among them
      threshold.updateAndGet(prev -> Math.min(prev, time));
    } else {
      // DESC TopK: keep largest K rows, threshold is the smallest time among them
      threshold.updateAndGet(prev -> Math.max(prev, time));
    }
  }

  public boolean mayQualify(long time) {
    long current = threshold.get();
    return ascending ? time < current : time > current;
  }

  public boolean mayQualifyRange(long startTime, long endTime) {
    long current = threshold.get();
    return ascending ? startTime < current : endTime > current;
  }

  public long getThreshold() {
    return threshold.get();
  }
}
