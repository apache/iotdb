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

package org.apache.iotdb.commons.pipe.datastructure.interval;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.TreeSet;

@NotThreadSafe
public class IntervalManager<T extends Interval<T>> {
  private final TreeSet<T> intervals = new TreeSet<>();

  // insert into new interval and merge
  public void addInterval(final T newInterval) {
    // Left closest
    T left = intervals.floor(newInterval);

    // Right closest
    T right = intervals.ceiling(newInterval);

    // Merge left ([0,1] + [2,3] → [0,3])
    while (left != null && left.end >= newInterval.start - 1) {
      newInterval.start = Math.min(left.start, newInterval.start);
      newInterval.end = Math.max(left.end, newInterval.end);
      newInterval.onMerged(left);
      intervals.remove(left);
      left = intervals.floor(newInterval);
    }

    // Merge right ([2,3] + [3,4] → [2,4])
    while (right != null && newInterval.end >= right.start - 1) {
      newInterval.start = Math.min(newInterval.start, right.start);
      newInterval.end = Math.max(newInterval.end, right.end);
      newInterval.onMerged(right);
      intervals.remove(right);
      right = intervals.ceiling(newInterval);
    }

    intervals.add(newInterval);
  }

  public T peek() {
    return intervals.first();
  }

  public boolean remove(final T interval) {
    if (intervals.remove(interval)) {
      interval.onRemoved();
      return true;
    }
    return false;
  }

  public int size() {
    return intervals.size();
  }

  @Override
  public String toString() {
    return "IntervalManager{" + "intervals=" + intervals + '}';
  }
}
