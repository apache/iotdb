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

package org.apache.iotdb.db.storageengine.dataregion.compaction.tool;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ListTimeRangeImpl implements ITimeRange {

  List<Interval> intervalList = new LinkedList<>();
  // 0-10， 20-30， 50-70
  // 25-60
  // 0-10. 20-70

  @Override
  public void addInterval(Interval interval) {
    List<Interval> mergedIntervals = new ArrayList<>();
    int index = 0;

    // 1. elements that do not overlap with the newly added element are placed directly in the
    // result
    while (index < intervalList.size() && intervalList.get(index).getEnd() < interval.getStart()) {
      mergedIntervals.add(intervalList.get(index));
      index++;
    }

    // 2. if the element overlaps with an existing element, start equals the minimum value of the
    // overlap and end equals the maximum value of the overlap
    while (index < intervalList.size() && intervalList.get(index).getStart() <= interval.getEnd()) {
      interval.setStart(Math.min(intervalList.get(index).getStart(), interval.getStart()));
      interval.setEnd(Math.max(intervalList.get(index).getEnd(), interval.getEnd()));
      index++;
    }
    mergedIntervals.add(interval);

    // 3. add the remaining elements to the result set
    while (index < intervalList.size()) {
      mergedIntervals.add(intervalList.get(index));
      index++;
    }

    intervalList.clear();
    intervalList.addAll(mergedIntervals);
  }

  public List<Interval> getIntervalList() {
    return intervalList;
  }

  /**
   * case 1: interval.getStart() <= currentInterval.getEnd()
   *
   * <p>currentInterval: [5,10], interval: [6,15],[1,7],[0,5],[10,15]
   *
   * <p>case 2: interval.getEnd() <= currentInterval.getEnd()
   *
   * <p>currentInterval: [5,10], interval:[1,9],[0,9],[1,10]
   */
  @Override
  public boolean isOverlapped(Interval interval) {
    for (Interval currentInterval : intervalList) {
      boolean isOverlap =
          interval.getStart() <= currentInterval.getEnd()
              && interval.getEnd() >= currentInterval.getStart();
      if (isOverlap) {
        return true;
      }
    }
    return false;
  }
}
