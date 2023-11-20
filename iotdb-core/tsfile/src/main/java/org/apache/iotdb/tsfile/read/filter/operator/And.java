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

package org.apache.iotdb.tsfile.read.filter.operator;

import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.base.BinaryLogicalFilter;

import java.util.ArrayList;
import java.util.List;

public class And extends BinaryLogicalFilter implements Filter {

  public And(Filter left, Filter right) {
    super(left, right);
  }

  @Override
  public boolean satisfy(long time, Object value) {
    return left.satisfy(time, value) && right.satisfy(time, value);
  }

  @Override
  public boolean satisfy(Statistics statistics) {
    return left.satisfy(statistics) && right.satisfy(statistics);
  }

  @Override
  public boolean allSatisfy(Statistics statistics) {
    return left.allSatisfy(statistics) && right.allSatisfy(statistics);
  }

  @Override
  public boolean satisfyStartEndTime(long startTime, long endTime) {
    return left.satisfyStartEndTime(startTime, endTime)
        && right.satisfyStartEndTime(startTime, endTime);
  }

  @Override
  public boolean containStartEndTime(long startTime, long endTime) {
    return left.containStartEndTime(startTime, endTime)
        && right.containStartEndTime(startTime, endTime);
  }

  @Override
  public List<TimeRange> getTimeRanges() {
    List<TimeRange> result = new ArrayList<>();
    List<TimeRange> leftTimeRanges = left.getTimeRanges();
    List<TimeRange> rightTimeRanges = right.getTimeRanges();

    int leftIndex = 0;
    int rightIndex = 0;
    int leftSize = leftTimeRanges.size();
    int rightSize = rightTimeRanges.size();
    while (leftIndex < leftSize && rightIndex < rightSize) {
      TimeRange leftRange = leftTimeRanges.get(leftIndex);
      TimeRange rightRange = rightTimeRanges.get(rightIndex);

      if (leftRange.getMax() < rightRange.getMin()) {
        leftIndex++;
      } else if (rightRange.getMax() < leftRange.getMin()) {
        rightIndex++;
      } else {
        TimeRange intersection =
            new TimeRange(
                Math.max(leftRange.getMin(), rightRange.getMin()),
                Math.min(leftRange.getMax(), rightRange.getMax()));
        result.add(intersection);
        if (leftRange.getMax() <= intersection.getMax()) {
          leftIndex++;
        }
        if (rightRange.getMax() <= intersection.getMax()) {
          rightIndex++;
        }
      }
    }

    return result;
  }

  @Override
  public Filter reverse() {
    return new Or(left.reverse(), right.reverse());
  }
}
