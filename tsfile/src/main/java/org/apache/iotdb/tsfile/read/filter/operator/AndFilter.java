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
import org.apache.iotdb.tsfile.read.filter.basic.BinaryFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterSerializeId;

import java.util.ArrayList;
import java.util.List;

/** Both the left and right operators of AndExpression must satisfy the condition. */
public class AndFilter extends BinaryFilter {

  private static final long serialVersionUID = -8212850098906044102L;

  public AndFilter() {}

  public AndFilter(Filter left, Filter right) {
    super(left, right);
  }

  @Override
  public boolean satisfy(Statistics statistics) {
    return left.satisfy(statistics) && right.satisfy(statistics);
  }

  @Override
  public boolean satisfy(long time, Object value) {
    return left.satisfy(time, value) && right.satisfy(time, value);
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
  public String toString() {
    return "(" + left + " && " + right + ")";
  }

  @Override
  public Filter copy() {
    return new AndFilter(left.copy(), right.copy());
  }

  @Override
  public FilterSerializeId getSerializeId() {
    return FilterSerializeId.AND;
  }

  @Override
  public List<TimeRange> getTimeRanges() {
    List<TimeRange> result = new ArrayList<>();
    List<TimeRange> leftTimeRanges = left.getTimeRanges();
    List<TimeRange> rightTimeRanges = right.getTimeRanges();

    int leftIndex = 0,
        rightIndex = 0,
        leftSize = leftTimeRanges.size(),
        rightSize = rightTimeRanges.size();
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
}
