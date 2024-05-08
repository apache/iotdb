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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Either of the left and right operators of AndExpression must satisfy the condition. */
public class OrFilter extends BinaryFilter implements Serializable {

  private static final long serialVersionUID = -968055896528472694L;

  public OrFilter() {}

  public OrFilter(Filter left, Filter right) {
    super(left, right);
  }

  @Override
  public String toString() {
    return "(" + left + " || " + right + ")";
  }

  @Override
  public Filter copy() {
    return new OrFilter(left.copy(), right.copy());
  }

  @Override
  public boolean satisfy(Statistics statistics) {
    return left.satisfy(statistics) || right.satisfy(statistics);
  }

  @Override
  public boolean satisfy(long time, Object value) {
    return left.satisfy(time, value) || right.satisfy(time, value);
  }

  @Override
  public boolean satisfyStartEndTime(long startTime, long endTime) {
    return left.satisfyStartEndTime(startTime, endTime)
        || right.satisfyStartEndTime(startTime, endTime);
  }

  @Override
  public boolean containStartEndTime(long startTime, long endTime) {
    return left.containStartEndTime(startTime, endTime)
        || right.containStartEndTime(startTime, endTime);
  }

  @Override
  public FilterSerializeId getSerializeId() {
    return FilterSerializeId.OR;
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
    TimeRange range;
    if (leftTimeRanges.isEmpty() && rightTimeRanges.isEmpty()) {
      return Collections.emptyList();
    } else if (leftTimeRanges.isEmpty()) {
      return rightTimeRanges;
    } else if (rightTimeRanges.isEmpty()) {
      return leftTimeRanges;
    } else {
      TimeRange leftRange = leftTimeRanges.get(leftIndex);
      TimeRange rightRange = rightTimeRanges.get(rightIndex);
      if (leftRange.getMin() <= rightRange.getMin()) {
        range = leftRange;
        leftIndex++;
      } else {
        range = rightRange;
        rightIndex++;
      }
    }

    while (leftIndex < leftSize || rightIndex < rightSize) {
      TimeRange chosenRange;
      if (leftIndex < leftSize && rightIndex < rightSize) {
        TimeRange leftRange = leftTimeRanges.get(leftIndex);
        TimeRange rightRange = rightTimeRanges.get(rightIndex);
        if (leftRange.getMin() <= rightRange.getMin()) {
          chosenRange = leftRange;
          leftIndex++;
        } else {
          chosenRange = rightRange;
          rightIndex++;
        }
      } else if (leftIndex < leftSize) {
        chosenRange = leftTimeRanges.get(leftIndex);
        leftIndex++;
      } else {
        chosenRange = rightTimeRanges.get(rightIndex);
        rightIndex++;
      }

      if (chosenRange.getMin() > range.getMax()) {
        result.add(range);
        range = chosenRange;
      } else {
        range.setMax(Math.max(range.getMax(), chosenRange.getMax()));
      }
    }

    result.add(range);

    return result;
  }
}
