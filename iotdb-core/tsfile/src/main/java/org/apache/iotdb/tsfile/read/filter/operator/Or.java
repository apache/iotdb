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

import org.apache.iotdb.tsfile.file.metadata.IMetadata;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.filter.basic.BinaryLogicalFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.basic.OperatorType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Or extends BinaryLogicalFilter {

  public Or(Filter left, Filter right) {
    super(left, right);
  }

  public Or(ByteBuffer buffer) {
    super(Filter.deserialize(buffer), Filter.deserialize(buffer));
  }

  @Override
  public boolean satisfy(long time, Object value) {
    return left.satisfy(time, value) || right.satisfy(time, value);
  }

  @Override
  public boolean satisfyRow(long time, Object[] values) {
    return left.satisfyRow(time, values) || right.satisfyRow(time, values);
  }

  @Override
  public boolean[] satisfyTsBlock(TsBlock tsBlock) {
    boolean[] leftResult = left.satisfyTsBlock(tsBlock);
    boolean[] rightResult = right.satisfyTsBlock(tsBlock);
    for (int i = 0; i < leftResult.length; i++) {
      leftResult[i] = leftResult[i] || rightResult[i];
    }
    return leftResult;
  }

  @Override
  public boolean canSkip(IMetadata metadata) {
    // we can only drop a chunk of records if we know that both the left and right predicates agree
    // that no matter what we don't need this chunk.
    return left.canSkip(metadata) && right.canSkip(metadata);
  }

  @Override
  public boolean allSatisfy(IMetadata metadata) {
    return left.allSatisfy(metadata) || right.allSatisfy(metadata);
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
  public List<TimeRange> getTimeRanges() {
    List<TimeRange> result = new ArrayList<>();
    List<TimeRange> leftTimeRanges = left.getTimeRanges();
    List<TimeRange> rightTimeRanges = right.getTimeRanges();

    // Check for empty lists
    if (leftTimeRanges.isEmpty()) {
      return rightTimeRanges;
    } else if (rightTimeRanges.isEmpty()) {
      return leftTimeRanges;
    }

    Index leftIndex = new Index();
    Index rightIndex = new Index();

    // Initialize the current range
    TimeRange range = chooseNextRange(leftTimeRanges, rightTimeRanges, leftIndex, rightIndex);

    // Merge overlapping ranges
    while (leftIndex.getCurrent() < leftTimeRanges.size()
        || rightIndex.getCurrent() < rightTimeRanges.size()) {
      TimeRange chosenRange =
          chooseNextRange(leftTimeRanges, rightTimeRanges, leftIndex, rightIndex);

      // Merge or add the non-overlapping range
      if (chosenRange.getMin() > range.getMax()) {
        result.add(range);
        range = chosenRange;
      } else {
        range.setMax(Math.max(range.getMax(), chosenRange.getMax()));
      }
    }

    // Add the last range
    result.add(range);

    return result;
  }

  private TimeRange chooseNextRange(
      List<TimeRange> leftTimeRanges,
      List<TimeRange> rightTimeRanges,
      Index leftIndex,
      Index rightIndex) {
    int leftCurrentIndex = leftIndex.getCurrent();
    int rightCurrentIndex = rightIndex.getCurrent();
    if (leftCurrentIndex < leftTimeRanges.size() && rightCurrentIndex < rightTimeRanges.size()) {
      TimeRange leftRange = leftTimeRanges.get(leftCurrentIndex);
      TimeRange rightRange = rightTimeRanges.get(rightCurrentIndex);

      // Choose the range with the smaller minimum start time
      if (leftRange.getMin() <= rightRange.getMin()) {
        leftIndex.increment();
        return leftRange;
      } else {
        rightIndex.increment();
        return rightRange;
      }
    } else if (leftCurrentIndex < leftTimeRanges.size()) {
      leftIndex.increment();
      return leftTimeRanges.get(leftCurrentIndex);
    } else {
      rightIndex.increment();
      return rightTimeRanges.get(rightCurrentIndex);
    }
  }

  private static class Index {
    private int current;

    public Index() {
      this.current = 0;
    }

    public void increment() {
      this.current++;
    }

    public int getCurrent() {
      return this.current;
    }
  }

  @Override
  public Filter reverse() {
    return new And(left.reverse(), right.reverse());
  }

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.OR;
  }

  @Override
  public String toString() {
    return "(" + left + " || " + right + ")";
  }
}
