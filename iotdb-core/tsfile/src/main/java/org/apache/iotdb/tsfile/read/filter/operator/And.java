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

public class And extends BinaryLogicalFilter {

  public And(Filter left, Filter right) {
    super(left, right);
  }

  public And(ByteBuffer buffer) {
    super(Filter.deserialize(buffer), Filter.deserialize(buffer));
  }

  @Override
  public boolean satisfy(long time, Object value) {
    return left.satisfy(time, value) && right.satisfy(time, value);
  }

  @Override
  public boolean satisfyRow(long time, Object[] values) {
    return left.satisfyRow(time, values) && right.satisfyRow(time, values);
  }

  @Override
  public boolean[] satisfyTsBlock(TsBlock tsBlock) {
    boolean[] leftResult = left.satisfyTsBlock(tsBlock);
    boolean[] rightResult = right.satisfyTsBlock(tsBlock);
    for (int i = 0; i < leftResult.length; i++) {
      leftResult[i] = leftResult[i] && rightResult[i];
    }
    return leftResult;
  }

  @Override
  public boolean canSkip(IMetadata metadata) {
    // we can drop a chunk of records if we know that either the left or the right predicate agrees
    // that no matter what we don't need this chunk.
    return left.canSkip(metadata) || right.canSkip(metadata);
  }

  @Override
  public boolean allSatisfy(IMetadata metadata) {
    return left.allSatisfy(metadata) && right.allSatisfy(metadata);
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

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.AND;
  }

  @Override
  public String toString() {
    return "(" + left + " && " + right + ")";
  }
}
