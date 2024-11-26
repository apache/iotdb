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

package org.apache.tsfile.read.filter.operator;

import org.apache.tsfile.file.metadata.IMetadata;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.basic.OperatorType;
import org.apache.tsfile.utils.Binary;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Not extends Filter {

  private final Filter filter;

  public static final String CONTAIN_NOT_ERR_MSG =
      "This predicate contains a not! "
          + "Did you forget to run this predicate through PredicateRemoveNotRewriter? ";

  public Not(Filter filter) {
    this.filter = Objects.requireNonNull(filter, "filter cannot be null");
  }

  public Not(ByteBuffer buffer) {
    this.filter = Objects.requireNonNull(Filter.deserialize(buffer), "filter cannot be null");
  }

  @Override
  public boolean satisfy(long time, Object value) {
    return !filter.satisfy(time, value);
  }

  @Override
  public boolean satisfyBoolean(long time, boolean value) {
    return !filter.satisfyBoolean(time, value);
  }

  @Override
  public boolean satisfyInteger(long time, int value) {
    return !filter.satisfyInteger(time, value);
  }

  @Override
  public boolean satisfyLong(long time, long value) {
    return !filter.satisfyLong(time, value);
  }

  @Override
  public boolean satisfyFloat(long time, float value) {
    return !filter.satisfyFloat(time, value);
  }

  @Override
  public boolean satisfyDouble(long time, double value) {
    return !filter.satisfyDouble(time, value);
  }

  @Override
  public boolean satisfyBinary(long time, Binary value) {
    return !filter.satisfyBinary(time, value);
  }

  @Override
  public boolean satisfyRow(long time, Object[] values) {
    return !filter.satisfyRow(time, values);
  }

  @Override
  public boolean satisfyBooleanRow(long time, boolean[] values) {
    return !filter.satisfyBooleanRow(time, values);
  }

  @Override
  public boolean satisfyIntegerRow(long time, int[] values) {
    return !filter.satisfyIntegerRow(time, values);
  }

  @Override
  public boolean satisfyLongRow(long time, long[] values) {
    return !filter.satisfyLongRow(time, values);
  }

  @Override
  public boolean satisfyFloatRow(long time, float[] values) {
    return !filter.satisfyFloatRow(time, values);
  }

  @Override
  public boolean satisfyDoubleRow(long time, double[] values) {
    return !filter.satisfyDoubleRow(time, values);
  }

  @Override
  public boolean satisfyBinaryRow(long time, Binary[] values) {
    return !filter.satisfyBinaryRow(time, values);
  }

  @Override
  public boolean[] satisfyTsBlock(TsBlock tsBlock) {
    boolean[] selection = new boolean[tsBlock.getPositionCount()];
    Arrays.fill(selection, true);
    return satisfyTsBlock(selection, tsBlock);
  }

  @Override
  public boolean[] satisfyTsBlock(boolean[] selection, TsBlock tsBlock) {
    boolean[] result = filter.satisfyTsBlock(selection, tsBlock);
    for (int i = 0; i < result.length; i++) {
      // compare the result of filter with the selection
      // if the result is different to selection, then the result should be true
      result[i] = result[i] != selection[i];
    }
    return result;
  }

  @Override
  public boolean canSkip(IMetadata metadata) {
    // [not filter.canSkip(block)] is not equivalent to [notFilter.canSkip(block)]
    // e.g. block min = 5, max = 15, filter = [value > 10], notFilter = [value <= 10)]
    //  not filter.canSkip(block) = true (expected false), notFilter.canSkip(block) = false
    throw new IllegalArgumentException(CONTAIN_NOT_ERR_MSG + this);
  }

  @Override
  public boolean allSatisfy(IMetadata metadata) {
    // same as canSkip
    throw new IllegalArgumentException(CONTAIN_NOT_ERR_MSG + this);
  }

  @Override
  public boolean satisfyStartEndTime(long startTime, long endTime) {
    throw new IllegalArgumentException(CONTAIN_NOT_ERR_MSG + this);
  }

  @Override
  public boolean containStartEndTime(long startTime, long endTime) {
    throw new IllegalArgumentException(CONTAIN_NOT_ERR_MSG + this);
  }

  public Filter getFilter() {
    return this.filter;
  }

  @Override
  public List<TimeRange> getTimeRanges() {
    List<TimeRange> list = filter.getTimeRanges();
    if (list.isEmpty()) {
      return list;
    }
    List<TimeRange> res = new ArrayList<>();
    if (list.get(0).getMin() != Long.MIN_VALUE) {
      res.add(new TimeRange(Long.MIN_VALUE, list.get(0).getMin() - 1));
    }
    for (int i = 1, size = list.size(); i < size; i++) {
      long left = list.get(i - 1).getMax() + 1;
      long right = list.get(i).getMin() - 1;
      if (left <= right) {
        res.add(new TimeRange(left, right));
      }
    }

    if (list.get(list.size() - 1).getMax() != Long.MAX_VALUE) {
      res.add(new TimeRange(list.get(list.size() - 1).getMax() + 1, Long.MAX_VALUE));
    }
    return res;
  }

  @Override
  public Filter reverse() {
    return filter;
  }

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.NOT;
  }

  @Override
  public void serialize(DataOutputStream outputStream) throws IOException {
    super.serialize(outputStream);
    filter.serialize(outputStream);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Not not = (Not) o;
    return filter.equals(not.filter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(filter);
  }

  @Override
  public String toString() {
    return "not(" + filter + ")";
  }
}
