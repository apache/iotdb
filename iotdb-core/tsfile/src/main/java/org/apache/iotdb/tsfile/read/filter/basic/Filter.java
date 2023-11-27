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

package org.apache.iotdb.tsfile.read.filter.basic;

import org.apache.iotdb.tsfile.file.metadata.IAlignedMetadata;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.filter.factory.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.factory.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.operator.And;
import org.apache.iotdb.tsfile.read.filter.operator.GroupByFilter;
import org.apache.iotdb.tsfile.read.filter.operator.GroupByMonthFilter;
import org.apache.iotdb.tsfile.read.filter.operator.Not;
import org.apache.iotdb.tsfile.read.filter.operator.Or;
import org.apache.iotdb.tsfile.read.filter.operator.TimeFilterOperators;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * A Filter is an executable expression tree describing the criteria for which records to keep when
 * loading data from a TsFile.
 *
 * <p>Currently, they are applied to all File/Chunk/Page when scanning to see if we can drop them
 * entirely, and then they are applied during column assembly to drop individual records that are
 * not wanted.
 *
 * <p>See {@link TimeFilterOperators}/{@link ValueFilterOperators} for the implementation of the
 * operator tokens,
 *
 * <p>and {@link TimeFilter}/{@link ValueFilter}/{@link FilterFactory} for the dsl functions for
 * constructing an expression tree.
 */
public interface Filter {

  /**
   * To examine whether the single point(with time and value) is satisfied with the filter.
   *
   * @param time single point time
   * @param value single point value
   * @return true if the single point is satisfied with the filter, false otherwise
   */
  boolean satisfy(long time, Object value);

  /**
   * To examine whether there are data points satisfied with the filter.
   *
   * @param statistics statistics with min time, max time, min value, max value.
   * @return false if there are no data points satisfied with the filter
   */
  boolean canSkip(Statistics<? extends Serializable> statistics);

  boolean canSkip(IAlignedMetadata alignedMetadata);

  /**
   * To examine whether all data points are satisfied with the filter.
   *
   * @param statistics statistics with min time, max time, min value, max value.
   * @return true if all data points are satisfied with the filter
   */
  boolean allSatisfy(Statistics<? extends Serializable> statistics);

  boolean allSatisfy(IAlignedMetadata alignedMetadata);

  /**
   * To examine whether the min time and max time are satisfied with the filter.
   *
   * <p>Note: only implemented by {@link ITimeFilter}
   *
   * @param startTime start time of a page, series or device
   * @param endTime end time of a page, series or device
   * @return false if [startTime, endTime] is not satisfied with the filter
   */
  boolean satisfyStartEndTime(long startTime, long endTime);

  /**
   * To examine whether the partition [startTime, endTime] is subsets of filter.
   *
   * <p>Note: only implemented by {@link ITimeFilter}
   *
   * @param startTime start time of a partition
   * @param endTime end time of a partition
   * @return true if the partition [startTime, endTime] is subsets of filter
   */
  boolean containStartEndTime(long startTime, long endTime);

  /**
   * Return the time ranges which satisfying the filter.
   *
   * <p>Note: only implemented by {@link ITimeFilter}
   */
  List<TimeRange> getTimeRanges();

  /**
   * Reverse the filter.
   *
   * @return the logical inverse filter
   */
  Filter reverse();

  /**
   * Copy the filter.
   *
   * <p>When the filter is <b>stateless</b>, we can return the filter itself.
   *
   * @return a copy of the filter
   */
  default Filter copy() {
    return this;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // serialize & deserialize related methods
  /////////////////////////////////////////////////////////////////////////////////////////////////

  OperatorType getOperatorType();

  void serialize(DataOutputStream outputStream) throws IOException;

  default void serialize(ByteBuffer buffer) throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    this.serialize(dataOutputStream);
    buffer.put(byteArrayOutputStream.toByteArray());
  }

  static Filter deserialize(ByteBuffer buffer) {
    OperatorType type = OperatorType.values()[ReadWriteIOUtils.readInt(buffer)];

    switch (type) {
      case TIME_EQ:
        return new TimeFilterOperators.TimeEq(buffer);
      case TIME_NEQ:
        return new TimeFilterOperators.TimeNotEq(buffer);
      case TIME_GT:
        return new TimeFilterOperators.TimeGt(buffer);
      case TIME_GTEQ:
        return new TimeFilterOperators.TimeGtEq(buffer);
      case TIME_LT:
        return new TimeFilterOperators.TimeLt(buffer);
      case TIME_LTEQ:
        return new TimeFilterOperators.TimeLtEq(buffer);
      case TIME_IN:
        return new TimeFilterOperators.TimeIn(buffer);
      case TIME_NOT_IN:
        return new TimeFilterOperators.TimeNotIn(buffer);
      case TIME_BETWEEN_AND:
        return new TimeFilterOperators.TimeBetweenAnd(buffer);
      case TIME_NOT_BETWEEN_AND:
        return new TimeFilterOperators.TimeNotBetweenAnd(buffer);
      case VALUE_EQ:
        return new ValueFilterOperators.ValueEq<>(buffer);
      case VALUE_NEQ:
        return new ValueFilterOperators.ValueNotEq<>(buffer);
      case VALUE_GT:
        return new ValueFilterOperators.ValueGt<>(buffer);
      case VALUE_GTEQ:
        return new ValueFilterOperators.ValueGtEq<>(buffer);
      case VALUE_LT:
        return new ValueFilterOperators.ValueLt<>(buffer);
      case VALUE_LTEQ:
        return new ValueFilterOperators.ValueLtEq<>(buffer);
      case VALUE_IN:
        return new ValueFilterOperators.ValueIn<>(buffer);
      case VALUE_NOT_IN:
        return new ValueFilterOperators.ValueNotIn<>(buffer);
      case VALUE_BETWEEN_AND:
        return new ValueFilterOperators.ValueBetweenAnd<>(buffer);
      case VALUE_NOT_BETWEEN_AND:
        return new ValueFilterOperators.ValueNotBetweenAnd<>(buffer);
      case VALUE_REGEXP:
        return new ValueFilterOperators.ValueRegexp(buffer);
      case VALUE_NOT_REGEXP:
        return new ValueFilterOperators.ValueNotRegexp(buffer);
      case GROUP_BY_TIME:
        return new GroupByFilter(buffer);
      case GROUP_BY_MONTH:
        return new GroupByMonthFilter(buffer);
      case AND:
        return new And(buffer);
      case OR:
        return new Or(buffer);
      case NOT:
        return new Not(buffer);
      default:
        throw new UnsupportedOperationException("Unsupported operator type:" + type);
    }
  }
}
