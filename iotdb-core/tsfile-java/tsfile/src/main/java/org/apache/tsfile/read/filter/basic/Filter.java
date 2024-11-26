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

package org.apache.tsfile.read.filter.basic;

import org.apache.tsfile.file.metadata.IMetadata;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.tsfile.read.filter.operator.And;
import org.apache.tsfile.read.filter.operator.GroupByFilter;
import org.apache.tsfile.read.filter.operator.GroupByMonthFilter;
import org.apache.tsfile.read.filter.operator.Not;
import org.apache.tsfile.read.filter.operator.Or;
import org.apache.tsfile.read.filter.operator.TimeFilterOperators;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.FilterDeserialize;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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
 * <p>See {@link TimeFilterOperators} for the implementation of the operator tokens,
 *
 * <p>and {@link TimeFilterApi}/{@link ValueFilterApi}/{@link FilterFactory} for the dsl functions
 * for constructing an expression tree.
 */
public abstract class Filter {

  /**
   * To examine whether the single point(with time and value) is satisfied with the filter.
   *
   * @param time single point time
   * @param value single point value
   * @return true if the single point is satisfied with the filter, false otherwise
   */
  public abstract boolean satisfy(long time, Object value);

  public abstract boolean satisfyBoolean(long time, boolean value);

  public abstract boolean satisfyInteger(long time, int value);

  public abstract boolean satisfyLong(long time, long value);

  public abstract boolean satisfyFloat(long time, float value);

  public abstract boolean satisfyDouble(long time, double value);

  public abstract boolean satisfyBinary(long time, Binary value);

  /**
   * To examine whether the row(with time and values) is satisfied with the filter.
   *
   * @param time row time
   * @param values row values
   * @return true if the row is satisfied with the filter, false otherwise
   */
  public abstract boolean satisfyRow(long time, Object[] values);

  public abstract boolean satisfyBooleanRow(long time, boolean[] values);

  public abstract boolean satisfyIntegerRow(long time, int[] values);

  public abstract boolean satisfyLongRow(long time, long[] values);

  public abstract boolean satisfyFloatRow(long time, float[] values);

  public abstract boolean satisfyDoubleRow(long time, double[] values);

  public abstract boolean satisfyBinaryRow(long time, Binary[] values);

  /**
   * @deprecated this method is out of date. Use {@link #satisfyTsBlock(boolean[],TsBlock)} instead.
   */
  @Deprecated
  public abstract boolean[] satisfyTsBlock(TsBlock tsBlock);

  /**
   * To examine whether the page(with many rows) is satisfied with the filter.
   *
   * @param tsBlock page data
   * @return for each row, true if the row is satisfied with the filter, false otherwise
   */
  public abstract boolean[] satisfyTsBlock(boolean[] selection, TsBlock tsBlock);

  /**
   * To examine whether the block can be skipped.
   *
   * @param metadata the metadata of the block (including the min-max statistics)
   * @return true if the block can be skipped, false otherwise
   */
  public abstract boolean canSkip(IMetadata metadata);

  /**
   * To examine whether the block all satisfied with the filter.
   *
   * @param metadata the metadata of the block (including the min-max statistics)
   * @return true if the block all satisfied with the filter, false otherwise
   */
  public abstract boolean allSatisfy(IMetadata metadata);

  /**
   * To examine whether the min time and max time are satisfied with the filter.
   *
   * <p>Note: only implemented by {@link TimeFilterApi}
   *
   * @param startTime start time of a page, series or device
   * @param endTime end time of a page, series or device
   * @return false if [startTime, endTime] is not satisfied with the filter
   */
  public abstract boolean satisfyStartEndTime(long startTime, long endTime);

  /**
   * To examine whether the partition [startTime, endTime] is subsets of filter.
   *
   * <p>Note: only implemented by {@link TimeFilter}
   *
   * @param startTime start time of a partition
   * @param endTime end time of a partition
   * @return true if the partition [startTime, endTime] is subsets of filter
   */
  public abstract boolean containStartEndTime(long startTime, long endTime);

  /**
   * Return the time ranges which satisfying the filter.
   *
   * <p>Note: only implemented by {@link TimeFilter}
   */
  public abstract List<TimeRange> getTimeRanges();

  /**
   * Reverse the filter.
   *
   * @return the logical inverse filter
   */
  public abstract Filter reverse();

  /**
   * Copy the filter.
   *
   * <p>When the filter is <b>stateless</b>, we can return the filter itself.
   *
   * @return a copy of the filter
   */
  public Filter copy() {
    return this;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // serialize & deserialize related methods
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public abstract OperatorType getOperatorType();

  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(getOperatorType().ordinal(), outputStream);

    // serialize more fields in subclasses
  }

  public void serialize(ByteBuffer buffer) throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    this.serialize(dataOutputStream);
    buffer.put(byteArrayOutputStream.toByteArray());
  }

  public static Filter deserialize(ByteBuffer buffer) {
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
      case VALUE_NEQ:
      case VALUE_GT:
      case VALUE_GTEQ:
      case VALUE_LT:
      case VALUE_LTEQ:
      case VALUE_IS_NULL:
      case VALUE_IS_NOT_NULL:
      case VALUE_IN:
      case VALUE_NOT_IN:
      case VALUE_REGEXP:
      case VALUE_NOT_REGEXP:
      case VALUE_LIKE:
      case VALUE_NOT_LIKE:
      case VALUE_BETWEEN_AND:
      case VALUE_NOT_BETWEEN_AND:
        return FilterDeserialize.deserializeValueFilter(type, buffer);
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
