/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.aggregation;

import org.apache.iotdb.calc.execution.aggregation.Accumulator;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.utils.TypeServices;
import org.apache.iotdb.db.utils.TypeServices.Aggregation.ExtremeValueAccumulatorStrategy;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.util.EnumSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public class ExtremeAccumulator
    implements Accumulator, TypeServices.Aggregation.ExtremeValueAccumulator {

  private static final Set<TSDataType> SUPPORTED_TYPES =
      EnumSet.of(TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE);

  private final TSDataType seriesDataType;
  private final TsPrimitiveType extremeResult;
  private final ExtremeValueAccumulatorStrategy strategy;
  private boolean initResult;

  public ExtremeAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    final Type type = Type.fromTsDataType(seriesDataType);
    this.extremeResult = type.getTsPrimitiveType();
    this.strategy = TypeServices.Aggregation.EXTREME_VALUE_ACCUMULATOR_STRATEGY_SERVICE.call(type);
    ensureSupported();
  }

  @Override
  public void addInput(Column[] columns, BitMap bitMap) {
    strategy.addInput(this, columns, bitMap);
  }

  // partialResult should be like: | PartialExtremeValue |
  @Override
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 1, "partialResult of ExtremeValue should be 1");
    if (partialResult[0].isNull(0)) {
      return;
    }
    strategy.addIntermediate(this, partialResult[0]);
  }

  @Override
  public void addStatistics(Statistics statistics) {
    if (statistics == null) {
      return;
    }
    strategy.addStatistics(this, statistics.getMaxValue());
    strategy.addStatistics(this, statistics.getMinValue());
  }

  @Override
  public void setFinal(Column finalResult) {
    if (finalResult.isNull(0)) {
      return;
    }
    initResult = true;
    strategy.setFinal(this, finalResult);
  }

  // columnBuilder should be single in ExtremeAccumulator
  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    checkArgument(columnBuilders.length == 1, "partialResult of ExtremeValue should be 1");
    if (!initResult) {
      columnBuilders[0].appendNull();
      return;
    }
    strategy.writeResult(columnBuilders[0], extremeResult);
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    if (!initResult) {
      columnBuilder.appendNull();
      return;
    }
    strategy.writeResult(columnBuilder, extremeResult);
  }

  @Override
  public void reset() {
    initResult = false;
    extremeResult.reset();
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {extremeResult.getDataType()};
  }

  @Override
  public TSDataType getFinalType() {
    return extremeResult.getDataType();
  }

  @Override
  public TsPrimitiveType getResult() {
    return extremeResult;
  }

  @Override
  public void updateIntResult(final int extVal) {
    int candidateResult = extremeResult.getInt();

    if (!initResult || compareExtreme(extVal, candidateResult) > 0) {
      initResult = true;
      extremeResult.setInt(extVal);
    }
  }

  @Override
  public void updateLongResult(final long extVal) {
    long candidateResult = extremeResult.getLong();

    if (!initResult || compareExtreme(extVal, candidateResult) > 0) {
      initResult = true;
      extremeResult.setLong(extVal);
    }
  }

  @Override
  public void updateFloatResult(final float extVal) {
    float absExtVal = Math.abs(extVal);
    float candidateResult = extremeResult.getFloat();
    float absCandidateResult = Math.abs(extremeResult.getFloat());

    if (!initResult
        || (absExtVal > absCandidateResult)
        || (absExtVal == absCandidateResult) && extVal > candidateResult) {
      initResult = true;
      extremeResult.setFloat(extVal);
    }
  }

  @Override
  public void updateDoubleResult(final double extVal) {
    double absExtVal = Math.abs(extVal);
    double candidateResult = extremeResult.getDouble();
    double absCandidateResult = Math.abs(extremeResult.getDouble());

    if (!initResult
        || (absExtVal > absCandidateResult)
        || (absExtVal == absCandidateResult) && extVal > candidateResult) {
      initResult = true;
      extremeResult.setDouble(extVal);
    }
  }

  @Override
  public void updateBinaryResult(final Binary value) {
    throw unsupportedDataTypeException();
  }

  private void ensureSupported() {
    if (!SUPPORTED_TYPES.contains(seriesDataType) || !strategy.isSupported()) {
      throw unsupportedDataTypeException();
    }
  }

  private UnSupportedDataTypeException unsupportedDataTypeException() {
    return new UnSupportedDataTypeException(
        String.format(DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE_FMT, seriesDataType));
  }

  private int compareExtreme(int left, int right) {
    int absComparison = Long.compare(Math.abs((long) left), Math.abs((long) right));
    return absComparison == 0 ? Integer.compare(left, right) : absComparison;
  }

  private int compareExtreme(long left, long right) {
    int absComparison = compareAbs(left, right);
    return absComparison == 0 ? Long.compare(left, right) : absComparison;
  }

  private int compareAbs(long left, long right) {
    if (left == Long.MIN_VALUE) {
      return right == Long.MIN_VALUE ? 0 : 1;
    }
    if (right == Long.MIN_VALUE) {
      return -1;
    }
    return Long.compare(Math.abs(left), Math.abs(right));
  }
}
