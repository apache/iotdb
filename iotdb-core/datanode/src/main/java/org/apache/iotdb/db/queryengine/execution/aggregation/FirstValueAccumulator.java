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
import org.apache.iotdb.db.utils.TypeServices.Aggregation.TimeValueAccumulatorStrategy;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import static com.google.common.base.Preconditions.checkArgument;

public class FirstValueAccumulator
    implements Accumulator, TypeServices.Aggregation.TimeValueAccumulator {

  protected final TSDataType seriesDataType;
  protected boolean hasCandidateResult;
  protected TsPrimitiveType firstValue;
  private final TimeValueAccumulatorStrategy strategy;
  protected long minTime = Long.MAX_VALUE;

  public FirstValueAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    final Type type = Type.fromTsDataType(seriesDataType);
    firstValue = type.getTsPrimitiveType();
    strategy = TypeServices.Aggregation.TIME_VALUE_ACCUMULATOR_STRATEGY_SERVICE.call(type);
    ensureSupported();
  }

  // Column should be like: | Time | Value |
  @Override
  public void addInput(Column[] columns, BitMap bitMap) {
    addInputWithStrategy(columns, bitMap, true, true);
  }

  protected final void addInputWithStrategy(
      final Column[] columns,
      final BitMap bitMap,
      final boolean stopAfterFirstValue,
      final boolean bitmapMarksSelected) {
    strategy.addInput(this, columns, bitMap, stopAfterFirstValue, bitmapMarksSelected);
  }

  // partialResult should be like: | FirstValue | MinTime |
  @Override
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 2, "partialResult of FirstValue should be 2");
    if (partialResult[0].isNull(0)) {
      return;
    }
    strategy.addIntermediate(this, partialResult[0], partialResult[1]);
  }

  @Override
  public void addStatistics(Statistics statistics) {
    if (statistics == null) {
      return;
    }
    strategy.addStatistics(this, statistics, true);
  }

  // finalResult should be single column, like: | finalFirstValue |
  @Override
  public void setFinal(Column finalResult) {
    reset();
    if (!finalResult.isNull(0)) {
      hasCandidateResult = true;
      strategy.setFinal(this, finalResult);
    }
  }

  // columnBuilder should be double in FirstValueAccumulator
  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    checkArgument(columnBuilders.length == 2, "partialResult of FirstValue should be 2");
    if (!hasCandidateResult) {
      columnBuilders[0].appendNull();
      columnBuilders[1].appendNull();
      return;
    }
    strategy.writeResult(columnBuilders[0], firstValue);
    columnBuilders[1].writeLong(minTime);
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    if (!hasCandidateResult) {
      columnBuilder.appendNull();
      return;
    }
    strategy.writeResult(columnBuilder, firstValue);
  }

  @Override
  public void reset() {
    hasCandidateResult = false;
    this.minTime = Long.MAX_VALUE;
    this.firstValue.reset();
  }

  @Override
  public boolean hasFinalResult() {
    return hasCandidateResult;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {firstValue.getDataType(), TSDataType.INT64};
  }

  @Override
  public TSDataType getFinalType() {
    return firstValue.getDataType();
  }

  @Override
  public int getPartialResultSize() {
    return 2;
  }

  @Override
  public TsPrimitiveType getTimeValueResult() {
    return firstValue;
  }

  @Override
  public void updateIntResult(final int value, final long curTime) {
    hasCandidateResult = true;
    if (curTime < minTime) {
      minTime = curTime;
      firstValue.setInt(value);
    }
  }

  @Override
  public void updateLongResult(final long value, final long curTime) {
    hasCandidateResult = true;
    if (curTime < minTime) {
      minTime = curTime;
      firstValue.setLong(value);
    }
  }

  @Override
  public void updateFloatResult(final float value, final long curTime) {
    hasCandidateResult = true;
    if (curTime < minTime) {
      minTime = curTime;
      firstValue.setFloat(value);
    }
  }

  @Override
  public void updateDoubleResult(final double value, final long curTime) {
    hasCandidateResult = true;
    if (curTime < minTime) {
      minTime = curTime;
      firstValue.setDouble(value);
    }
  }

  @Override
  public void updateBooleanResult(final boolean value, final long curTime) {
    hasCandidateResult = true;
    if (curTime < minTime) {
      minTime = curTime;
      firstValue.setBoolean(value);
    }
  }

  @Override
  public void updateBinaryResult(final Binary value, final long curTime) {
    hasCandidateResult = true;
    if (curTime < minTime) {
      minTime = curTime;
      firstValue.setBinary(value);
    }
  }

  private void ensureSupported() {
    if (!strategy.isSupported()) {
      throw new UnSupportedDataTypeException(
          String.format(DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE_FMT, seriesDataType));
    }
  }
}
