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
import org.apache.iotdb.db.utils.TypeServices;
import org.apache.iotdb.db.utils.TypeServices.Aggregation.ExtremeValueAccumulator;
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

import static com.google.common.base.Preconditions.checkArgument;

public class MinValueAccumulator implements Accumulator, ExtremeValueAccumulator {

  private final TSDataType seriesDataType;
  private final TsPrimitiveType minResult;
  private final ExtremeValueAccumulatorStrategy strategy;
  private boolean initResult = false;

  public MinValueAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    final Type type = Type.fromTsDataType(seriesDataType);
    this.minResult = type.getTsPrimitiveType();
    this.strategy = TypeServices.Aggregation.EXTREME_VALUE_ACCUMULATOR_STRATEGY_SERVICE.call(type);
  }

  // Column should be like: | Time | Value |
  @Override
  public void addInput(Column[] columns, BitMap bitMap) {
    ensureSupported();
    strategy.addInput(this, columns, bitMap);
  }

  // partialResult should be like: | partialMinValue1 |
  @Override
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 1, "partialResult of MinValue should be 1");
    if (partialResult[0].isNull(0)) {
      return;
    }
    ensureSupported();
    strategy.addIntermediate(this, partialResult[0]);
  }

  @Override
  public void addStatistics(Statistics statistics) {
    if (statistics == null) {
      return;
    }
    ensureSupported();
    strategy.addStatistics(this, statistics.getMinValue());
  }

  // finalResult should be single column, like: | finalCountValue |
  @Override
  public void setFinal(Column finalResult) {
    if (finalResult.isNull(0)) {
      return;
    }
    initResult = true;
    ensureSupported();
    strategy.setFinal(this, finalResult);
  }

  // columnBuilder should be single in MinValueAccumulator
  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    checkArgument(columnBuilders.length == 1, "partialResult of MinValue should be 1");
    if (!initResult) {
      columnBuilders[0].appendNull();
      return;
    }
    ensureSupported();
    strategy.writeResult(columnBuilders[0], minResult);
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    if (!initResult) {
      columnBuilder.appendNull();
      return;
    }
    ensureSupported();
    strategy.writeResult(columnBuilder, minResult);
  }

  @Override
  public void reset() {
    initResult = false;
    this.minResult.reset();
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {minResult.getDataType()};
  }

  @Override
  public TSDataType getFinalType() {
    return minResult.getDataType();
  }

  @Override
  public TsPrimitiveType getResult() {
    return minResult;
  }

  @Override
  public void updateIntResult(final int value) {
    if (!initResult || value < minResult.getInt()) {
      initResult = true;
      minResult.setInt(value);
    }
  }

  @Override
  public void updateLongResult(final long value) {
    if (!initResult || value < minResult.getLong()) {
      initResult = true;
      minResult.setLong(value);
    }
  }

  @Override
  public void updateFloatResult(final float value) {
    if (!initResult || value < minResult.getFloat()) {
      initResult = true;
      minResult.setFloat(value);
    }
  }

  @Override
  public void updateDoubleResult(final double value) {
    if (!initResult || value < minResult.getDouble()) {
      initResult = true;
      minResult.setDouble(value);
    }
  }

  @Override
  public void updateBinaryResult(final Binary value) {
    if (!initResult || value.compareTo(minResult.getBinary()) < 0) {
      initResult = true;
      minResult.setBinary(value);
    }
  }

  private void ensureSupported() {
    if (!strategy.isSupported()) {
      throw new UnSupportedDataTypeException(
          String.format("Unsupported data type in MinValue: %s", seriesDataType));
    }
  }
}
