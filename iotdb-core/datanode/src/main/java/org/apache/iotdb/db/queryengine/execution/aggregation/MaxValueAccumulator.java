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

public class MaxValueAccumulator implements Accumulator, ExtremeValueAccumulator {

  private final TSDataType seriesDataType;
  private final TsPrimitiveType maxResult;
  private final ExtremeValueAccumulatorStrategy strategy;
  private boolean initResult;

  public MaxValueAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    final Type type = Type.fromTsDataType(seriesDataType);
    this.maxResult = type.getTsPrimitiveType();
    this.strategy = TypeServices.Aggregation.EXTREME_VALUE_ACCUMULATOR_STRATEGY_SERVICE.call(type);
  }

  // Column should be like: | Time | Value |
  @Override
  public void addInput(Column[] columns, BitMap bitMap) {
    ensureSupported();
    strategy.addInput(this, columns, bitMap);
  }

  // partialResult should be like: | partialMaxValue1 |
  @Override
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 1, "partialResult of MaxValue should be 1");
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
    strategy.addStatistics(this, statistics.getMaxValue());
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

  // columnBuilder should be single in countAccumulator
  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    checkArgument(columnBuilders.length == 1, "partialResult of MaxValue should be 1");
    if (!initResult) {
      columnBuilders[0].appendNull();
      return;
    }
    ensureSupported();
    strategy.writeResult(columnBuilders[0], maxResult);
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    if (!initResult) {
      columnBuilder.appendNull();
      return;
    }
    ensureSupported();
    strategy.writeResult(columnBuilder, maxResult);
  }

  @Override
  public void reset() {
    initResult = false;
    this.maxResult.reset();
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {maxResult.getDataType()};
  }

  @Override
  public TSDataType getFinalType() {
    return maxResult.getDataType();
  }

  @Override
  public TsPrimitiveType getResult() {
    return maxResult;
  }

  @Override
  public void updateIntResult(final int value) {
    if (!initResult || value > maxResult.getInt()) {
      initResult = true;
      maxResult.setInt(value);
    }
  }

  @Override
  public void updateLongResult(final long value) {
    if (!initResult || value > maxResult.getLong()) {
      initResult = true;
      maxResult.setLong(value);
    }
  }

  @Override
  public void updateFloatResult(final float value) {
    if (!initResult || value > maxResult.getFloat()) {
      initResult = true;
      maxResult.setFloat(value);
    }
  }

  @Override
  public void updateDoubleResult(final double value) {
    if (!initResult || value > maxResult.getDouble()) {
      initResult = true;
      maxResult.setDouble(value);
    }
  }

  @Override
  public void updateBinaryResult(final Binary value) {
    if (!initResult || value.compareTo(maxResult.getBinary()) > 0) {
      initResult = true;
      maxResult.setBinary(value);
    }
  }

  private void ensureSupported() {
    if (!strategy.isSupported()) {
      throw new UnSupportedDataTypeException(
          String.format("Unsupported data type in MaxValue: %s", seriesDataType));
    }
  }
}
