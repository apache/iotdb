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
package org.apache.iotdb.calc.execution.operator.source.relational.aggregation;

import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.calc.utils.TypeServices;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.IntegerStatistics;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.RamUsageEstimator;

import static com.google.common.base.Preconditions.checkArgument;

public class AvgAccumulator implements TableAccumulator, TypeServices.AvgState {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AvgAccumulator.class);
  private final TSDataType argumentDataType;
  private final TypeServices.AvgInputStrategy inputStrategy;
  private long countValue;
  private double sumValue;
  private boolean initResult = false;

  public AvgAccumulator(TSDataType argumentDataType) {
    this.argumentDataType = argumentDataType;
    Type type = Type.fromTsDataType(argumentDataType);
    this.inputStrategy = TypeServices.AVG_INPUT_STRATEGY_SERVICE.call(type);
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new AvgAccumulator(argumentDataType);
  }

  @Override
  public void addInput(Column[] arguments, AggregationMask mask) {
    checkArgument(
        arguments.length == 1,
        CalcMessages.EXCEPTION_ARGUMENT_OF_AVG_SHOULD_BE_ONE_COLUMN_82162B82);
    inputStrategy.addInput(this, arguments[0], mask);
  }

  @Override
  public void removeInput(Column[] arguments) {
    checkArgument(
        arguments.length == 1,
        CalcMessages.EXCEPTION_ARGUMENT_OF_AVG_SHOULD_BE_ONE_COLUMN_82162B82);
    inputStrategy.removeInput(this, arguments[0]);
  }

  @Override
  public void addIntermediate(Column argument) {
    checkArgument(
        argument instanceof BinaryColumn
            || (argument instanceof RunLengthEncodedColumn
                && ((RunLengthEncodedColumn) argument).getValue() instanceof BinaryColumn),
        CalcMessages
            .EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_AVG_SHOULD_BE_BINARYCOLUMN_9CCDD5EB);

    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (argument.isNull(i)) {
        continue;
      }

      initResult = true;
      long midCountValue = BytesUtils.bytesToLong(argument.getBinary(i).getValues(), 8);
      double midSumValue = BytesUtils.bytesToDouble(argument.getBinary(i).getValues(), 8);
      countValue += midCountValue;
      sumValue += midSumValue;
    }
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        CalcMessages
            .EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_AVG_SHOULD_BE_BINARYCOLUMN_9CCDD5EB);
    if (!initResult) {
      columnBuilder.appendNull();
    } else {
      columnBuilder.writeBinary(new Binary(serializeState()));
    }
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    if (!initResult) {
      columnBuilder.appendNull();
    } else {
      columnBuilder.writeDouble(sumValue / countValue);
    }
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public void addStatistics(Statistics[] statistics) {
    if (statistics == null || statistics[0] == null) {
      return;
    }
    initResult = true;
    countValue += statistics[0].getCount();
    if (statistics[0] instanceof IntegerStatistics) {
      sumValue += statistics[0].getSumLongValue();
    } else {
      sumValue += statistics[0].getSumDoubleValue();
    }
    if (countValue == 0) {
      initResult = false;
    }
  }

  @Override
  public void reset() {
    this.initResult = false;
    this.countValue = 0;
    this.sumValue = 0.0;
  }

  @Override
  public boolean removable() {
    return true;
  }

  private byte[] serializeState() {
    byte[] bytes = new byte[16];
    BytesUtils.longToBytes(countValue, bytes, 0);
    BytesUtils.doubleToBytes(sumValue, bytes, 8);
    return bytes;
  }

  @Override
  public double getSumValue() {
    return sumValue;
  }

  @Override
  public long getCountValue() {
    return countValue;
  }

  @Override
  public void updateAvg(double sum, long count, boolean initialized) {
    sumValue = sum;
    countValue = count;
    initResult |= initialized;
  }
}
