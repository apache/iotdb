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
package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.IntegerStatistics;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import static com.google.common.base.Preconditions.checkArgument;

public class AvgAccumulator implements TableAccumulator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AvgAccumulator.class);
  private final TSDataType argumentDataType;
  private long countValue;
  private double sumValue;
  private boolean initResult = false;

  public AvgAccumulator(TSDataType argumentDataType) {
    this.argumentDataType = argumentDataType;
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
  public void addInput(Column[] arguments) {
    checkArgument(arguments.length == 1, "argument of Avg should be one column");
    switch (argumentDataType) {
      case INT32:
        addIntInput(arguments[0]);
        return;
      case INT64:
        addLongInput(arguments[0]);
        return;
      case FLOAT:
        addFloatInput(arguments[0]);
        return;
      case DOUBLE:
        addDoubleInput(arguments[0]);
        return;
      case TEXT:
      case BLOB:
      case STRING:
      case BOOLEAN:
      case DATE:
      case TIMESTAMP:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in aggregation AVG : %s", argumentDataType));
    }
  }

  @Override
  public void addIntermediate(Column argument) {
    checkArgument(
        argument instanceof BinaryColumn
            || (argument instanceof RunLengthEncodedColumn
                && ((RunLengthEncodedColumn) argument).getValue() instanceof BinaryColumn),
        "intermediate input and output of Avg should be BinaryColumn");

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
        "intermediate input and output of Avg should be BinaryColumn");
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

  private byte[] serializeState() {
    byte[] bytes = new byte[16];
    BytesUtils.longToBytes(countValue, bytes, 0);
    BytesUtils.doubleToBytes(sumValue, bytes, 8);
    return bytes;
  }

  private void addIntInput(Column column) {
    int count = column.getPositionCount();
    for (int i = 0; i < count; i++) {
      if (!column.isNull(i)) {
        initResult = true;
        countValue++;
        sumValue += column.getInt(i);
      }
    }
  }

  private void addLongInput(Column column) {
    int count = column.getPositionCount();
    for (int i = 0; i < count; i++) {
      if (!column.isNull(i)) {
        initResult = true;
        countValue++;
        sumValue += column.getLong(i);
      }
    }
  }

  private void addFloatInput(Column column) {
    int count = column.getPositionCount();
    for (int i = 0; i < count; i++) {
      if (!column.isNull(i)) {
        initResult = true;
        countValue++;
        sumValue += column.getFloat(i);
      }
    }
  }

  private void addDoubleInput(Column column) {
    int count = column.getPositionCount();
    for (int i = 0; i < count; i++) {
      if (!column.isNull(i)) {
        initResult = true;
        countValue++;
        sumValue += column.getDouble(i);
      }
    }
  }
}
