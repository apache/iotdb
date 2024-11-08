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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation;

import org.apache.iotdb.db.queryengine.execution.aggregation.VarianceAccumulator;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import static com.google.common.base.Preconditions.checkArgument;

public class TableVarianceAccumulator implements TableAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableVarianceAccumulator.class);
  private final TSDataType seriesDataType;
  private final VarianceAccumulator.VarianceType varianceType;

  private long count;
  private double mean;
  private double m2;

  public TableVarianceAccumulator(
      TSDataType seriesDataType, VarianceAccumulator.VarianceType varianceType) {
    this.seriesDataType = seriesDataType;
    this.varianceType = varianceType;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new TableVarianceAccumulator(seriesDataType, varianceType);
  }

  @Override
  public void addInput(Column[] arguments) {
    switch (seriesDataType) {
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
      case BOOLEAN:
      case DATE:
      case STRING:
      case TIMESTAMP:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in aggregation variance : %s", seriesDataType));
    }
  }

  @Override
  public void addIntermediate(Column argument) {
    checkArgument(
        argument instanceof BinaryColumn
            || (argument instanceof RunLengthEncodedColumn
                && ((RunLengthEncodedColumn) argument).getValue() instanceof BinaryColumn),
        "intermediate input and output should be BinaryColumn");

    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (argument.isNull(i)) {
        continue;
      }

      byte[] bytes = argument.getBinary(i).getValues();
      long intermediateCount = BytesUtils.bytesToLong(bytes, Long.BYTES);
      double intermediateMean = BytesUtils.bytesToDouble(bytes, Long.BYTES);
      double intermediateM2 = BytesUtils.bytesToDouble(bytes, (Long.BYTES + Double.BYTES));

      long newCount = count + intermediateCount;
      double newMean = ((intermediateCount * intermediateMean) + (count * mean)) / newCount;
      double delta = intermediateMean - mean;

      m2 = m2 + intermediateM2 + delta * delta * intermediateCount * count / newCount;
      count = newCount;
      mean = newMean;
    }
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        "intermediate input and output should be BinaryColumn");

    if (count == 0) {
      columnBuilder.appendNull();
    } else {
      byte[] bytes = new byte[24];
      BytesUtils.longToBytes(count, bytes, 0);
      BytesUtils.doubleToBytes(mean, bytes, Long.BYTES);
      BytesUtils.doubleToBytes(m2, bytes, Long.BYTES + Double.BYTES);
      columnBuilder.writeBinary(new Binary(bytes));
    }
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    switch (varianceType) {
      case STDDEV_POP:
        if (count == 0) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.writeDouble(Math.sqrt(m2 / count));
        }
        break;
      case STDDEV_SAMP:
        if (count < 2) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.writeDouble(Math.sqrt(m2 / (count - 1)));
        }
        break;
      case VAR_POP:
        if (count == 0) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.writeDouble(m2 / count);
        }
        break;
      case VAR_SAMP:
        if (count < 2) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.writeDouble(m2 / (count - 1));
        }
        break;
      default:
        throw new EnumConstantNotPresentException(
            VarianceAccumulator.VarianceType.class, varianceType.name());
    }
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public void addStatistics(Statistics[] statistics) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public void reset() {
    count = 0;
    mean = 0.0;
    m2 = 0.0;
  }

  private void addIntInput(Column column) {
    for (int i = 0; i < column.getPositionCount(); i++) {
      if (column.isNull(i)) {
        continue;
      }

      int value = column.getInt(i);
      count++;
      double delta = value - mean;
      mean += delta / count;
      m2 += delta * (value - mean);
    }
  }

  private void addLongInput(Column column) {
    for (int i = 0; i < column.getPositionCount(); i++) {
      if (column.isNull(i)) {
        continue;
      }

      long value = column.getLong(i);
      count++;
      double delta = value - mean;
      mean += delta / count;
      m2 += delta * (value - mean);
    }
  }

  private void addFloatInput(Column column) {
    for (int i = 0; i < column.getPositionCount(); i++) {
      if (column.isNull(i)) {
        continue;
      }

      float value = column.getFloat(i);
      count++;
      double delta = value - mean;
      mean += delta / count;
      m2 += delta * (value - mean);
    }
  }

  private void addDoubleInput(Column column) {
    for (int i = 0; i < column.getPositionCount(); i++) {
      if (column.isNull(i)) {
        continue;
      }

      double value = column.getDouble(i);
      count++;
      double delta = value - mean;
      mean += delta / count;
      m2 += delta * (value - mean);
    }
  }
}
