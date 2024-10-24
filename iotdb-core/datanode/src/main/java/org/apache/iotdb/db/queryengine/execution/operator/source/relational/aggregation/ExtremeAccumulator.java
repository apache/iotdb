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

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

public class ExtremeAccumulator implements TableAccumulator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ExtremeAccumulator.class);
  private static final String UNSUPPORTED_DATA_TYPE = "Unsupported data type in Extreme: %s";
  private final TSDataType seriesDataType;
  private final TsPrimitiveType extremeResult;
  private boolean initResult;

  public ExtremeAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    this.extremeResult = TsPrimitiveType.getByType(seriesDataType);
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new ExtremeAccumulator(seriesDataType);
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
      case STRING:
      case BLOB:
      case BOOLEAN:
      case DATE:
      case TIMESTAMP:
      default:
        throw new UnSupportedDataTypeException(
            String.format(UNSUPPORTED_DATA_TYPE, seriesDataType));
    }
  }

  @Override
  public void addIntermediate(Column argument) {
    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (argument.isNull(i)) {
        continue;
      }

      switch (seriesDataType) {
        case INT32:
          updateIntResult(argument.getInt(i));
          break;
        case INT64:
          updateLongResult(argument.getLong(i));
          break;
        case FLOAT:
          updateFloatResult(argument.getFloat(i));
          break;
        case DOUBLE:
          updateDoubleResult(argument.getDouble(i));
          break;
        case TEXT:
        case STRING:
        case BLOB:
        case BOOLEAN:
        case DATE:
        case TIMESTAMP:
        default:
          throw new UnSupportedDataTypeException(
              String.format(UNSUPPORTED_DATA_TYPE, seriesDataType));
      }
    }
  }

  @Override
  public void addStatistics(Statistics[] statistics) {
    if (statistics == null || statistics[0] == null) {
      return;
    }

    switch (seriesDataType) {
      case INT32:
        updateIntResult((int) statistics[0].getMaxValue());
        updateIntResult((int) statistics[0].getMinValue());
        break;
      case INT64:
        updateLongResult((long) statistics[0].getMaxValue());
        updateLongResult((long) statistics[0].getMinValue());
        break;
      case FLOAT:
        updateFloatResult((float) statistics[0].getMaxValue());
        updateFloatResult((float) statistics[0].getMinValue());
        break;
      case DOUBLE:
        updateDoubleResult((double) statistics[0].getMaxValue());
        updateDoubleResult((double) statistics[0].getMinValue());
        break;
      case TEXT:
      case STRING:
      case BLOB:
      case BOOLEAN:
      case DATE:
      case TIMESTAMP:
      default:
        throw new UnSupportedDataTypeException(
            String.format(UNSUPPORTED_DATA_TYPE, seriesDataType));
    }
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    if (!initResult) {
      columnBuilder.appendNull();
      return;
    }

    switch (seriesDataType) {
      case INT32:
        columnBuilder.writeInt(extremeResult.getInt());
        break;
      case INT64:
        columnBuilder.writeLong(extremeResult.getLong());
        break;
      case FLOAT:
        columnBuilder.writeFloat(extremeResult.getFloat());
        break;
      case DOUBLE:
        columnBuilder.writeDouble(extremeResult.getDouble());
        break;
      case TEXT:
      case STRING:
      case BLOB:
      case BOOLEAN:
      case DATE:
      case TIMESTAMP:
      default:
        throw new UnSupportedDataTypeException(
            String.format(UNSUPPORTED_DATA_TYPE, seriesDataType));
    }
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    if (!initResult) {
      columnBuilder.appendNull();
      return;
    }

    switch (seriesDataType) {
      case INT32:
        columnBuilder.writeInt(extremeResult.getInt());
        break;
      case INT64:
        columnBuilder.writeLong(extremeResult.getLong());
        break;
      case FLOAT:
        columnBuilder.writeFloat(extremeResult.getFloat());
        break;
      case DOUBLE:
        columnBuilder.writeDouble(extremeResult.getDouble());
        break;
      case TEXT:
      case STRING:
      case BLOB:
      case BOOLEAN:
      case DATE:
      case TIMESTAMP:
      default:
        throw new UnSupportedDataTypeException(
            String.format(UNSUPPORTED_DATA_TYPE, seriesDataType));
    }
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

  private void addIntInput(Column column) {
    for (int i = 0; i < column.getPositionCount(); i++) {
      if (!column.isNull(i)) {
        updateIntResult(column.getInt(i));
      }
    }
  }

  private void updateIntResult(int val) {
    int absExtVal = Math.abs(val);
    int candidateResult = extremeResult.getInt();
    int absCandidateResult = Math.abs(extremeResult.getInt());

    if (!initResult
        || (absExtVal > absCandidateResult)
        || (absExtVal == absCandidateResult) && val > candidateResult) {
      initResult = true;
      extremeResult.setInt(val);
    }
  }

  private void addLongInput(Column column) {
    for (int i = 0; i < column.getPositionCount(); i++) {
      if (!column.isNull(i)) {
        updateLongResult(column.getLong(i));
      }
    }
  }

  private void updateLongResult(long val) {
    long absExtVal = Math.abs(val);
    long candidateResult = extremeResult.getLong();
    long absCandidateResult = Math.abs(extremeResult.getLong());

    if (!initResult
        || (absExtVal > absCandidateResult)
        || (absExtVal == absCandidateResult) && val > candidateResult) {
      initResult = true;
      extremeResult.setLong(val);
    }
  }

  private void addFloatInput(Column column) {
    for (int i = 0; i < column.getPositionCount(); i++) {
      if (!column.isNull(i)) {
        updateFloatResult(column.getFloat(i));
      }
    }
  }

  private void updateFloatResult(float val) {
    float absExtVal = Math.abs(val);
    float candidateResult = extremeResult.getFloat();
    float absCandidateResult = Math.abs(extremeResult.getFloat());

    if (!initResult
        || (absExtVal > absCandidateResult)
        || (absExtVal == absCandidateResult) && val > candidateResult) {
      initResult = true;
      extremeResult.setFloat(val);
    }
  }

  private void addDoubleInput(Column column) {
    for (int i = 0; i < column.getPositionCount(); i++) {
      if (!column.isNull(i)) {
        updateDoubleResult(column.getDouble(i));
      }
    }
  }

  private void updateDoubleResult(double val) {
    double absExtVal = Math.abs(val);
    double candidateResult = extremeResult.getDouble();
    double absCandidateResult = Math.abs(extremeResult.getDouble());

    if (!initResult
        || (absExtVal > absCandidateResult)
        || (absExtVal == absCandidateResult) && val > candidateResult) {
      initResult = true;
      extremeResult.setDouble(val);
    }
  }
}
