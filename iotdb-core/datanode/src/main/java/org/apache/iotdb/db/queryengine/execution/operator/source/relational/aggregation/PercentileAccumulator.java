/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.Percentile;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.nio.ByteBuffer;

public class PercentileAccumulator implements TableAccumulator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(PercentileAccumulator.class);

  private final TSDataType seriesDataType;
  private Percentile percentile = new Percentile();
  private double percentage;

  public PercentileAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new PercentileAccumulator(seriesDataType);
  }

  @Override
  public void addInput(Column[] arguments, AggregationMask mask) {
    if (arguments.length != 2) {
      throw new SemanticException(
          String.format("PERCENTILE requires 2 arguments, but got %d", arguments.length));
    }
    percentage = arguments[1].getDouble(0);
    switch (seriesDataType) {
      case INT32:
        addIntInput(arguments[0], mask);
        return;
      case INT64:
      case TIMESTAMP:
        addLongInput(arguments[0], mask);
        return;
      case FLOAT:
        addFloatInput(arguments[0], mask);
        return;
      case DOUBLE:
        addDoubleInput(arguments[0], mask);
        return;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in Percentile Aggregation: %s", seriesDataType));
    }
  }

  @Override
  public void addIntermediate(Column argument) {
    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (!argument.isNull(i)) {
        byte[] data = argument.getBinary(i).getValues();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        this.percentage = ReadWriteIOUtils.readDouble(buffer);
        percentile.merge(Percentile.deserialize(buffer));
      }
    }
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    int percentileDataLength = percentile.getSerializedSize();
    ByteBuffer buffer = ByteBuffer.allocate(8 + percentileDataLength);
    ReadWriteIOUtils.write(percentage, buffer);
    percentile.serialize(buffer);
    columnBuilder.writeBinary(new Binary(buffer.array()));
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    double result = percentile.getPercentile(percentage);
    if (Double.isNaN(result)) {
      columnBuilder.appendNull();
      return;
    }
    switch (seriesDataType) {
      case INT32:
        columnBuilder.writeInt((int) result);
        break;
      case INT64:
      case TIMESTAMP:
        columnBuilder.writeLong((long) result);
        break;
      case FLOAT:
        columnBuilder.writeFloat((float) result);
        break;
      case DOUBLE:
        columnBuilder.writeDouble(result);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in PERCENTILE Aggregation: %s", seriesDataType));
    }
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public void addStatistics(Statistics[] statistics) {
    throw new UnsupportedOperationException("PercentileAccumulator does not support statistics");
  }

  @Override
  public void reset() {
    percentile.clear();
  }

  private void addIntInput(Column column, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          percentile.addValue(column.getInt(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!column.isNull(position)) {
          percentile.addValue(column.getInt(position));
        }
      }
    }
  }

  private void addLongInput(Column column, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          percentile.addValue(column.getLong(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!column.isNull(position)) {
          percentile.addValue(column.getLong(position));
        }
      }
    }
  }

  private void addFloatInput(Column column, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          percentile.addValue(column.getFloat(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!column.isNull(position)) {
          percentile.addValue(column.getFloat(position));
        }
      }
    }
  }

  private void addDoubleInput(Column column, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          percentile.addValue(column.getDouble(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!column.isNull(position)) {
          percentile.addValue(column.getDouble(position));
        }
      }
    }
  }
}
