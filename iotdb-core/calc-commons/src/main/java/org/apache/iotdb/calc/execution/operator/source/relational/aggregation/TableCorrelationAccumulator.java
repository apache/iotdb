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

import org.apache.iotdb.db.queryengine.execution.aggregation.CorrelationAccumulator;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;

public class TableCorrelationAccumulator implements TableAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableCorrelationAccumulator.class);
  private final TSDataType xDataType;
  private final TSDataType yDataType;
  private final CorrelationAccumulator.CorrelationType correlationType;

  private long count;
  private double meanX;
  private double meanY;
  private double m2X;
  private double m2Y;
  private double c2;

  public TableCorrelationAccumulator(
      TSDataType xDataType,
      TSDataType yDataType,
      CorrelationAccumulator.CorrelationType correlationType) {
    this.xDataType = xDataType;
    this.yDataType = yDataType;
    this.correlationType = correlationType;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new TableCorrelationAccumulator(xDataType, yDataType, correlationType);
  }

  @Override
  public void addInput(Column[] arguments, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (arguments[0].isNull(i) || arguments[1].isNull(i)) {
          continue;
        }
        double x = getDoubleValue(arguments[0], i, xDataType);
        double y = getDoubleValue(arguments[1], i, yDataType);
        update(x, y);
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      for (int i = 0; i < positionCount; i++) {
        int position = selectedPositions[i];
        if (arguments[0].isNull(position) || arguments[1].isNull(position)) {
          continue;
        }
        double x = getDoubleValue(arguments[0], position, xDataType);
        double y = getDoubleValue(arguments[1], position, yDataType);
        update(x, y);
      }
    }
  }

  private double getDoubleValue(Column column, int position, TSDataType dataType) {
    switch (dataType) {
      case INT32:
      case DATE:
        return column.getInt(position);
      case INT64:
      case TIMESTAMP:
        return column.getLong(position);
      case FLOAT:
        return column.getFloat(position);
      case DOUBLE:
        return column.getDouble(position);
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in Correlation Aggregation: %s", dataType));
    }
  }

  private void update(double x, double y) {
    long newCount = count + 1;
    double deltaX = x - meanX;
    double deltaY = y - meanY;

    meanX += deltaX / newCount;
    meanY += deltaY / newCount;

    c2 += deltaX * (y - meanY);
    m2X += deltaX * (x - meanX);
    m2Y += deltaY * (y - meanY);

    count = newCount;
  }

  @Override
  public void removeInput(Column[] arguments) {
    throw new UnsupportedOperationException("Remove not implemented for Correlation Accumulator");
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
      ByteBuffer buffer = ByteBuffer.wrap(bytes);

      long otherCount = buffer.getLong();
      double otherMeanX = buffer.getDouble();
      double otherMeanY = buffer.getDouble();
      double otherM2X = buffer.getDouble();
      double otherM2Y = buffer.getDouble();
      double otherC2 = buffer.getDouble();

      merge(otherCount, otherMeanX, otherMeanY, otherM2X, otherM2Y, otherC2);
    }
  }

  private void merge(
      long otherCount,
      double otherMeanX,
      double otherMeanY,
      double otherM2X,
      double otherM2Y,
      double otherC2) {
    if (otherCount == 0) {
      return;
    }
    if (count == 0) {
      count = otherCount;
      meanX = otherMeanX;
      meanY = otherMeanY;
      m2X = otherM2X;
      m2Y = otherM2Y;
      c2 = otherC2;
    } else {
      long newCount = count + otherCount;
      double deltaX = otherMeanX - meanX;
      double deltaY = otherMeanY - meanY;

      c2 += otherC2 + deltaX * deltaY * count * otherCount / newCount;
      m2X += otherM2X + deltaX * deltaX * count * otherCount / newCount;
      m2Y += otherM2Y + deltaY * deltaY * count * otherCount / newCount;

      meanX += deltaX * otherCount / newCount;
      meanY += deltaY * otherCount / newCount;
      count = newCount;
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
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Double.BYTES * 5);
      buffer.putLong(count);
      buffer.putDouble(meanX);
      buffer.putDouble(meanY);
      buffer.putDouble(m2X);
      buffer.putDouble(m2Y);
      buffer.putDouble(c2);
      columnBuilder.writeBinary(new Binary(buffer.array()));
    }
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    switch (correlationType) {
      case CORR:
        if (count < 2) {
          columnBuilder.appendNull();
        } else if (m2X == 0 || m2Y == 0) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.writeDouble(c2 / Math.sqrt(m2X * m2Y));
        }
        break;
      case COVAR_POP:
        if (count == 0) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.writeDouble(c2 / count);
        }
        break;
      case COVAR_SAMP:
        if (count < 2) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.writeDouble(c2 / (count - 1));
        }
        break;
      default:
        throw new UnsupportedOperationException("Unknown type: " + correlationType);
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
    meanX = 0;
    meanY = 0;
    m2X = 0;
    m2Y = 0;
    c2 = 0;
  }

  @Override
  public boolean removable() {
    return false;
  }
}
