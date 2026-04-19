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

import org.apache.iotdb.db.queryengine.execution.aggregation.RegressionAccumulator;

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

public class TableRegressionAccumulator implements TableAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableRegressionAccumulator.class);
  private static final int INTERMEDIATE_SIZE = Long.BYTES + 4 * Double.BYTES;

  private final TSDataType yDataType;
  private final TSDataType xDataType;
  private final RegressionAccumulator.RegressionType regressionType;

  private long count;
  private double meanX;
  private double meanY;
  private double m2X;
  private double c2;

  public TableRegressionAccumulator(
      TSDataType yDataType,
      TSDataType xDataType,
      RegressionAccumulator.RegressionType regressionType) {
    this.yDataType = yDataType;
    this.xDataType = xDataType;
    this.regressionType = regressionType;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new TableRegressionAccumulator(yDataType, xDataType, regressionType);
  }

  @Override
  public void addInput(Column[] arguments, AggregationMask mask) {

    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (arguments[0].isNull(i) || arguments[1].isNull(i)) {
          continue;
        }
        double y = getDoubleValue(arguments[0], i, yDataType);
        double x = getDoubleValue(arguments[1], i, xDataType);
        update(x, y);
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      for (int i = 0; i < positionCount; i++) {
        int position = selectedPositions[i];
        if (arguments[0].isNull(position) || arguments[1].isNull(position)) {
          continue;
        }
        double y = getDoubleValue(arguments[0], position, yDataType);
        double x = getDoubleValue(arguments[1], position, xDataType);
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
            String.format("Unsupported data type in Regression Aggregation: %s", dataType));
    }
  }

  private void update(double x, double y) {
    long n = count + 1;
    double oldMeanX = meanX;
    meanX = oldMeanX + (x - oldMeanX) / n;
    double oldMeanY = meanY;
    double newMeanY = oldMeanY + (y - oldMeanY) / n;
    meanY = newMeanY;
    c2 += (x - oldMeanX) * (y - newMeanY);
    m2X += (x - oldMeanX) * (x - meanX);
    count = n;
  }

  @Override
  public void addIntermediate(Column argument) {
    checkArgument(
        argument instanceof BinaryColumn
            || (argument instanceof RunLengthEncodedColumn
                && ((RunLengthEncodedColumn) argument).getValue() instanceof BinaryColumn));

    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (argument.isNull(i)) continue;
      byte[] bytes = argument.getBinary(i).getValues();
      ByteBuffer buffer = ByteBuffer.wrap(bytes);

      long otherCount = buffer.getLong();
      double otherMeanX = buffer.getDouble();
      double otherMeanY = buffer.getDouble();
      double otherM2X = buffer.getDouble();
      double otherC2 = buffer.getDouble();

      merge(otherCount, otherMeanX, otherMeanY, otherM2X, otherC2);
    }
  }

  private void merge(
      long otherCount, double otherMeanX, double otherMeanY, double otherM2X, double otherC2) {
    if (otherCount == 0) return;
    if (count == 0) {
      count = otherCount;
      meanX = otherMeanX;
      meanY = otherMeanY;
      m2X = otherM2X;
      c2 = otherC2;
    } else {
      long na = count;
      long nb = otherCount;
      long n = na + nb;
      double meanXValue = meanX;
      double meanYValue = meanY;
      double deltaX = otherMeanX - meanXValue;
      double deltaY = otherMeanY - meanYValue;
      m2X += otherM2X + na * nb * deltaX * deltaX / (double) n;
      c2 += otherC2 + deltaX * deltaY * na * nb / (double) n;
      meanX = meanXValue + deltaX * nb / (double) n;
      meanY = meanYValue + deltaY * nb / (double) n;
      count = n;
    }
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        "intermediate output of Regression should be BinaryColumn");
    if (count == 0) {
      columnBuilder.appendNull();
    } else {
      byte[] bytes = new byte[INTERMEDIATE_SIZE];
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      buffer.putLong(count);
      buffer.putDouble(meanX);
      buffer.putDouble(meanY);
      buffer.putDouble(m2X);
      buffer.putDouble(c2);
      columnBuilder.writeBinary(new Binary(bytes));
    }
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    if (count == 0) {
      columnBuilder.appendNull();
      return;
    }
    switch (regressionType) {
      case REGR_SLOPE:
        double slope = c2 / m2X;
        if (Double.isFinite(slope)) {
          columnBuilder.writeDouble(slope);
        } else {
          columnBuilder.appendNull();
        }
        break;
      case REGR_INTERCEPT:
        double intercept = meanY - (c2 / m2X) * meanX;
        if (Double.isFinite(intercept)) {
          columnBuilder.writeDouble(intercept);
        } else {
          columnBuilder.appendNull();
        }
        break;
      default:
        throw new UnsupportedOperationException("Unknown type: " + regressionType);
    }
  }

  @Override
  public void removeInput(Column[] arguments) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public void addStatistics(Statistics[] statistics) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void reset() {
    count = 0;
    meanX = 0;
    meanY = 0;
    m2X = 0;
    c2 = 0;
  }

  @Override
  public boolean removable() {
    return false;
  }
}
