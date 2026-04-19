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

import org.apache.iotdb.db.queryengine.execution.aggregation.CovarianceAccumulator;

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

public class TableCovarianceAccumulator implements TableAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableCovarianceAccumulator.class);
  private static final int INTERMEDIATE_SIZE = Long.BYTES + 3 * Double.BYTES;

  private final TSDataType xDataType;
  private final TSDataType yDataType;
  private final CovarianceAccumulator.CovarianceType covarianceType;

  private long count;
  private double meanX;
  private double meanY;
  private double c2;

  public TableCovarianceAccumulator(
      TSDataType xDataType,
      TSDataType yDataType,
      CovarianceAccumulator.CovarianceType covarianceType) {
    this.xDataType = xDataType;
    this.yDataType = yDataType;
    this.covarianceType = covarianceType;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new TableCovarianceAccumulator(xDataType, yDataType, covarianceType);
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
            String.format("Unsupported data type in Covariance Aggregation: %s", dataType));
    }
  }

  private void update(double x, double y) {
    long newCount = count + 1;
    double oldMeanX = meanX;
    meanX = oldMeanX + (x - oldMeanX) / newCount;
    double oldMeanY = meanY;
    double newMeanY = oldMeanY + (y - oldMeanY) / newCount;
    meanY = newMeanY;
    c2 += (x - oldMeanX) * (y - newMeanY);
    count = newCount;
  }

  @Override
  public void removeInput(Column[] arguments) {
    throw new UnsupportedOperationException("Remove not implemented for Covariance Accumulator");
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
      double otherC2 = buffer.getDouble();

      merge(otherCount, otherMeanX, otherMeanY, otherC2);
    }
  }

  private void merge(long otherCount, double otherMeanX, double otherMeanY, double otherC2) {
    if (otherCount == 0) {
      return;
    }
    if (count == 0) {
      count = otherCount;
      meanX = otherMeanX;
      meanY = otherMeanY;
      c2 = otherC2;
      return;
    }

    long newCount = count + otherCount;
    double deltaX = otherMeanX - meanX;
    double deltaY = otherMeanY - meanY;

    c2 += otherC2 + deltaX * deltaY * count * otherCount / newCount;
    meanX += deltaX * otherCount / newCount;
    meanY += deltaY * otherCount / newCount;
    count = newCount;
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        "intermediate input and output should be BinaryColumn");

    if (count == 0) {
      columnBuilder.appendNull();
      return;
    }

    byte[] bytes = new byte[INTERMEDIATE_SIZE];
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    buffer.putLong(count);
    buffer.putDouble(meanX);
    buffer.putDouble(meanY);
    buffer.putDouble(c2);
    columnBuilder.writeBinary(new Binary(bytes));
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    switch (covarianceType) {
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
        throw new UnsupportedOperationException("Unknown type: " + covarianceType);
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
    c2 = 0;
  }

  @Override
  public boolean removable() {
    return false;
  }
}
