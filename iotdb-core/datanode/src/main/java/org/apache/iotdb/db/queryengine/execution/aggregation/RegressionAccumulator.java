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

package org.apache.iotdb.db.queryengine.execution.aggregation;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;

public class RegressionAccumulator implements Accumulator {

  private static final int INTERMEDIATE_SIZE = Long.BYTES + 4 * Double.BYTES;

  public enum RegressionType {
    REGR_SLOPE,
    REGR_INTERCEPT
  }

  private final TSDataType[] seriesDataTypes;
  private final RegressionType regressionType;

  private long count;
  private double meanX;
  private double meanY;
  private double m2X;
  private double c2;

  public RegressionAccumulator(TSDataType[] seriesDataTypes, RegressionType regressionType) {
    this.seriesDataTypes = seriesDataTypes;
    this.regressionType = regressionType;
  }

  @Override
  public void addInput(Column[] columns, BitMap bitMap) {

    int size = columns[1].getPositionCount();
    for (int i = 0; i < size; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (columns[1].isNull(i) || columns[2].isNull(i)) {
        continue;
      }

      double y = getDoubleValue(columns[1], i, seriesDataTypes[0]);
      double x = getDoubleValue(columns[2], i, seriesDataTypes[1]);

      update(x, y);
    }
  }

  private double getDoubleValue(Column column, int position, TSDataType dataType) {
    switch (dataType) {
      case INT32:
        return column.getInt(position);
      case INT64:
      case TIMESTAMP:
        return column.getLong(position);
      case FLOAT:
        return column.getFloat(position);
      case DOUBLE:
        return column.getDouble(position);
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
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
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 1, "partialResult of Regression should be 1");
    if (partialResult[0].isNull(0)) {
      return;
    }
    byte[] bytes = partialResult[0].getBinary(0).getValues();
    ByteBuffer buffer = ByteBuffer.wrap(bytes);

    long otherCount = buffer.getLong();
    double otherMeanX = buffer.getDouble();
    double otherMeanY = buffer.getDouble();
    double otherM2X = buffer.getDouble();
    double otherC2 = buffer.getDouble();

    merge(otherCount, otherMeanX, otherMeanY, otherM2X, otherC2);
  }

  private void merge(
      long otherCount, double otherMeanX, double otherMeanY, double otherM2X, double otherC2) {
    if (otherCount == 0) {
      return;
    }
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
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    checkArgument(columnBuilders.length == 1, "partialResult of Regression should be 1");
    if (count == 0) {
      columnBuilders[0].appendNull();
    } else {
      byte[] bytes = new byte[INTERMEDIATE_SIZE];
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      buffer.putLong(count);
      buffer.putDouble(meanX);
      buffer.putDouble(meanY);
      buffer.putDouble(m2X);
      buffer.putDouble(c2);
      columnBuilders[0].writeBinary(new Binary(bytes));
    }
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
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
  public void removeIntermediate(Column[] input) {
    checkArgument(input.length == 1, "Input of Regression should be 1");
    if (input[0].isNull(0)) {
      return;
    }

    byte[] bytes = input[0].getBinary(0).getValues();
    ByteBuffer buffer = ByteBuffer.wrap(bytes);

    long otherCount = buffer.getLong();
    if (otherCount == 0) {
      return;
    }
    checkArgument(
        count >= otherCount, "Regression state count is smaller than removed state count");

    if (count == otherCount) {
      reset();
      return;
    }

    double otherMeanX = buffer.getDouble();
    double otherMeanY = buffer.getDouble();
    double otherM2X = buffer.getDouble();
    double otherC2 = buffer.getDouble();

    long totalCount = count;
    long newCount = totalCount - otherCount;

    double newMeanX = ((double) totalCount * meanX - (double) otherCount * otherMeanX) / newCount;
    double newMeanY = ((double) totalCount * meanY - (double) otherCount * otherMeanY) / newCount;

    double deltaX = otherMeanX - newMeanX;
    double deltaY = otherMeanY - newMeanY;
    double correction = ((double) newCount * otherCount) / totalCount;

    c2 = c2 - otherC2 - deltaX * deltaY * correction;
    m2X = m2X - otherM2X - deltaX * deltaX * correction;

    meanX = newMeanX;
    meanY = newMeanY;
    count = newCount;
  }

  @Override
  public void addStatistics(Statistics statistics) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public void setFinal(Column finalResult) {}

  @Override
  public void reset() {
    count = 0;
    meanX = 0;
    meanY = 0;
    m2X = 0;
    c2 = 0;
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {TSDataType.TEXT};
  }

  @Override
  public TSDataType getFinalType() {
    return TSDataType.DOUBLE;
  }
}
