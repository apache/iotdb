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
    long newCount = count + 1;
    double deltaX = x - meanX;
    double deltaY = y - meanY;

    meanX += deltaX / newCount;
    meanY += deltaY / newCount;

    c2 += deltaX * (y - meanY);
    m2X += deltaX * (x - meanX);

    count = newCount;
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
      long newCount = count + otherCount;
      double deltaX = otherMeanX - meanX;
      double deltaY = otherMeanY - meanY;

      c2 += otherC2 + deltaX * deltaY * count * otherCount / newCount;
      m2X += otherM2X + deltaX * deltaX * count * otherCount / newCount;

      meanX += deltaX * otherCount / newCount;
      meanY += deltaY * otherCount / newCount;
      count = newCount;
    }
  }

  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    checkArgument(columnBuilders.length == 1, "partialResult of Regression should be 1");
    if (count == 0) {
      columnBuilders[0].appendNull();
    } else {
      byte[] bytes = new byte[40];
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

    if (m2X == 0) {
      columnBuilder.appendNull();
      return;
    }

    double slope = c2 / m2X;

    switch (regressionType) {
      case REGR_SLOPE:
        columnBuilder.writeDouble(slope);
        break;
      case REGR_INTERCEPT:
        columnBuilder.writeDouble(meanY - slope * meanX);
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
