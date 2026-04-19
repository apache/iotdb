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

package org.apache.iotdb.db.queryengine.execution.aggregation;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;

public class CorrelationAccumulator implements Accumulator {

  private static final int INTERMEDIATE_SIZE = Long.BYTES + 5 * Double.BYTES;

  public enum CorrelationType {
    CORR
  }

  private final TSDataType[] seriesDataTypes;
  private final CorrelationType correlationType;

  private long count;
  private double meanX;
  private double meanY;
  private double m2X;
  private double m2Y;
  private double c2;

  public CorrelationAccumulator(TSDataType[] seriesDataTypes, CorrelationType correlationType) {
    this.seriesDataTypes = seriesDataTypes;
    this.correlationType = correlationType;
  }

  @Override
  public void addInput(Column[] columns, BitMap bitMap) {

    int size = columns[0].getPositionCount();
    for (int i = 0; i < size; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (columns[1].isNull(i) || columns[2].isNull(i)) {
        continue;
      }

      double x = getDoubleValue(columns[1], i, seriesDataTypes[0]);
      double y = getDoubleValue(columns[2], i, seriesDataTypes[1]);

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

    double oldMeanX = meanX;
    double oldMeanY = meanY;

    meanX = oldMeanX + (x - oldMeanX) / newCount;
    meanY = oldMeanY + (y - oldMeanY) / newCount;

    c2 += (x - oldMeanX) * (y - meanY);
    m2X += (x - oldMeanX) * (x - meanX);
    m2Y += (y - oldMeanY) * (y - meanY);

    count = newCount;
  }

  @Override
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 1, "partialResult of Correlation should be 1");
    if (partialResult[0].isNull(0)) {
      return;
    }
    byte[] bytes = partialResult[0].getBinary(0).getValues();
    ByteBuffer buffer = ByteBuffer.wrap(bytes);

    long otherCount = buffer.getLong();
    double otherMeanX = buffer.getDouble();
    double otherMeanY = buffer.getDouble();
    double otherM2X = buffer.getDouble();
    double otherM2Y = buffer.getDouble();
    double otherC2 = buffer.getDouble();

    merge(otherCount, otherMeanX, otherMeanY, otherM2X, otherM2Y, otherC2);
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
      long na = count;
      long n = na + otherCount;
      double meanXValue = meanX;
      double meanYValue = meanY;
      double deltaX = otherMeanX - meanXValue;
      double deltaY = otherMeanY - meanYValue;

      m2X += otherM2X + na * otherCount * deltaX * deltaX / (double) n;
      m2Y += otherM2Y + na * otherCount * deltaY * deltaY / (double) n;
      c2 += otherC2 + deltaX * deltaY * na * otherCount / (double) n;
      meanX = meanXValue + deltaX * otherCount / (double) n;
      meanY = meanYValue + deltaY * otherCount / (double) n;
      count = n;
    }
  }

  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    checkArgument(columnBuilders.length == 1, "partialResult of Correlation should be 1");
    if (count == 0) {
      columnBuilders[0].appendNull();
    } else {
      byte[] bytes = new byte[INTERMEDIATE_SIZE];
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      buffer.putLong(count);
      buffer.putDouble(meanX);
      buffer.putDouble(meanY);
      buffer.putDouble(m2X);
      buffer.putDouble(m2Y);
      buffer.putDouble(c2);
      columnBuilders[0].writeBinary(new Binary(bytes));
    }
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    if (correlationType != CorrelationType.CORR) {
      throw new UnsupportedOperationException("Unknown type: " + correlationType);
    }

    double result = c2 / Math.sqrt(m2X * m2Y);
    if (Double.isFinite(result)) {
      columnBuilder.writeDouble(result);
    } else {
      columnBuilder.appendNull();
    }
  }

  @Override
  public void removeIntermediate(Column[] input) {
    checkArgument(input.length == 1, "Input of Correlation should be 1");
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
        count >= otherCount, "Correlation state count is smaller than removed state count");

    if (count == otherCount) {
      reset();
      return;
    }

    double otherMeanX = buffer.getDouble();
    double otherMeanY = buffer.getDouble();
    double otherM2X = buffer.getDouble();
    double otherM2Y = buffer.getDouble();
    double otherC2 = buffer.getDouble();

    long totalCount = count;
    long newCount = totalCount - otherCount;

    double newMeanX = (totalCount * meanX - otherCount * otherMeanX) / newCount;
    double newMeanY = (totalCount * meanY - otherCount * otherMeanY) / newCount;

    double deltaX = otherMeanX - newMeanX;
    double deltaY = otherMeanY - newMeanY;
    double correction = ((double) newCount * otherCount) / totalCount;

    c2 = c2 - otherC2 - deltaX * deltaY * correction;
    m2X = m2X - otherM2X - deltaX * deltaX * correction;
    m2Y = m2Y - otherM2Y - deltaY * deltaY * correction;

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
    m2Y = 0;
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
