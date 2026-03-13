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

  public enum CorrelationType {
    CORR,
    COVAR_POP,
    COVAR_SAMP
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
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    checkArgument(columnBuilders.length == 1, "partialResult of Correlation should be 1");
    if (count == 0) {
      columnBuilders[0].appendNull();
    } else {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Double.BYTES * 5);
      buffer.putLong(count);
      buffer.putDouble(meanX);
      buffer.putDouble(meanY);
      buffer.putDouble(m2X);
      buffer.putDouble(m2Y);
      buffer.putDouble(c2);
      columnBuilders[0].writeBinary(new Binary(buffer.array()));
    }
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    switch (correlationType) {
      case CORR:
        if (count < 2) {

          columnBuilder.appendNull();
        } else if (m2X == 0 || m2Y == 0) {

          columnBuilder.writeDouble(0.0);
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
  public void removeIntermediate(Column[] input) {

    throw new UnsupportedOperationException("Remove not implemented for Correlation");
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
