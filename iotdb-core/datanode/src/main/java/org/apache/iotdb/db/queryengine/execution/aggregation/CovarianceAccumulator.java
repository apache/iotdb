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

public class CovarianceAccumulator implements Accumulator {

  public enum CovarianceType {
    COVAR_POP,
    COVAR_SAMP
  }

  private final TSDataType[] seriesDataTypes;
  private final CovarianceType covarianceType;

  private long count;
  private double meanX;
  private double meanY;
  private double c2;

  public CovarianceAccumulator(TSDataType[] seriesDataTypes, CovarianceType covarianceType) {
    this.seriesDataTypes = seriesDataTypes;
    this.covarianceType = covarianceType;
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
    meanX = oldMeanX + (x - oldMeanX) / newCount;
    double oldMeanY = meanY;
    double newMeanY = oldMeanY + (y - oldMeanY) / newCount;
    meanY = newMeanY;
    c2 += (x - oldMeanX) * (y - newMeanY);
    count = newCount;
  }

  @Override
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 1, "partialResult of Covariance should be 1");
    if (partialResult[0].isNull(0)) {
      return;
    }

    byte[] bytes = partialResult[0].getBinary(0).getValues();
    ByteBuffer buffer = ByteBuffer.wrap(bytes);

    long otherCount = buffer.getLong();
    double otherMeanX = buffer.getDouble();
    double otherMeanY = buffer.getDouble();
    double otherC2 = buffer.getDouble();

    merge(otherCount, otherMeanX, otherMeanY, otherC2);
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
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    checkArgument(columnBuilders.length == 1, "partialResult of Covariance should be 1");
    if (count == 0) {
      columnBuilders[0].appendNull();
      return;
    }

    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Double.BYTES * 3);
    buffer.putLong(count);
    buffer.putDouble(meanX);
    buffer.putDouble(meanY);
    buffer.putDouble(c2);
    columnBuilders[0].writeBinary(new Binary(buffer.array()));
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
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
  public void removeIntermediate(Column[] input) {
    throw new UnsupportedOperationException("Remove not implemented for Covariance");
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
