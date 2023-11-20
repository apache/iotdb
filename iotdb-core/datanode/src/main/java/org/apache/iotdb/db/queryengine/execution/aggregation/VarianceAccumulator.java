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

package org.apache.iotdb.db.queryengine.execution.aggregation;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.BytesUtils;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

public class VarianceAccumulator implements Accumulator {
  public enum VarianceType {
    STDDEV_POP,
    STDDEV_SAMP,
    VAR_POP,
    VAR_SAMP,
  }

  private final TSDataType seriesDataType;

  private final VarianceType varianceType;

  private long count;
  private double mean;
  private double m2;

  public VarianceAccumulator(TSDataType seriesDataType, VarianceType varianceType) {
    this.seriesDataType = seriesDataType;
    this.varianceType = varianceType;
  }

  @Override
  public void addInput(Column[] column, BitMap bitMap, int lastIndex) {
    switch (seriesDataType) {
      case INT32:
        addIntInput(column, bitMap, lastIndex);
        return;
      case INT64:
        addLongInput(column, bitMap, lastIndex);
        return;
      case FLOAT:
        addFloatInput(column, bitMap, lastIndex);
        return;
      case DOUBLE:
        addDoubleInput(column, bitMap, lastIndex);
        return;
      case TEXT:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in aggregation variance : %s", seriesDataType));
    }
  }

  @Override
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 1, "partialResult of variance should be 1");
    if (partialResult[0].isNull(0)) {
      return;
    }
    byte[] bytes = partialResult[0].getBinary(0).getValues();
    long intermediateCount = BytesUtils.bytesToLong(bytes, Long.BYTES);
    double intermediateMean = BytesUtils.bytesToDouble(bytes, Long.BYTES);
    double intermediateM2 = BytesUtils.bytesToDouble(bytes, (Long.BYTES + Double.BYTES));

    long newCount = count + intermediateCount;
    double newMean = ((intermediateCount * intermediateMean) + (count * mean)) / newCount;
    double delta = intermediateMean - mean;

    m2 = m2 + intermediateM2 + delta * delta * intermediateCount * count / newCount;
    count = newCount;
    mean = newMean;
  }

  @Override
  public void removeIntermediate(Column[] input) {
    checkArgument(input.length == 1, "Input of variance should be 1");
    if (input[0].isNull(0)) {
      return;
    }
    // Deserialize
    byte[] bytes = input[0].getBinary(0).getValues();
    long intermediateCount = BytesUtils.bytesToLong(bytes, Long.BYTES);
    double intermediateMean = BytesUtils.bytesToDouble(bytes, Long.BYTES);
    double intermediateM2 = BytesUtils.bytesToDouble(bytes, (Long.BYTES + Double.BYTES));
    // Remove from state
    long newCount = count - intermediateCount;
    double newMean = ((count * mean) - (intermediateCount * intermediateMean)) / newCount;
    double delta = intermediateMean - mean;

    m2 = m2 - intermediateM2 - delta * delta * intermediateCount * count / newCount;
    count = newCount;
    mean = newMean;
  }

  @Override
  public void addStatistics(Statistics statistics) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public void setFinal(Column finalResult) {
    reset();
    if (finalResult.isNull(0)) {
      return;
    }
    count = 1;
    double value = finalResult.getDouble(0);
    mean = value;
    m2 = value * value;
  }

  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    checkArgument(columnBuilders.length == 1, "partialResult of variance should be 1");
    if (count == 0) {
      columnBuilders[0].appendNull();
    } else {
      byte[] bytes = serialize();
      columnBuilders[0].writeBinary(new Binary(bytes));
    }
  }

  private byte[] serialize() {
    byte[] countBytes = BytesUtils.longToBytes(count);
    byte[] meanBytes = BytesUtils.doubleToBytes(mean);
    byte[] m2Bytes = BytesUtils.doubleToBytes(m2);

    return BytesUtils.concatByteArrayList(Arrays.asList(countBytes, meanBytes, m2Bytes));
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    switch (varianceType) {
      case STDDEV_POP:
        if (count == 0) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.writeDouble(Math.sqrt(m2 / count));
        }
        break;
      case STDDEV_SAMP:
        if (count < 2) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.writeDouble(Math.sqrt(m2 / (count - 1)));
        }
        break;
      case VAR_POP:
        if (count == 0) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.writeDouble(m2 / count);
        }
        break;
      case VAR_SAMP:
        if (count < 2) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.writeDouble(m2 / (count - 1));
        }
        break;
    }
  }

  @Override
  public void reset() {
    count = 0;
    mean = 0.0;
    m2 = 0.0;
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

  private void addIntInput(Column[] columns, BitMap bitmap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      if (bitmap != null && !bitmap.isMarked(i)) {
        continue;
      }
      if (!columns[1].isNull(i)) {
        int value = columns[1].getInt(i);
        count++;
        double delta = value - mean;
        mean += delta / count;
        m2 += delta * (value - mean);
      }
    }
  }

  private void addLongInput(Column[] columns, BitMap bitmap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      if (bitmap != null && !bitmap.isMarked(i)) {
        continue;
      }
      if (!columns[1].isNull(i)) {
        long value = columns[1].getLong(i);
        count++;
        double delta = value - mean;
        mean += delta / count;
        m2 += delta * (value - mean);
      }
    }
  }

  private void addFloatInput(Column[] columns, BitMap bitmap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      if (bitmap != null && !bitmap.isMarked(i)) {
        continue;
      }
      if (!columns[1].isNull(i)) {
        float value = columns[1].getFloat(i);
        count++;
        double delta = value - mean;
        mean += delta / count;
        m2 += delta * (value - mean);
      }
    }
  }

  private void addDoubleInput(Column[] columns, BitMap bitmap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      if (bitmap != null && !bitmap.isMarked(i)) {
        continue;
      }
      if (!columns[1].isNull(i)) {
        double value = columns[1].getDouble(i);
        count++;
        double delta = value - mean;
        mean += delta / count;
        m2 += delta * (value - mean);
      }
    }
  }
}
