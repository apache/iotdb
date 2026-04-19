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

public class CentralMomentAccumulator implements Accumulator {

  private static final int INTERMEDIATE_SIZE = Long.BYTES + 4 * Double.BYTES;

  public enum MomentType {
    SKEWNESS,
    KURTOSIS
  }

  private final TSDataType seriesDataType;
  private final MomentType momentType;

  private long count;
  private double mean;
  private double m2;
  private double m3;
  private double m4;

  public CentralMomentAccumulator(TSDataType seriesDataType, MomentType momentType) {
    this.seriesDataType = seriesDataType;
    this.momentType = momentType;
  }

  @Override
  public void addInput(Column[] columns, BitMap bitMap) {

    int size = columns[1].getPositionCount();
    for (int i = 0; i < size; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (columns[1].isNull(i)) {
        continue;
      }
      update(getDoubleValue(columns[1], i));
    }
  }

  private double getDoubleValue(Column column, int position) {
    switch (seriesDataType) {
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
        throw new UnsupportedOperationException(
            "Unsupported data type in CentralMoment Aggregation: " + seriesDataType);
    }
  }

  private void update(double value) {
    long n1 = count;
    long n = n1 + 1;
    double m1 = mean;
    double m2 = this.m2;
    double m3 = this.m3;
    double delta = value - m1;
    double deltaN = delta / n;
    double deltaN2 = deltaN * deltaN;
    double dm2 = delta * deltaN * n1;

    count = n;
    mean = m1 + deltaN;
    this.m2 = m2 + dm2;
    this.m3 = m3 + dm2 * deltaN * (n - 2) - 3 * deltaN * m2;
    m4 += dm2 * deltaN2 * (n * (double) n - 3 * n + 3) + 6 * deltaN2 * m2 - 4 * deltaN * m3;
  }

  @Override
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 1, "partialResult of CentralMoment should be 1");
    if (partialResult[0].isNull(0)) {
      return;
    }
    byte[] bytes = partialResult[0].getBinary(0).getValues();
    ByteBuffer buffer = ByteBuffer.wrap(bytes);

    long otherCount = buffer.getLong();
    double otherMean = buffer.getDouble();
    double otherM2 = buffer.getDouble();
    double otherM3 = buffer.getDouble();
    double otherM4 = buffer.getDouble();

    merge(otherCount, otherMean, otherM2, otherM3, otherM4);
  }

  private void merge(long nB, double meanB, double m2B, double m3B, double m4B) {
    if (nB == 0) return;
    if (count == 0) {
      count = nB;
      mean = meanB;
      m2 = m2B;
      m3 = m3B;
      m4 = m4B;
    } else {
      long nA = count;
      double m1A = mean;
      double m2A = m2;
      double m3A = m3;
      double n = nA + nB;
      double delta = meanB - m1A;
      double delta2 = delta * delta;
      double delta3 = delta * delta2;
      double delta4 = delta2 * delta2;

      count = (long) n;
      mean = (nA * m1A + nB * meanB) / n;
      m2 = m2A + m2B + delta2 * nA * nB / n;
      m3 =
          m3A
              + m3B
              + delta3 * nA * nB * (nA - nB) / (n * n)
              + 3 * delta * (nA * m2B - nB * m2A) / n;
      m4 +=
          m4B
              + delta4 * nA * nB * (nA * nA - nA * nB + nB * nB) / (n * n * n)
              + 6 * delta2 * (nA * nA * m2B + nB * nB * m2A) / (n * n)
              + 4 * delta * (nA * m3B - nB * m3A) / n;
    }
  }

  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    checkArgument(columnBuilders.length == 1, "partialResult should be 1");
    if (count == 0) {
      columnBuilders[0].appendNull();
    } else {

      byte[] bytes = new byte[INTERMEDIATE_SIZE];
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      buffer.putLong(count);
      buffer.putDouble(mean);
      buffer.putDouble(m2);
      buffer.putDouble(m3);
      buffer.putDouble(m4);
      columnBuilders[0].writeBinary(new Binary(bytes));
    }
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    if (count == 0 || m2 == 0) {
      columnBuilder.appendNull();
      return;
    }

    if (momentType == MomentType.SKEWNESS) {
      if (count < 3) {
        columnBuilder.appendNull();
        return;
      }
      double result = Math.sqrt((double) count) * m3 / Math.pow(m2, 1.5);
      columnBuilder.writeDouble(result);
    } else {
      if (count < 4) {
        columnBuilder.appendNull();
      } else {

        double variance = m2 / (count - 1);
        double term1 =
            (count * (count + 1) * m4)
                / ((count - 1) * (count - 2) * (count - 3) * variance * variance);
        double term2 = (3 * Math.pow(count - 1, 2)) / ((count - 2) * (count - 3));
        columnBuilder.writeDouble(term1 - term2);
      }
    }
  }

  @Override
  public void removeIntermediate(Column[] input) {
    checkArgument(input.length == 1, "Input of CentralMoment should be 1");
    if (input[0].isNull(0)) {
      return;
    }

    byte[] bytes = input[0].getBinary(0).getValues();
    ByteBuffer buffer = ByteBuffer.wrap(bytes);

    long nB = buffer.getLong();
    if (nB == 0) {
      return;
    }
    checkArgument(count >= nB, "CentralMoment state count is smaller than removed state count");

    if (count == nB) {
      reset();
      return;
    }

    double meanB = buffer.getDouble();
    double m2B = buffer.getDouble();
    double m3B = buffer.getDouble();
    double m4B = buffer.getDouble();

    long nTotal = count;
    long nA = nTotal - nB;

    double meanA = ((double) nTotal * mean - (double) nB * meanB) / nA;

    double delta = meanB - meanA;
    double delta2 = delta * delta;
    double delta3 = delta * delta2;
    double delta4 = delta2 * delta2;

    double m2A = m2 - m2B - delta2 * nA * nB / nTotal;
    double m3A =
        m3
            - m3B
            - delta3 * nA * nB * (nA - nB) / ((double) nTotal * nTotal)
            - 3.0 * delta * (nA * m2B - nB * m2A) / nTotal;

    double m4A =
        m4
            - m4B
            - delta4
                * nA
                * nB
                * ((double) nA * nA - (double) nA * nB + (double) nB * nB)
                / ((double) nTotal * nTotal * nTotal)
            - 6.0
                * delta2
                * ((double) nA * nA * m2B + (double) nB * nB * m2A)
                / ((double) nTotal * nTotal)
            - 4.0 * delta * (nA * m3B - nB * m3A) / nTotal;

    count = nA;
    mean = meanA;
    m2 = m2A;
    m3 = m3A;
    m4 = m4A;
  }

  @Override
  public void addStatistics(Statistics statistics) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setFinal(Column finalResult) {}

  @Override
  public void reset() {
    count = 0;
    mean = 0;
    m2 = 0;
    m3 = 0;
    m4 = 0;
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
