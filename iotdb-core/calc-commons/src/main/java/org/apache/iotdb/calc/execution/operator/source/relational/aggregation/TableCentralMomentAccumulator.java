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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation;

import org.apache.iotdb.calc.execution.aggregation.CentralMomentAccumulator;
import org.apache.iotdb.calc.i18n.CalcMessages;

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

public class TableCentralMomentAccumulator implements TableAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableCentralMomentAccumulator.class);
  private static final int INTERMEDIATE_SIZE = Long.BYTES + 4 * Double.BYTES;
  private static final double EPSILON = 1e-12;

  private final TSDataType seriesDataType;
  private final CentralMomentAccumulator.MomentType momentType;

  private long count;
  private double mean;
  private double m2;
  private double m3;
  private double m4;

  public TableCentralMomentAccumulator(
      TSDataType seriesDataType, CentralMomentAccumulator.MomentType momentType) {
    this.seriesDataType = seriesDataType;
    this.momentType = momentType;
  }

  @Override
  public void addInput(Column[] arguments, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();
    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!arguments[0].isNull(i)) {
          update(getDoubleValue(arguments[0], i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      for (int i = 0; i < positionCount; i++) {
        int position = selectedPositions[i];
        if (!arguments[0].isNull(position)) {
          update(getDoubleValue(arguments[0], position));
        }
      }
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
        throw new UnSupportedDataTypeException(
            String.format(
                CalcMessages.EXCEPTION_UNSUPPORTED_DATA_TYPE_CENTRALMOMENT_AGGREGATION_ARG_74B9A3A8,
                seriesDataType));
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
  public void addIntermediate(Column argument) {
    checkArgument(
        argument instanceof BinaryColumn
            || (argument instanceof RunLengthEncodedColumn
                && ((RunLengthEncodedColumn) argument).getValue() instanceof BinaryColumn),
        CalcMessages.EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_SHOULD_BE_BINARYCOLUMN_3B5148FA);

    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (argument.isNull(i)) {
        continue;
      }
      byte[] bytes = argument.getBinary(i).getValues();
      ByteBuffer buffer = ByteBuffer.wrap(bytes);

      long otherCount = buffer.getLong();
      double otherMean = buffer.getDouble();
      double otherM2 = buffer.getDouble();
      double otherM3 = buffer.getDouble();
      double otherM4 = buffer.getDouble();

      merge(otherCount, otherMean, otherM2, otherM3, otherM4);
    }
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
      long n = nA + nB;
      double nDouble = n;
      double delta = meanB - m1A;
      double delta2 = delta * delta;
      double delta3 = delta * delta2;
      double delta4 = delta2 * delta2;

      count = n;
      mean = (nA * m1A + nB * meanB) / nDouble;
      m2 = m2A + m2B + delta2 * nA * nB / nDouble;
      m3 =
          m3A
              + m3B
              + delta3 * nA * nB * (nA - nB) / (nDouble * nDouble)
              + 3 * delta * (nA * m2B - nB * m2A) / nDouble;
      m4 +=
          m4B
              + delta4 * nA * nB * (nA * nA - nA * nB + nB * nB) / (nDouble * nDouble * nDouble)
              + 6 * delta2 * (nA * nA * m2B + nB * nB * m2A) / (nDouble * nDouble)
              + 4 * delta * (nA * m3B - nB * m3A) / nDouble;
    }
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        CalcMessages.EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_SHOULD_BE_BINARYCOLUMN_3B5148FA);

    if (count == 0) {
      columnBuilder.appendNull();
    } else {

      ByteBuffer buffer = ByteBuffer.allocate(INTERMEDIATE_SIZE);
      buffer.putLong(count);
      buffer.putDouble(mean);
      buffer.putDouble(m2);
      buffer.putDouble(m3);
      buffer.putDouble(m4);
      columnBuilder.writeBinary(new Binary(buffer.array()));
    }
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    if (count == 0 || Math.abs(m2) < EPSILON) {
      columnBuilder.appendNull();
      return;
    }

    if (momentType == CentralMomentAccumulator.MomentType.SKEWNESS) {
      if (count < 3) {
        columnBuilder.appendNull();
        return;
      }
      double n = count;
      double result = n * Math.sqrt(n - 1) * m3 / ((n - 2) * Math.pow(m2, 1.5));
      columnBuilder.writeDouble(result);
    } else {
      if (count < 4) {
        columnBuilder.appendNull();
      } else {
        double n = count;
        double variance = m2 / (count - 1);
        double term1 = (n * (n + 1) * m4) / ((n - 1) * (n - 2) * (n - 3) * variance * variance);
        double term2 = (3 * (n - 1) * (n - 1)) / ((n - 2) * (n - 3));
        columnBuilder.writeDouble(term1 - term2);
      }
    }
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new TableCentralMomentAccumulator(seriesDataType, momentType);
  }

  @Override
  public void removeInput(Column[] arguments) {
    checkArgument(
        arguments.length == 1, CalcMessages.EXCEPTION_INPUT_OF_CENTRALMOMENT_SHOULD_BE_1_FD23B170);
    if (count == 0 || arguments[0].isNull(0)) {
      return;
    }

    double value = getDoubleValue(arguments[0], 0);
    if (count == 1) {
      reset();
      return;
    }

    long nTotal = count;
    long nA = nTotal - 1;

    double meanA = (nTotal * mean - value) / nA;

    double delta = value - meanA;
    double delta2 = delta * delta;
    double delta3 = delta * delta2;
    double delta4 = delta2 * delta2;

    double m2A = m2 - delta2 * nA / nTotal;
    double m3A =
        m3 - delta3 * nA * (nA - 1) / ((double) nTotal * nTotal) + 3.0 * delta * m2A / nTotal;
    double m4A =
        m4
            - delta4 * nA * ((double) nA * nA - nA + 1) / ((double) nTotal * nTotal * nTotal)
            - 6.0 * delta2 * m2A / ((double) nTotal * nTotal)
            + 4.0 * delta * m3A / nTotal;

    count = nA;
    mean = meanA;
    m2 = normalizeZero(m2A);
    m3 = normalizeZero(m3A);
    m4 = normalizeZero(m4A);
  }

  private double normalizeZero(double value) {
    return Math.abs(value) < EPSILON ? 0 : value;
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
    mean = 0;
    m2 = 0;
    m3 = 0;
    m4 = 0;
  }

  @Override
  public boolean removable() {
    return true;
  }
}
