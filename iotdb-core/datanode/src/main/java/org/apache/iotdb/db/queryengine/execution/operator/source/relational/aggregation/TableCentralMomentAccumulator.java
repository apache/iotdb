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

import org.apache.iotdb.db.queryengine.execution.aggregation.CentralMomentAccumulator;

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
                "Unsupported data type in CentralMoment Aggregation: %s", seriesDataType));
    }
  }

  private void update(double value) {
    long n1 = count;
    count++;
    double delta = value - mean;
    double delta_n = delta / count;
    double delta_n2 = delta_n * delta_n;
    double term1 = delta * delta_n * n1;
    mean += delta_n;
    m4 += term1 * delta_n2 * (count * count - 3 * count + 3) + 6 * delta_n2 * m2 - 4 * delta_n * m3;
    m3 += term1 * delta_n * (count - 2) - 3 * delta_n * m2;
    m2 += term1;
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
      long nTotal = nA + nB;
      double delta = meanB - mean;
      double delta2 = delta * delta;
      double delta3 = delta * delta2;
      double delta4 = delta2 * delta2;

      m4 +=
          m4B
              + delta4 * nA * nB * (nA * nA - nA * nB + nB * nB) / (nTotal * nTotal * nTotal)
              + 6.0 * delta2 * (nA * nA * m2B + nB * nB * m2) / (nTotal * nTotal)
              + 4.0 * delta * (nA * m3B - nB * m3) / nTotal;

      m3 +=
          m3B
              + delta3 * nA * nB * (nA - nB) / (nTotal * nTotal)
              + 3.0 * delta * (nA * m2B - nB * m2) / nTotal;

      m2 += m2B + delta2 * nA * nB / nTotal;

      mean += delta * nB / nTotal;
      count = nTotal;
    }
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        "intermediate input and output should be BinaryColumn");

    if (count == 0) {
      columnBuilder.appendNull();
    } else {

      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Double.BYTES * 4);
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
    if (count == 0 || m2 == 0) {
      columnBuilder.appendNull();
      return;
    }

    if (momentType == CentralMomentAccumulator.MomentType.SKEWNESS) {
      if (count < 3) {
        columnBuilder.appendNull();
      } else {
        double variance = m2 / (count - 1);
        double stdev = Math.sqrt(variance);
        double result = (count * m3) / ((count - 1) * (count - 2) * stdev * stdev * stdev);
        columnBuilder.writeDouble(result);
      }
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
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new TableCentralMomentAccumulator(seriesDataType, momentType);
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
    mean = 0;
    m2 = 0;
    m3 = 0;
    m4 = 0;
  }

  @Override
  public boolean removable() {
    return false;
  }
}
