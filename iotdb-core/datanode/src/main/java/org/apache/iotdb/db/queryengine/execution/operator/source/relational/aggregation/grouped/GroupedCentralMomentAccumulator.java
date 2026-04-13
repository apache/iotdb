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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped;

import org.apache.iotdb.db.queryengine.execution.aggregation.CentralMomentAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.AggregationMask;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.DoubleBigArray;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.LongBigArray;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;

public class GroupedCentralMomentAccumulator implements GroupedAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedCentralMomentAccumulator.class);
  private static final int INTERMEDIATE_SIZE = Long.BYTES + 4 * Double.BYTES;

  private final TSDataType seriesDataType;
  private final CentralMomentAccumulator.MomentType momentType;

  private final LongBigArray counts = new LongBigArray();
  private final DoubleBigArray means = new DoubleBigArray();
  private final DoubleBigArray m2s = new DoubleBigArray();
  private final DoubleBigArray m3s = new DoubleBigArray();
  private final DoubleBigArray m4s = new DoubleBigArray();

  public GroupedCentralMomentAccumulator(
      TSDataType seriesDataType, CentralMomentAccumulator.MomentType momentType) {
    this.seriesDataType = seriesDataType;
    this.momentType = momentType;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE
        + counts.sizeOf()
        + means.sizeOf()
        + m2s.sizeOf()
        + m3s.sizeOf()
        + m4s.sizeOf();
  }

  @Override
  public void setGroupCount(long groupCount) {
    counts.ensureCapacity(groupCount);
    means.ensureCapacity(groupCount);
    m2s.ensureCapacity(groupCount);
    m3s.ensureCapacity(groupCount);
    m4s.ensureCapacity(groupCount);
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();
    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!arguments[0].isNull(i)) {
          update(groupIds[i], getDoubleValue(arguments[0], i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      for (int i = 0; i < positionCount; i++) {
        int position = selectedPositions[i];
        if (!arguments[0].isNull(position)) {
          update(groupIds[position], getDoubleValue(arguments[0], position));
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

  private void update(int groupId, double value) {
    long n1 = counts.get(groupId);
    long n = n1 + 1;
    double m1 = means.get(groupId);
    double m2 = m2s.get(groupId);
    double m3 = m3s.get(groupId);
    double m4 = m4s.get(groupId);

    double delta = value - m1;
    double deltaN = delta / n;
    double deltaN2 = deltaN * deltaN;
    double dm2 = delta * deltaN * n1;

    counts.set(groupId, n);
    means.set(groupId, m1 + deltaN);
    m2s.set(groupId, m2 + dm2);
    m3s.set(groupId, m3 + dm2 * deltaN * (n - 2) - 3 * deltaN * m2);
    m4s.set(
        groupId,
        m4 + dm2 * deltaN2 * (n * (double) n - 3 * n + 3) + 6 * deltaN2 * m2 - 4 * deltaN * m3);
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {
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

      merge(groupIds[i], otherCount, otherMean, otherM2, otherM3, otherM4);
    }
  }

  private void merge(int groupId, long nB, double meanB, double m2B, double m3B, double m4B) {
    if (nB == 0) return;
    long nA = counts.get(groupId);
    if (nA == 0) {
      counts.set(groupId, nB);
      means.set(groupId, meanB);
      m2s.set(groupId, m2B);
      m3s.set(groupId, m3B);
      m4s.set(groupId, m4B);
    } else {
      double m1A = means.get(groupId);
      double m2A = m2s.get(groupId);
      double m3A = m3s.get(groupId);
      double n = nA + nB;
      double delta = meanB - m1A;
      double delta2 = delta * delta;
      double delta3 = delta * delta2;
      double delta4 = delta2 * delta2;

      counts.set(groupId, (long) n);
      means.set(groupId, (nA * m1A + nB * meanB) / n);
      m2s.set(groupId, m2A + m2B + delta2 * nA * nB / n);
      m3s.set(
          groupId,
          m3A
              + m3B
              + delta3 * nA * nB * (nA - nB) / (n * n)
              + 3 * delta * (nA * m2B - nB * m2A) / n);
      m4s.set(
          groupId,
          m4s.get(groupId)
              + m4B
              + delta4 * nA * nB * (nA * nA - nA * nB + nB * nB) / (n * n * n)
              + 6 * delta2 * (nA * nA * m2B + nB * nB * m2A) / (n * n)
              + 4 * delta * (nA * m3B - nB * m3A) / n);
    }
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        "intermediate input and output should be BinaryColumn");

    if (counts.get(groupId) == 0) {
      columnBuilder.appendNull();
    } else {
      ByteBuffer buffer = ByteBuffer.allocate(INTERMEDIATE_SIZE);
      buffer.putLong(counts.get(groupId));
      buffer.putDouble(means.get(groupId));
      buffer.putDouble(m2s.get(groupId));
      buffer.putDouble(m3s.get(groupId));
      buffer.putDouble(m4s.get(groupId));
      columnBuilder.writeBinary(new Binary(buffer.array()));
    }
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
    long count = counts.get(groupId);
    double m2 = m2s.get(groupId);

    if (count == 0 || m2 == 0) {
      columnBuilder.appendNull();
      return;
    }

    if (momentType == CentralMomentAccumulator.MomentType.SKEWNESS) {
      if (count < 3) {
        columnBuilder.appendNull();
        return;
      }
      double m3 = m3s.get(groupId);
      double result = Math.sqrt((double) count) * m3 / Math.pow(m2, 1.5);
      columnBuilder.writeDouble(result);
    } else {
      if (count < 4) {
        columnBuilder.appendNull();
      } else {
        double m4 = m4s.get(groupId);
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
  public void prepareFinal() {}

  @Override
  public void reset() {
    counts.reset();
    means.reset();
    m2s.reset();
    m3s.reset();
    m4s.reset();
  }
}
