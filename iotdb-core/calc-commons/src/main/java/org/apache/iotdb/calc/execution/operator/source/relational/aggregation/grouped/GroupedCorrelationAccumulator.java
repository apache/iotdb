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

import org.apache.iotdb.db.queryengine.execution.aggregation.CorrelationAccumulator;
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

public class GroupedCorrelationAccumulator implements GroupedAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedCorrelationAccumulator.class);
  private final TSDataType xDataType;
  private final TSDataType yDataType;
  private final CorrelationAccumulator.CorrelationType correlationType;

  private final LongBigArray counts = new LongBigArray();
  private final DoubleBigArray meanXs = new DoubleBigArray();
  private final DoubleBigArray meanYs = new DoubleBigArray();
  private final DoubleBigArray m2Xs = new DoubleBigArray();
  private final DoubleBigArray m2Ys = new DoubleBigArray();
  private final DoubleBigArray c2s = new DoubleBigArray();

  public GroupedCorrelationAccumulator(
      TSDataType xDataType,
      TSDataType yDataType,
      CorrelationAccumulator.CorrelationType correlationType) {
    this.xDataType = xDataType;
    this.yDataType = yDataType;
    this.correlationType = correlationType;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE
        + counts.sizeOf()
        + meanXs.sizeOf()
        + meanYs.sizeOf()
        + m2Xs.sizeOf()
        + m2Ys.sizeOf()
        + c2s.sizeOf();
  }

  @Override
  public void setGroupCount(long groupCount) {
    counts.ensureCapacity(groupCount);
    meanXs.ensureCapacity(groupCount);
    meanYs.ensureCapacity(groupCount);
    m2Xs.ensureCapacity(groupCount);
    m2Ys.ensureCapacity(groupCount);
    c2s.ensureCapacity(groupCount);
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (arguments[0].isNull(i) || arguments[1].isNull(i)) {
          continue;
        }
        double x = getDoubleValue(arguments[0], i, xDataType);
        double y = getDoubleValue(arguments[1], i, yDataType);
        update(groupIds[i], x, y);
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      for (int i = 0; i < positionCount; i++) {
        int position = selectedPositions[i];
        if (arguments[0].isNull(position) || arguments[1].isNull(position)) {
          continue;
        }
        double x = getDoubleValue(arguments[0], position, xDataType);
        double y = getDoubleValue(arguments[1], position, yDataType);
        update(groupIds[position], x, y);
      }
    }
  }

  private double getDoubleValue(Column column, int position, TSDataType dataType) {
    switch (dataType) {
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
            String.format("Unsupported data type in Correlation Aggregation: %s", dataType));
    }
  }

  private void update(int groupId, double x, double y) {
    long newCount = counts.get(groupId) + 1;
    double deltaX = x - meanXs.get(groupId);
    double deltaY = y - meanYs.get(groupId);

    meanXs.add(groupId, deltaX / newCount);
    meanYs.add(groupId, deltaY / newCount);

    c2s.add(groupId, deltaX * (y - meanYs.get(groupId)));
    m2Xs.add(groupId, deltaX * (x - meanXs.get(groupId)));
    m2Ys.add(groupId, deltaY * (y - meanYs.get(groupId)));

    counts.set(groupId, newCount);
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
      double otherMeanX = buffer.getDouble();
      double otherMeanY = buffer.getDouble();
      double otherM2X = buffer.getDouble();
      double otherM2Y = buffer.getDouble();
      double otherC2 = buffer.getDouble();

      merge(groupIds[i], otherCount, otherMeanX, otherMeanY, otherM2X, otherM2Y, otherC2);
    }
  }

  private void merge(
      int groupId,
      long otherCount,
      double otherMeanX,
      double otherMeanY,
      double otherM2X,
      double otherM2Y,
      double otherC2) {
    if (otherCount == 0) {
      return;
    }
    if (counts.get(groupId) == 0) {
      counts.set(groupId, otherCount);
      meanXs.set(groupId, otherMeanX);
      meanYs.set(groupId, otherMeanY);
      m2Xs.set(groupId, otherM2X);
      m2Ys.set(groupId, otherM2Y);
      c2s.set(groupId, otherC2);
    } else {
      long newCount = counts.get(groupId) + otherCount;
      double deltaX = otherMeanX - meanXs.get(groupId);
      double deltaY = otherMeanY - meanYs.get(groupId);

      c2s.add(groupId, otherC2 + deltaX * deltaY * counts.get(groupId) * otherCount / newCount);
      m2Xs.add(groupId, otherM2X + deltaX * deltaX * counts.get(groupId) * otherCount / newCount);
      m2Ys.add(groupId, otherM2Y + deltaY * deltaY * counts.get(groupId) * otherCount / newCount);

      meanXs.add(groupId, deltaX * otherCount / newCount);
      meanYs.add(groupId, deltaY * otherCount / newCount);
      counts.set(groupId, newCount);
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
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Double.BYTES * 5);
      buffer.putLong(counts.get(groupId));
      buffer.putDouble(meanXs.get(groupId));
      buffer.putDouble(meanYs.get(groupId));
      buffer.putDouble(m2Xs.get(groupId));
      buffer.putDouble(m2Ys.get(groupId));
      buffer.putDouble(c2s.get(groupId));
      columnBuilder.writeBinary(new Binary(buffer.array()));
    }
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
    switch (correlationType) {
      case CORR:
        if (counts.get(groupId) < 2) {
          columnBuilder.appendNull();
        } else if (m2Xs.get(groupId) == 0 || m2Ys.get(groupId) == 0) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.writeDouble(
              c2s.get(groupId) / Math.sqrt(m2Xs.get(groupId) * m2Ys.get(groupId)));
        }
        break;
      case COVAR_POP:
        if (counts.get(groupId) == 0) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.writeDouble(c2s.get(groupId) / counts.get(groupId));
        }
        break;
      case COVAR_SAMP:
        if (counts.get(groupId) < 2) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.writeDouble(c2s.get(groupId) / (counts.get(groupId) - 1));
        }
        break;
      default:
        throw new UnsupportedOperationException("Unknown type: " + correlationType);
    }
  }

  @Override
  public void prepareFinal() {}

  @Override
  public void reset() {
    counts.reset();
    meanXs.reset();
    meanYs.reset();
    m2Xs.reset();
    m2Ys.reset();
    c2s.reset();
  }
}
