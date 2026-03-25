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

import org.apache.iotdb.db.queryengine.execution.aggregation.CovarianceAccumulator;
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

public class GroupedCovarianceAccumulator implements GroupedAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedCovarianceAccumulator.class);

  private final TSDataType xDataType;
  private final TSDataType yDataType;
  private final CovarianceAccumulator.CovarianceType covarianceType;

  private final LongBigArray counts = new LongBigArray();
  private final DoubleBigArray meanXs = new DoubleBigArray();
  private final DoubleBigArray meanYs = new DoubleBigArray();
  private final DoubleBigArray c2s = new DoubleBigArray();

  public GroupedCovarianceAccumulator(
      TSDataType xDataType,
      TSDataType yDataType,
      CovarianceAccumulator.CovarianceType covarianceType) {
    this.xDataType = xDataType;
    this.yDataType = yDataType;
    this.covarianceType = covarianceType;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE + counts.sizeOf() + meanXs.sizeOf() + meanYs.sizeOf() + c2s.sizeOf();
  }

  @Override
  public void setGroupCount(long groupCount) {
    counts.ensureCapacity(groupCount);
    meanXs.ensureCapacity(groupCount);
    meanYs.ensureCapacity(groupCount);
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
            String.format("Unsupported data type in Covariance Aggregation: %s", dataType));
    }
  }

  private void update(int groupId, double x, double y) {
    long newCount = counts.get(groupId) + 1;
    double oldMeanX = meanXs.get(groupId);
    double oldMeanY = meanYs.get(groupId);
    double newMeanX = oldMeanX + (x - oldMeanX) / newCount;
    double newMeanY = oldMeanY + (y - oldMeanY) / newCount;

    meanXs.set(groupId, newMeanX);
    meanYs.set(groupId, newMeanY);
    c2s.add(groupId, (x - oldMeanX) * (y - newMeanY));
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
      double otherC2 = buffer.getDouble();

      merge(groupIds[i], otherCount, otherMeanX, otherMeanY, otherC2);
    }
  }

  private void merge(
      int groupId, long otherCount, double otherMeanX, double otherMeanY, double otherC2) {
    if (otherCount == 0) {
      return;
    }

    long count = counts.get(groupId);
    if (count == 0) {
      counts.set(groupId, otherCount);
      meanXs.set(groupId, otherMeanX);
      meanYs.set(groupId, otherMeanY);
      c2s.set(groupId, otherC2);
      return;
    }

    long newCount = count + otherCount;
    double meanX = meanXs.get(groupId);
    double meanY = meanYs.get(groupId);
    double deltaX = otherMeanX - meanX;
    double deltaY = otherMeanY - meanY;

    c2s.add(groupId, otherC2 + deltaX * deltaY * count * otherCount / newCount);
    meanXs.add(groupId, deltaX * otherCount / newCount);
    meanYs.add(groupId, deltaY * otherCount / newCount);
    counts.set(groupId, newCount);
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        "intermediate input and output should be BinaryColumn");

    long count = counts.get(groupId);
    if (count == 0) {
      columnBuilder.appendNull();
      return;
    }

    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Double.BYTES * 3);
    buffer.putLong(count);
    buffer.putDouble(meanXs.get(groupId));
    buffer.putDouble(meanYs.get(groupId));
    buffer.putDouble(c2s.get(groupId));
    columnBuilder.writeBinary(new Binary(buffer.array()));
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
    long count = counts.get(groupId);
    switch (covarianceType) {
      case COVAR_POP:
        if (count == 0) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.writeDouble(c2s.get(groupId) / count);
        }
        break;
      case COVAR_SAMP:
        if (count < 2) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.writeDouble(c2s.get(groupId) / (count - 1));
        }
        break;
      default:
        throw new UnsupportedOperationException("Unknown type: " + covarianceType);
    }
  }

  @Override
  public void prepareFinal() {}

  @Override
  public void reset() {
    counts.reset();
    meanXs.reset();
    meanYs.reset();
    c2s.reset();
  }
}
