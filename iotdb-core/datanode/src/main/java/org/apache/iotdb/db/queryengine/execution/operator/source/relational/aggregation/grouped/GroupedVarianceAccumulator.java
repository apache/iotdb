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

import org.apache.iotdb.db.queryengine.execution.aggregation.VarianceAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.DoubleBigArray;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.LongBigArray;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import static com.google.common.base.Preconditions.checkArgument;

public class GroupedVarianceAccumulator implements GroupedAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedVarianceAccumulator.class);
  private final TSDataType seriesDataType;
  private final VarianceAccumulator.VarianceType varianceType;

  private final LongBigArray counts = new LongBigArray();
  private final DoubleBigArray means = new DoubleBigArray();
  private final DoubleBigArray m2s = new DoubleBigArray();

  public GroupedVarianceAccumulator(
      TSDataType seriesDataType, VarianceAccumulator.VarianceType varianceType) {
    this.seriesDataType = seriesDataType;
    this.varianceType = varianceType;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE + counts.sizeOf() + means.sizeOf() + m2s.sizeOf();
  }

  @Override
  public void setGroupCount(long groupCount) {
    counts.ensureCapacity(groupCount);
    means.ensureCapacity(groupCount);
    m2s.ensureCapacity(groupCount);
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments) {
    switch (seriesDataType) {
      case INT32:
        addIntInput(groupIds, arguments[0]);
        return;
      case INT64:
        addLongInput(groupIds, arguments[0]);
        return;
      case FLOAT:
        addFloatInput(groupIds, arguments[0]);
        return;
      case DOUBLE:
        addDoubleInput(groupIds, arguments[0]);
        return;
      case TEXT:
      case BLOB:
      case BOOLEAN:
      case DATE:
      case STRING:
      case TIMESTAMP:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in aggregation variance : %s", seriesDataType));
    }
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
      long intermediateCount = BytesUtils.bytesToLong(bytes, Long.BYTES);
      double intermediateMean = BytesUtils.bytesToDouble(bytes, Long.BYTES);
      double intermediateM2 = BytesUtils.bytesToDouble(bytes, (Long.BYTES + Double.BYTES));

      long newCount = counts.get(groupIds[i]) + intermediateCount;
      double newMean =
          ((intermediateCount * intermediateMean)
                  + (counts.get(groupIds[i]) * means.get(groupIds[i])))
              / newCount;
      double delta = intermediateMean - means.get(groupIds[i]);

      m2s.add(
          groupIds[i],
          intermediateM2 + delta * delta * intermediateCount * counts.get(groupIds[i]) / newCount);
      counts.set(groupIds[i], newCount);
      means.set(groupIds[i], newMean);
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
      byte[] bytes = new byte[24];
      BytesUtils.longToBytes(counts.get(groupId), bytes, 0);
      BytesUtils.doubleToBytes(means.get(groupId), bytes, Long.BYTES);
      BytesUtils.doubleToBytes(m2s.get(groupId), bytes, Long.BYTES + Double.BYTES);
      columnBuilder.writeBinary(new Binary(bytes));
    }
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
    switch (varianceType) {
      case STDDEV_POP:
        if (counts.get(groupId) == 0) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.writeDouble(Math.sqrt(m2s.get(groupId) / counts.get(groupId)));
        }
        break;
      case STDDEV_SAMP:
        if (counts.get(groupId) < 2) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.writeDouble(Math.sqrt(m2s.get(groupId) / (counts.get(groupId) - 1)));
        }
        break;
      case VAR_POP:
        if (counts.get(groupId) == 0) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.writeDouble(m2s.get(groupId) / counts.get(groupId));
        }
        break;
      case VAR_SAMP:
        if (counts.get(groupId) < 2) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.writeDouble(m2s.get(groupId) / (counts.get(groupId) - 1));
        }
        break;
      default:
        throw new EnumConstantNotPresentException(
            VarianceAccumulator.VarianceType.class, varianceType.name());
    }
  }

  @Override
  public void prepareFinal() {}

  @Override
  public void reset() {
    counts.reset();
    means.reset();
    m2s.reset();
  }

  private void addIntInput(int[] groupIds, Column column) {
    for (int i = 0; i < column.getPositionCount(); i++) {
      if (column.isNull(i)) {
        continue;
      }

      int value = column.getInt(i);
      counts.increment(groupIds[i]);
      double delta = value - means.get(groupIds[i]);
      means.add(groupIds[i], delta / counts.get(groupIds[i]));
      m2s.add(groupIds[i], delta * (value - means.get(groupIds[i])));
    }
  }

  private void addLongInput(int[] groupIds, Column column) {
    for (int i = 0; i < column.getPositionCount(); i++) {
      if (column.isNull(i)) {
        continue;
      }

      long value = column.getLong(i);
      counts.increment(groupIds[i]);
      double delta = value - means.get(groupIds[i]);
      means.add(groupIds[i], delta / counts.get(groupIds[i]));
      m2s.add(groupIds[i], delta * (value - means.get(groupIds[i])));
    }
  }

  private void addFloatInput(int[] groupIds, Column column) {
    for (int i = 0; i < column.getPositionCount(); i++) {
      if (column.isNull(i)) {
        continue;
      }

      float value = column.getFloat(i);
      counts.increment(groupIds[i]);
      double delta = value - means.get(groupIds[i]);
      means.add(groupIds[i], delta / counts.get(groupIds[i]));
      m2s.add(groupIds[i], delta * (value - means.get(groupIds[i])));
    }
  }

  private void addDoubleInput(int[] groupIds, Column column) {
    for (int i = 0; i < column.getPositionCount(); i++) {
      if (column.isNull(i)) {
        continue;
      }

      double value = column.getDouble(i);
      counts.increment(groupIds[i]);
      double delta = value - means.get(groupIds[i]);
      means.add(groupIds[i], delta / counts.get(groupIds[i]));
      m2s.add(groupIds[i], delta * (value - means.get(groupIds[i])));
    }
  }
}
