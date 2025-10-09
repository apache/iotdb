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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.Percentile;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.AggregationMask;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.PercentileBigArray;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.nio.ByteBuffer;

public class GroupedPercentileAccumulator implements GroupedAccumulator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedPercentileAccumulator.class);
  private final TSDataType seriesDataType;
  private double percentage;
  private final PercentileBigArray array = new PercentileBigArray();

  public GroupedPercentileAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public void setGroupCount(long groupCount) {
    array.ensureCapacity(groupCount);
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    if (arguments.length != 2) {
      throw new SemanticException(
          String.format("PERCENTILE requires 2 arguments, but got %d", arguments.length));
    }
    percentage = arguments[1].getDouble(0);

    switch (seriesDataType) {
      case INT32:
        addIntInput(groupIds, arguments, mask);
        break;
      case INT64:
      case TIMESTAMP:
        addLongInput(groupIds, arguments, mask);
        break;
      case FLOAT:
        addFloatInput(groupIds, arguments, mask);
        break;
      case DOUBLE:
        addDoubleInput(groupIds, arguments, mask);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format(
                "Unsupported data type in PERCENTILE Aggregation: %s", seriesDataType));
    }
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {
    for (int i = 0; i < groupIds.length; i++) {
      int groupId = groupIds[i];
      if (!argument.isNull(i)) {
        byte[] data = argument.getBinary(i).getValues();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        this.percentage = ReadWriteIOUtils.readDouble(buffer);
        Percentile other = Percentile.deserialize(buffer);
        array.get(groupId).merge(other);
      }
    }
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder columnBuilder) {
    Percentile percentile = array.get(groupId);
    int percentileDataLength = percentile.getSerializedSize();
    ByteBuffer buffer = ByteBuffer.allocate(8 + percentileDataLength);
    ReadWriteIOUtils.write(percentage, buffer);
    percentile.serialize(buffer);
    columnBuilder.writeBinary(new Binary(buffer.array()));
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
    Percentile percentile = array.get(groupId);
    double result = percentile.getPercentile(percentage);
    if (Double.isNaN(result)) {
      columnBuilder.appendNull();
      return;
    }
    switch (seriesDataType) {
      case INT32:
        columnBuilder.writeInt((int) result);
        break;
      case INT64:
      case TIMESTAMP:
        columnBuilder.writeLong((long) result);
        break;
      case FLOAT:
        columnBuilder.writeFloat((float) result);
        break;
      case DOUBLE:
        columnBuilder.writeDouble(result);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in PERCENTILE Aggregation: %s", seriesDataType));
    }
  }

  @Override
  public void prepareFinal() {}

  @Override
  public void reset() {
    array.reset();
  }

  public void addIntInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    Column valueColumn = arguments[0];

    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        int groupId = groupIds[i];
        Percentile percentile = array.get(groupId);
        if (!valueColumn.isNull(i)) {
          percentile.addValue(valueColumn.getInt(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        Percentile percentile = array.get(groupId);
        if (!valueColumn.isNull(position)) {
          percentile.addValue(valueColumn.getInt(position));
        }
      }
    }
  }

  public void addLongInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    Column valueColumn = arguments[0];

    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        int groupId = groupIds[i];
        Percentile percentile = array.get(groupId);
        if (!valueColumn.isNull(i)) {
          percentile.addValue(valueColumn.getLong(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        Percentile percentile = array.get(groupId);
        if (!valueColumn.isNull(position)) {
          percentile.addValue(valueColumn.getLong(position));
        }
      }
    }
  }

  public void addFloatInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    Column valueColumn = arguments[0];

    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        int groupId = groupIds[i];
        Percentile percentile = array.get(groupId);
        if (!valueColumn.isNull(i)) {
          percentile.addValue(valueColumn.getFloat(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        Percentile percentile = array.get(groupId);
        if (!valueColumn.isNull(position)) {
          percentile.addValue(valueColumn.getFloat(position));
        }
      }
    }
  }

  public void addDoubleInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    Column valueColumn = arguments[0];

    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        int groupId = groupIds[i];
        Percentile percentile = array.get(groupId);
        if (!valueColumn.isNull(i)) {
          percentile.addValue(valueColumn.getDouble(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        Percentile percentile = array.get(groupId);
        if (!valueColumn.isNull(position)) {
          percentile.addValue(valueColumn.getDouble(position));
        }
      }
    }
  }
}
