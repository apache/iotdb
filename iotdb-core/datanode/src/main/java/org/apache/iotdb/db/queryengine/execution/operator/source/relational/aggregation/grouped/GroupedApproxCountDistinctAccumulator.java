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

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.AggregationMask;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.HyperLogLog;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.HyperLogLogStateFactory;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.ObjectBigArray;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;

public class GroupedApproxCountDistinctAccumulator implements GroupedAccumulator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedApproxCountDistinctAccumulator.class);
  private final TSDataType seriesDataType;

  private final HyperLogLogStateFactory.GroupedHyperLogLogState state =
      HyperLogLogStateFactory.createGroupedState();

  public GroupedApproxCountDistinctAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
  }

  @Override
  public long getEstimatedSize() {
    ObjectBigArray<HyperLogLog> hlls = state.getHyperLogLogs();
    return INSTANCE_SIZE + hlls.sizeOf();
  }

  @Override
  public void setGroupCount(long groupCount) {
    ObjectBigArray<HyperLogLog> hlls = state.getHyperLogLogs();
    hlls.ensureCapacity(groupCount);
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    ObjectBigArray<HyperLogLog> hlls;
    if (arguments.length == 1) {
      hlls = HyperLogLogStateFactory.getOrCreateHyperLogLog(state);
    } else if (arguments.length == 2) {
      double maxStandardError = arguments[1].getDouble(0);
      hlls = HyperLogLogStateFactory.getOrCreateHyperLogLog(state, maxStandardError);
    } else {
      throw new IllegalArgumentException(
          "argument of APPROX_COUNT_DISTINCT should be one column with Max Standard Error");
    }
    switch (seriesDataType) {
      case BOOLEAN:
        addBooleanInput(groupIds, arguments[0], mask, hlls);
        return;
      case INT32:
      case DATE:
        addIntInput(groupIds, arguments[0], mask, hlls);
        return;
      case INT64:
      case TIMESTAMP:
        addLongInput(groupIds, arguments[0], mask, hlls);
        return;
      case FLOAT:
        addFloatInput(groupIds, arguments[0], mask, hlls);
        return;
      case DOUBLE:
        addDoubleInput(groupIds, arguments[0], mask, hlls);
        return;
      case TEXT:
      case STRING:
      case BLOB:
        addBinaryInput(groupIds, arguments[0], mask, hlls);
        return;
      default:
        throw new UnSupportedDataTypeException(
            String.format(
                "Unsupported data type in APPROX_COUNT_DISTINCT Aggregation: %s", seriesDataType));
    }
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {
    ObjectBigArray<HyperLogLog> hlls = HyperLogLogStateFactory.getOrCreateHyperLogLog(state);
    for (int i = 0; i < groupIds.length; i++) {
      if (argument.isNull(i)) {
        continue;
      }
      switch (seriesDataType) {
        case BOOLEAN:
          hlls.get(groupIds[i]).add(argument.getBoolean(i));
        case INT32:
        case DATE:
          hlls.get(groupIds[i]).add(argument.getInt(i));
        case INT64:
        case TIMESTAMP:
          hlls.get(groupIds[i]).add(argument.getLong(i));
        case FLOAT:
          hlls.get(groupIds[i]).add(argument.getFloat(i));
        case DOUBLE:
          hlls.get(groupIds[i]).add(argument.getDouble(i));
        case TEXT:
        case STRING:
        case BLOB:
          hlls.get(groupIds[i]).add(argument.getBinary(i));
        default:
          throw new UnSupportedDataTypeException(
              String.format(
                  "Unsupported data type in APPROX_COUNT_DISTINCT Aggregation: %s",
                  seriesDataType));
      }
    }
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder columnBuilder) {
    ObjectBigArray<HyperLogLog> hlls = state.getHyperLogLogs();
    columnBuilder.writeLong(hlls.get(groupId).cardinality());
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
    ObjectBigArray<HyperLogLog> hlls = state.getHyperLogLogs();
    columnBuilder.writeLong(hlls.get(groupId).cardinality());
  }

  @Override
  public void prepareFinal() {}

  @Override
  public void reset() {
    ObjectBigArray<HyperLogLog> hlls = state.getHyperLogLogs();
    hlls.forEach(HyperLogLog::reset);
    hlls.reset();
  }

  public void addBooleanInput(
      int[] groupIds, Column column, AggregationMask mask, ObjectBigArray<HyperLogLog> hlls) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          hlls.get(groupIds[i]).add(column.getBoolean(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        if (!column.isNull(position)) {
          hlls.get(groupId).add(column.getBoolean(i));
        }
      }
    }
  }

  public void addIntInput(
      int[] groupIds, Column column, AggregationMask mask, ObjectBigArray<HyperLogLog> hlls) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          hlls.get(groupIds[i]).add(column.getInt(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        if (!column.isNull(position)) {
          hlls.get(groupId).add(column.getInt(i));
        }
      }
    }
  }

  public void addLongInput(
      int[] groupIds, Column column, AggregationMask mask, ObjectBigArray<HyperLogLog> hlls) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          hlls.get(groupIds[i]).add(column.getLong(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        if (!column.isNull(position)) {
          hlls.get(groupId).add(column.getLong(i));
        }
      }
    }
  }

  public void addFloatInput(
      int[] groupIds, Column column, AggregationMask mask, ObjectBigArray<HyperLogLog> hlls) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          hlls.get(groupIds[i]).add(column.getFloat(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        if (!column.isNull(position)) {
          hlls.get(groupId).add(column.getFloat(i));
        }
      }
    }
  }

  public void addDoubleInput(
      int[] groupIds, Column column, AggregationMask mask, ObjectBigArray<HyperLogLog> hlls) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          hlls.get(groupIds[i]).add(column.getDouble(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        if (!column.isNull(position)) {
          hlls.get(groupId).add(column.getDouble(i));
        }
      }
    }
  }

  public void addBinaryInput(
      int[] groupIds, Column column, AggregationMask mask, ObjectBigArray<HyperLogLog> hlls) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          hlls.get(groupIds[i]).add(column.getBinary(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        if (!column.isNull(position)) {
          hlls.get(groupId).add(column.getBinary(i));
        }
      }
    }
  }
}
