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
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate.HyperLogLog;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate.HyperLogLogStateFactory;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.HyperLogLogBigArray;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate.HyperLogLog.DEFAULT_STANDARD_ERROR;

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
    return INSTANCE_SIZE + state.getEstimatedSize();
  }

  @Override
  public void setGroupCount(long groupCount) {
    HyperLogLogBigArray hlls = state.getHyperLogLogs();
    hlls.ensureCapacity(groupCount);
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    double maxStandardError =
        arguments.length == 1 ? DEFAULT_STANDARD_ERROR : arguments[1].getDouble(0);
    HyperLogLogBigArray hlls = getOrCreateHyperLogLog(state);

    switch (seriesDataType) {
      case BOOLEAN:
        addBooleanInput(groupIds, arguments[0], mask, hlls, maxStandardError);
        return;
      case INT32:
      case DATE:
        addIntInput(groupIds, arguments[0], mask, hlls, maxStandardError);
        return;
      case INT64:
      case TIMESTAMP:
        addLongInput(groupIds, arguments[0], mask, hlls, maxStandardError);
        return;
      case FLOAT:
        addFloatInput(groupIds, arguments[0], mask, hlls, maxStandardError);
        return;
      case DOUBLE:
        addDoubleInput(groupIds, arguments[0], mask, hlls, maxStandardError);
        return;
      case TEXT:
      case STRING:
      case BLOB:
      case OBJECT:
        addBinaryInput(groupIds, arguments[0], mask, hlls, maxStandardError);
        return;
      default:
        throw new UnSupportedDataTypeException(
            String.format(
                "Unsupported data type in APPROX_COUNT_DISTINCT Aggregation: %s", seriesDataType));
    }
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {
    for (int i = 0; i < groupIds.length; i++) {
      int groupId = groupIds[i];
      if (!argument.isNull(i)) {
        HyperLogLog current = new HyperLogLog(argument.getBinary(i).getValues());
        state.merge(groupId, current);
      }
    }
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder columnBuilder) {
    HyperLogLogBigArray hlls = state.getHyperLogLogs();
    columnBuilder.writeBinary(new Binary(hlls.get(groupId).serialize()));
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
    HyperLogLogBigArray hlls = state.getHyperLogLogs();
    columnBuilder.writeLong(hlls.get(groupId).cardinality());
  }

  @Override
  public void prepareFinal() {}

  @Override
  public void reset() {
    state.getHyperLogLogs().reset();
  }

  public void addBooleanInput(
      int[] groupIds,
      Column column,
      AggregationMask mask,
      HyperLogLogBigArray hlls,
      double maxStandardError) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        int groupId = groupIds[i];
        HyperLogLog hll = hlls.get(groupId, maxStandardError);
        if (!column.isNull(i)) {
          hll.add(column.getBoolean(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        HyperLogLog hll = hlls.get(groupId, maxStandardError);
        if (!column.isNull(position)) {
          hll.add(column.getBoolean(position));
        }
      }
    }
  }

  public void addIntInput(
      int[] groupIds,
      Column column,
      AggregationMask mask,
      HyperLogLogBigArray hlls,
      double maxStandardError) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        int groupId = groupIds[i];
        HyperLogLog hll = hlls.get(groupId, maxStandardError);
        if (!column.isNull(i)) {
          hll.add(column.getInt(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        HyperLogLog hll = hlls.get(groupId, maxStandardError);
        if (!column.isNull(position)) {
          hll.add(column.getInt(position));
        }
      }
    }
  }

  public void addLongInput(
      int[] groupIds,
      Column column,
      AggregationMask mask,
      HyperLogLogBigArray hlls,
      double maxStandardError) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        int groupId = groupIds[i];
        HyperLogLog hll = hlls.get(groupId, maxStandardError);
        if (!column.isNull(i)) {
          hll.add(column.getLong(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        HyperLogLog hll = hlls.get(groupId, maxStandardError);
        if (!column.isNull(position)) {
          hll.add(column.getLong(position));
        }
      }
    }
  }

  public void addFloatInput(
      int[] groupIds,
      Column column,
      AggregationMask mask,
      HyperLogLogBigArray hlls,
      double maxStandardError) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        int groupId = groupIds[i];
        HyperLogLog hll = hlls.get(groupId, maxStandardError);
        if (!column.isNull(i)) {
          hll.add(column.getFloat(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        HyperLogLog hll = hlls.get(groupId, maxStandardError);
        if (!column.isNull(position)) {
          hll.add(column.getFloat(position));
        }
      }
    }
  }

  public void addDoubleInput(
      int[] groupIds,
      Column column,
      AggregationMask mask,
      HyperLogLogBigArray hlls,
      double maxStandardError) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        int groupId = groupIds[i];
        HyperLogLog hll = hlls.get(groupId, maxStandardError);
        if (!column.isNull(i)) {
          hll.add(column.getDouble(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        HyperLogLog hll = hlls.get(groupId, maxStandardError);
        if (!column.isNull(position)) {
          hll.add(column.getDouble(position));
        }
      }
    }
  }

  public void addBinaryInput(
      int[] groupIds,
      Column column,
      AggregationMask mask,
      HyperLogLogBigArray hlls,
      double maxStandardError) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        int groupId = groupIds[i];
        HyperLogLog hll = hlls.get(groupId, maxStandardError);
        if (!column.isNull(i)) {
          hll.add(column.getBinary(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        HyperLogLog hll = hlls.get(groupId, maxStandardError);
        if (!column.isNull(position)) {
          hll.add(column.getBinary(position));
        }
      }
    }
  }

  public static HyperLogLogBigArray getOrCreateHyperLogLog(
      HyperLogLogStateFactory.GroupedHyperLogLogState state) {
    if (state.isEmpty()) {
      state.setHyperLogLogs(new HyperLogLogBigArray());
    }
    return state.getHyperLogLogs();
  }
}
