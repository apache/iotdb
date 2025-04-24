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
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.HyperLogLogBigArray;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.HyperLogLog.DEFAULT_STANDARD_ERROR;

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
    HyperLogLogBigArray hlls = state.getHyperLogLogs();
    return INSTANCE_SIZE + hlls.sizeOf();
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
    HyperLogLogBigArray hlls = HyperLogLogStateFactory.getOrCreateHyperLogLog(state);

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

    if (groupIds.length == 0) {
      return;
    }

    HyperLogLogBigArray merged = new HyperLogLogBigArray();
    for (int i = 0; i < groupIds.length; i++) {
      int groupId = groupIds[i];
      if (!argument.isNull(i)) {
        HyperLogLog current = new HyperLogLog(argument.getBinary(i).getValues());

        if (merged.get(groupId) == null) {
          merged.set(groupId, current);
        } else {
          HyperLogLog hll = merged.get(groupId);
          hll.merge(current);
          merged.set(groupId, hll);
        }
      }
    }

    if (state.isAllNull()) {
      state.setHyperLogLogs(merged);
    } else {
      HyperLogLogBigArray hlls = state.getHyperLogLogs();
      for (int groupId : groupIds) {
        if (hlls.get(groupId) == null) {
          hlls.set(groupId, merged.get(groupId));
        } else {
          HyperLogLog hll = new HyperLogLog(hlls.get(groupId).serialize());
          hll.merge(merged.get(groupId));
          hlls.set(groupId, hll);
        }
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
    HyperLogLogBigArray hlls = state.getHyperLogLogs();
    hlls.reset();
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
        hlls.setIfNull(groupId, maxStandardError);
        if (!column.isNull(i)) {
          hlls.get(groupId).add(column.getBoolean(i));
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
          hlls.get(groupId, maxStandardError).add(column.getBoolean(i));
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
        hlls.setIfNull(groupId, maxStandardError);
        if (!column.isNull(i)) {
          hlls.get(groupId).add(column.getInt(i));
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
          hlls.get(groupId, maxStandardError).add(column.getInt(i));
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
        hlls.setIfNull(groupId, maxStandardError);
        if (!column.isNull(i)) {
          hlls.get(groupId).add(column.getLong(i));
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
          hlls.get(groupId, maxStandardError).add(column.getLong(i));
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
        hlls.setIfNull(groupId, maxStandardError);
        if (!column.isNull(i)) {
          hlls.get(groupId).add(column.getFloat(i));
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
          hlls.get(groupId, maxStandardError).add(column.getFloat(i));
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
        hlls.setIfNull(groupId, maxStandardError);
        if (!column.isNull(i)) {
          hlls.get(groupId).add(column.getDouble(i));
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
          hlls.get(groupId, maxStandardError).add(column.getDouble(i));
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
        hlls.setIfNull(groupId, maxStandardError);
        if (!column.isNull(i)) {
          hlls.get(groupId).add(column.getBinary(i));
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
          hlls.get(groupId, maxStandardError).add(column.getBinary(i));
        }
      }
    }
  }
}
