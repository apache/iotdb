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
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.SpaceSaving;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.SpaceSavingStateFactory;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.SpaceSavingBigArray;

import com.google.gson.Gson;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.nio.charset.StandardCharsets;

public class GroupedApproxMostFrequentAccumulator implements GroupedAccumulator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedApproxMostFrequentAccumulator.class);
  private final TSDataType seriesDataType;
  private final SpaceSavingStateFactory.GroupedSpaceSavingState state =
      SpaceSavingStateFactory.createGroupedState();

  public GroupedApproxMostFrequentAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE + state.getEstimatedSize();
  }

  @Override
  public void setGroupCount(long groupCount) {
    state.getSpaceSavings().ensureCapacity(groupCount);
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    int maxBuckets = arguments[0].getInt(0);
    int capacity = arguments[2].getInt(0);
    SpaceSavingBigArray spaceSavingBigArray = getOrCreateSpaceSaving(state);

    switch (seriesDataType) {
      case BOOLEAN:
        addBooleanInput(groupIds, arguments[0], mask, spaceSavingBigArray, maxBuckets, capacity);
        break;
      case INT32:
      case DATE:
        addIntInput(groupIds, arguments[0], mask, spaceSavingBigArray, maxBuckets, capacity);
        break;
      case INT64:
      case TIMESTAMP:
        addLongInput(groupIds, arguments[0], mask, spaceSavingBigArray, maxBuckets, capacity);
        break;
      case FLOAT:
        addFloatInput(groupIds, arguments[0], mask, spaceSavingBigArray, maxBuckets, capacity);
        break;
      case DOUBLE:
        addDoubleInput(groupIds, arguments[0], mask, spaceSavingBigArray, maxBuckets, capacity);
        break;
      case TEXT:
      case STRING:
      case BLOB:
        addBinaryInput(groupIds, arguments[0], mask, spaceSavingBigArray, maxBuckets, capacity);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format(
                "Unsupported data type in APPROX_COUNT_DISTINCT Aggregation: %s", seriesDataType));
    }
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {
    for (int i = 0; i < groupIds.length; i++) {
      if (!argument.isNull(i)) {
        SpaceSaving current = new SpaceSaving(argument.getBinary(i).getValues());
        state.merge(groupIds[i], current);
      }
    }
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder columnBuilder) {
    SpaceSavingBigArray spaceSavingBigArray = state.getSpaceSavings();
    columnBuilder.writeBinary(new Binary(spaceSavingBigArray.get(groupId).serialize()));
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
    SpaceSavingBigArray spaceSavingBigArray = state.getSpaceSavings();
    Binary result =
        new Binary(
            new Gson().toJson(spaceSavingBigArray.get(groupId).getBuckets()),
            StandardCharsets.UTF_8);
    columnBuilder.writeBinary(result);
  }

  @Override
  public void prepareFinal() {}

  @Override
  public void reset() {
    state.getSpaceSavings().reset();
  }

  public void addBooleanInput(
      int[] groupIds,
      Column column,
      AggregationMask mask,
      SpaceSavingBigArray spaceSavingBigArray,
      int maxBuckets,
      int capacity) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        int groupId = groupIds[i];
        SpaceSaving spaceSaving = spaceSavingBigArray.get(groupId, maxBuckets, capacity);
        if (!column.isNull(i)) {
          spaceSaving.add(column.getBoolean(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        SpaceSaving spaceSaving = spaceSavingBigArray.get(groupId, maxBuckets, capacity);
        if (!column.isNull(position)) {
          spaceSaving.add(column.getBoolean(position));
        }
      }
    }
  }

  public void addIntInput(
      int[] groupIds,
      Column column,
      AggregationMask mask,
      SpaceSavingBigArray spaceSavingBigArray,
      int maxBuckets,
      int capacity) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        int groupId = groupIds[i];
        SpaceSaving spaceSaving = spaceSavingBigArray.get(groupId, maxBuckets, capacity);
        if (!column.isNull(i)) {
          spaceSaving.add(column.getInt(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        SpaceSaving spaceSaving = spaceSavingBigArray.get(groupId, maxBuckets, capacity);
        if (!column.isNull(position)) {
          spaceSaving.add(column.getInt(position));
        }
      }
    }
  }

  public void addLongInput(
      int[] groupIds,
      Column column,
      AggregationMask mask,
      SpaceSavingBigArray spaceSavingBigArray,
      int maxBuckets,
      int capacity) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        int groupId = groupIds[i];
        SpaceSaving spaceSaving = spaceSavingBigArray.get(groupId, maxBuckets, capacity);
        if (!column.isNull(i)) {
          spaceSaving.add(column.getLong(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        SpaceSaving spaceSaving = spaceSavingBigArray.get(groupId, maxBuckets, capacity);
        if (!column.isNull(position)) {
          spaceSaving.add(column.getLong(position));
        }
      }
    }
  }

  public void addFloatInput(
      int[] groupIds,
      Column column,
      AggregationMask mask,
      SpaceSavingBigArray spaceSavingBigArray,
      int maxBuckets,
      int capacity) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        int groupId = groupIds[i];
        SpaceSaving spaceSaving = spaceSavingBigArray.get(groupId, maxBuckets, capacity);
        if (!column.isNull(i)) {
          spaceSaving.add(column.getFloat(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        SpaceSaving spaceSaving = spaceSavingBigArray.get(groupId, maxBuckets, capacity);
        if (!column.isNull(position)) {
          spaceSaving.add(column.getFloat(position));
        }
      }
    }
  }

  public void addDoubleInput(
      int[] groupIds,
      Column column,
      AggregationMask mask,
      SpaceSavingBigArray spaceSavingBigArray,
      int maxBuckets,
      int capacity) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        int groupId = groupIds[i];
        SpaceSaving spaceSaving = spaceSavingBigArray.get(groupId, maxBuckets, capacity);
        if (!column.isNull(i)) {
          spaceSaving.add(column.getDouble(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        SpaceSaving spaceSaving = spaceSavingBigArray.get(groupId, maxBuckets, capacity);
        if (!column.isNull(position)) {
          spaceSaving.add(column.getDouble(position));
        }
      }
    }
  }

  public void addBinaryInput(
      int[] groupIds,
      Column column,
      AggregationMask mask,
      SpaceSavingBigArray spaceSavingBigArray,
      int maxBuckets,
      int capacity) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        int groupId = groupIds[i];
        SpaceSaving spaceSaving = spaceSavingBigArray.get(groupId, maxBuckets, capacity);
        if (!column.isNull(i)) {
          spaceSaving.add(column.getBinary(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        SpaceSaving spaceSaving = spaceSavingBigArray.get(groupId, maxBuckets, capacity);
        if (!column.isNull(position)) {
          spaceSaving.add(column.getBinary(position));
        }
      }
    }
  }

  public static SpaceSavingBigArray getOrCreateSpaceSaving(
      SpaceSavingStateFactory.GroupedSpaceSavingState state) {
    if (state.isEmpty()) {
      state.setSpaceSavings(new SpaceSavingBigArray());
    }
    return state.getSpaceSavings();
  }
}
