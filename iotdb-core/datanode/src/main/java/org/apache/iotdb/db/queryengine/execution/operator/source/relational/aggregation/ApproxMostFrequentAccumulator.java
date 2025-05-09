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

import com.google.gson.Gson;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.nio.charset.StandardCharsets;

public class ApproxMostFrequentAccumulator implements TableAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ApproxMostFrequentAccumulator.class);
  private final TSDataType seriesDataType;
  private final SpaceSavingStateFactory.SingleSpaceSavingState state =
      SpaceSavingStateFactory.createSingleState();

  public ApproxMostFrequentAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE
        + RamUsageEstimator.shallowSizeOfInstance(ApproxMostFrequentAccumulator.class)
        + state.getEstimatedSize();
  }

  @Override
  public TableAccumulator copy() {
    return new ApproxMostFrequentAccumulator(seriesDataType);
  }

  @Override
  public void addInput(Column[] arguments, AggregationMask mask) {
    int maxBuckets = arguments[0].getInt(0);
    int capacity = arguments[2].getInt(0);
    SpaceSaving spaceSaving = getOrCreateSpaceSaving(state, maxBuckets, capacity);

    switch (seriesDataType) {
      case INT32:
      case DATE:
        addIntInput(arguments[1], mask, spaceSaving);
        return;
      case INT64:
      case TIMESTAMP:
        addLongInput(arguments[1], mask, spaceSaving);
        return;
      case FLOAT:
        addFloatInput(arguments[1], mask, spaceSaving);
        return;
      case DOUBLE:
        addDoubleInput(arguments[1], mask, spaceSaving);
        return;
      case TEXT:
      case STRING:
      case BLOB:
        addBinaryInput(arguments[1], mask, spaceSaving);
        return;
      case BOOLEAN:
        addBooleanInput(arguments[1], mask, spaceSaving);
        return;
      default:
        throw new UnSupportedDataTypeException(
            String.format(
                "Unsupported data type in APPROX_COUNT_DISTINCT Aggregation: %s", seriesDataType));
    }
  }

  @Override
  public void addIntermediate(Column argument) {
    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (!argument.isNull(i)) {
        SpaceSaving current = new SpaceSaving(argument.getBinary(i).getValues());
        state.merge(current);
      }
    }
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    columnBuilder.writeBinary(new Binary(state.getSpaceSaving().serialize()));
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    columnBuilder.writeBinary(
        new Binary(new Gson().toJson(state.getSpaceSaving().getBuckets()), StandardCharsets.UTF_8));
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public void addStatistics(Statistics[] statistics) {
    throw new UnsupportedOperationException(
        "ApproxMostFrequentAccumulator does not support statistics");
  }

  @Override
  public void reset() {
    state.getSpaceSaving().reset();
  }

  public void addBooleanInput(Column valueColumn, AggregationMask mask, SpaceSaving spaceSaving) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < valueColumn.getPositionCount(); i++) {
        if (!valueColumn.isNull(i)) {
          spaceSaving.add(valueColumn.getBoolean(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          spaceSaving.add(valueColumn.getBoolean(position));
        }
      }
    }
  }

  public void addIntInput(Column valueColumn, AggregationMask mask, SpaceSaving spaceSaving) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < valueColumn.getPositionCount(); i++) {
        if (!valueColumn.isNull(i)) {
          spaceSaving.add(valueColumn.getInt(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          spaceSaving.add(valueColumn.getInt(position));
        }
      }
    }
  }

  public void addLongInput(Column valueColumn, AggregationMask mask, SpaceSaving spaceSaving) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < valueColumn.getPositionCount(); i++) {
        if (!valueColumn.isNull(i)) {
          spaceSaving.add(valueColumn.getLong(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          spaceSaving.add(valueColumn.getLong(position));
        }
      }
    }
  }

  public void addFloatInput(Column valueColumn, AggregationMask mask, SpaceSaving spaceSaving) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < valueColumn.getPositionCount(); i++) {
        if (!valueColumn.isNull(i)) {
          spaceSaving.add(valueColumn.getFloat(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          spaceSaving.add(valueColumn.getFloat(position));
        }
      }
    }
  }

  public void addDoubleInput(Column valueColumn, AggregationMask mask, SpaceSaving spaceSaving) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < valueColumn.getPositionCount(); i++) {
        if (!valueColumn.isNull(i)) {
          spaceSaving.add(valueColumn.getDouble(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          spaceSaving.add(valueColumn.getDouble(position));
        }
      }
    }
  }

  public void addBinaryInput(Column valueColumn, AggregationMask mask, SpaceSaving spaceSaving) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < valueColumn.getPositionCount(); i++) {
        if (!valueColumn.isNull(i)) {
          spaceSaving.add(valueColumn.getBinary(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          spaceSaving.add(valueColumn.getBinary(position));
        }
      }
    }
  }

  public static SpaceSaving getOrCreateSpaceSaving(
      SpaceSavingStateFactory.SingleSpaceSavingState state, int maxBuckets, int capacity) {
    SpaceSaving spaceSaving = state.getSpaceSaving();
    if (spaceSaving == null) {
      spaceSaving = new SpaceSaving(maxBuckets, capacity);
      state.setSpaceSaving(spaceSaving);
    }
    return spaceSaving;
  }
}
