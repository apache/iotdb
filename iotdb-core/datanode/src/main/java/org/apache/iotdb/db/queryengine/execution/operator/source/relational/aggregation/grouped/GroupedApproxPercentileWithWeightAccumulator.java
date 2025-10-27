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
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate.TDigest;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;

public class GroupedApproxPercentileWithWeightAccumulator
    extends AbstractGroupedApproxPercentileAccumulator {

  public GroupedApproxPercentileWithWeightAccumulator(TSDataType seriesDataType) {
    super(seriesDataType);
  }

  @Override
  public void addIntInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    Column valueColumn = arguments[0];
    Column weightColumn = arguments[1];

    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        int groupId = groupIds[i];
        TDigest tDigest = array.get(groupId);
        if (!valueColumn.isNull(i)) {
          tDigest.add(valueColumn.getInt(i), weightColumn.getInt(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        TDigest tDigest = array.get(groupId);
        if (!valueColumn.isNull(position)) {
          tDigest.add(valueColumn.getInt(position), weightColumn.getInt(position));
        }
      }
    }
  }

  @Override
  public void addLongInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    Column valueColumn = arguments[0];
    Column weightColumn = arguments[1];

    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        int groupId = groupIds[i];
        TDigest tDigest = array.get(groupId);
        if (!valueColumn.isNull(i)) {
          tDigest.add(valueColumn.getLong(i), weightColumn.getInt(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        TDigest tDigest = array.get(groupId);
        if (!valueColumn.isNull(position)) {
          tDigest.add(valueColumn.getLong(position), weightColumn.getInt(position));
        }
      }
    }
  }

  @Override
  public void addFloatInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    Column valueColumn = arguments[0];
    Column weightColumn = arguments[1];

    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        int groupId = groupIds[i];
        TDigest tDigest = array.get(groupId);
        if (!valueColumn.isNull(i)) {
          tDigest.add(valueColumn.getFloat(i), weightColumn.getInt(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        TDigest tDigest = array.get(groupId);
        if (!valueColumn.isNull(position)) {
          tDigest.add(valueColumn.getFloat(position), weightColumn.getInt(position));
        }
      }
    }
  }

  @Override
  public void addDoubleInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    Column valueColumn = arguments[0];
    Column weightColumn = arguments[1];

    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        int groupId = groupIds[i];
        TDigest tDigest = array.get(groupId);
        if (!valueColumn.isNull(i)) {
          tDigest.add(valueColumn.getDouble(i), weightColumn.getInt(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      int groupId;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        groupId = groupIds[position];
        TDigest tDigest = array.get(groupId);
        if (!valueColumn.isNull(position)) {
          tDigest.add(valueColumn.getDouble(position), weightColumn.getInt(position));
        }
      }
    }
  }
}
