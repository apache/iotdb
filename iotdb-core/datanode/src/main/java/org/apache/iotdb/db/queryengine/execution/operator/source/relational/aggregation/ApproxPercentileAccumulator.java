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

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;

public class ApproxPercentileAccumulator extends AbstractApproxPercentileAccumulator {

  public ApproxPercentileAccumulator(TSDataType seriesDataType) {
    super(seriesDataType);
  }

  @Override
  public void addIntInput(Column[] arguments, AggregationMask mask) {

    Column valueColumn = arguments[0];
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < valueColumn.getPositionCount(); i++) {
        if (!valueColumn.isNull(i)) {
          tDigest.add(valueColumn.getInt(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          tDigest.add(valueColumn.getInt(position));
        }
      }
    }
  }

  @Override
  public void addLongInput(Column[] arguments, AggregationMask mask) {

    Column valueColumn = arguments[0];
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < valueColumn.getPositionCount(); i++) {
        if (!valueColumn.isNull(i)) {
          tDigest.add(toDoubleExact(valueColumn.getLong(i)));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          tDigest.add(toDoubleExact(valueColumn.getLong(position)));
        }
      }
    }
  }

  @Override
  public void addFloatInput(Column[] arguments, AggregationMask mask) {

    Column valueColumn = arguments[0];
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < valueColumn.getPositionCount(); i++) {
        if (!valueColumn.isNull(i)) {
          tDigest.add(valueColumn.getFloat(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          tDigest.add(valueColumn.getFloat(position));
        }
      }
    }
  }

  @Override
  public void addDoubleInput(Column[] arguments, AggregationMask mask) {
    Column valueColumn = arguments[0];
    int positionCount = mask.getPositionCount();
    if (mask.isSelectAll()) {
      for (int i = 0; i < valueColumn.getPositionCount(); i++) {
        if (!valueColumn.isNull(i)) {
          tDigest.add(valueColumn.getDouble(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          tDigest.add(valueColumn.getDouble(position));
        }
      }
    }
  }
}
