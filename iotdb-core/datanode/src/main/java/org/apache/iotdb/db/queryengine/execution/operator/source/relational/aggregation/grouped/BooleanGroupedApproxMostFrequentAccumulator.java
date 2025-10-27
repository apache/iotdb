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
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.AggregationMask;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.BooleanApproxMostFrequentAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate.SpaceSaving;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.SpaceSavingBigArray;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.utils.RamUsageEstimator;

public class BooleanGroupedApproxMostFrequentAccumulator
    extends AbstractGroupedApproxMostFrequentAccumulator<Boolean> {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(BooleanGroupedApproxMostFrequentAccumulator.class);

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE + state.getEstimatedSize();
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    int maxBuckets = arguments[1].getInt(0);
    int capacity = arguments[2].getInt(0);
    if (maxBuckets <= 0 || capacity <= 0) {
      throw new SemanticException(
          "The second and third argument must be greater than 0, but got k="
              + maxBuckets
              + ", capacity="
              + capacity);
    }
    SpaceSavingBigArray<Boolean> spaceSavingBigArray = getOrCreateSpaceSaving(state);
    Column column = arguments[0];

    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        int groupId = groupIds[i];
        SpaceSaving<Boolean> spaceSaving =
            spaceSavingBigArray.get(
                groupId,
                maxBuckets,
                capacity,
                BooleanApproxMostFrequentAccumulator::serializeBucket,
                BooleanApproxMostFrequentAccumulator::deserializeBucket,
                BooleanApproxMostFrequentAccumulator::calculateKeyByte);
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
        SpaceSaving<Boolean> spaceSaving =
            spaceSavingBigArray.get(
                groupId,
                maxBuckets,
                capacity,
                BooleanApproxMostFrequentAccumulator::serializeBucket,
                BooleanApproxMostFrequentAccumulator::deserializeBucket,
                BooleanApproxMostFrequentAccumulator::calculateKeyByte);
        if (!column.isNull(position)) {
          spaceSaving.add(column.getBoolean(position));
        }
      }
    }
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {
    for (int i = 0; i < groupIds.length; i++) {
      if (!argument.isNull(i)) {
        SpaceSaving<Boolean> current =
            new SpaceSaving<>(
                argument.getBinary(i).getValues(),
                BooleanApproxMostFrequentAccumulator::serializeBucket,
                BooleanApproxMostFrequentAccumulator::deserializeBucket,
                BooleanApproxMostFrequentAccumulator::calculateKeyByte);
        state.merge(groupIds[i], current);
      }
    }
  }
}
