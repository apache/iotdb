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

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate.Counter;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate.SpaceSaving;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate.SpaceSavingStateFactory;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.List;

public class DoubleApproxMostFrequentAccumulator
    extends AbstractApproxMostFrequentAccumulator<Double> {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(DoubleApproxMostFrequentAccumulator.class);

  public DoubleApproxMostFrequentAccumulator() {}

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE
        + RamUsageEstimator.shallowSizeOfInstance(DoubleApproxMostFrequentAccumulator.class)
        + state.getEstimatedSize();
  }

  @Override
  public TableAccumulator copy() {
    return new DoubleApproxMostFrequentAccumulator();
  }

  @Override
  public void addInput(Column[] arguments, AggregationMask mask) {
    int maxBuckets = arguments[1].getInt(0);
    int capacity = arguments[2].getInt(0);
    if (maxBuckets <= 0 || capacity <= 0) {
      throw new SemanticException(
          "The second and third argument must be greater than 0, but got k="
              + maxBuckets
              + ", capacity="
              + capacity);
    }
    SpaceSaving<Double> spaceSaving = getOrCreateSpaceSaving(state, maxBuckets, capacity);

    int positionCount = mask.getPositionCount();

    Column valueColumn = arguments[0];

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

  @Override
  public void addIntermediate(Column argument) {
    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (!argument.isNull(i)) {
        SpaceSaving<Double> current =
            new SpaceSaving<>(
                argument.getBinary(i).getValues(),
                DoubleApproxMostFrequentAccumulator::serializeBucket,
                DoubleApproxMostFrequentAccumulator::deserializeBucket,
                DoubleApproxMostFrequentAccumulator::calculateKeyByte);
        state.merge(current);
      }
    }
  }

  public static SpaceSaving<Double> getOrCreateSpaceSaving(
      SpaceSavingStateFactory.SingleSpaceSavingState<Double> state, int maxBuckets, int capacity) {
    SpaceSaving<Double> spaceSaving = state.getSpaceSaving();
    if (spaceSaving == null) {
      spaceSaving =
          new SpaceSaving<>(
              maxBuckets,
              capacity,
              DoubleApproxMostFrequentAccumulator::serializeBucket,
              DoubleApproxMostFrequentAccumulator::deserializeBucket,
              DoubleApproxMostFrequentAccumulator::calculateKeyByte);
      state.setSpaceSaving(spaceSaving);
    }
    return spaceSaving;
  }

  public static void serializeBucket(Double key, long count, ByteBuffer output) {
    ReadWriteIOUtils.write(key, output);
    ReadWriteIOUtils.write(count, output);
  }

  public static void deserializeBucket(ByteBuffer input, SpaceSaving<Double> spaceSaving) {
    double key = ReadWriteIOUtils.readDouble(input);
    long count = ReadWriteIOUtils.readLong(input);
    spaceSaving.add(key, count);
  }

  public static int calculateKeyByte(List<Counter<Double>> counters) {
    return counters.stream().mapToInt(counter -> Double.BYTES).sum();
  }
}
