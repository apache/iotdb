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
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.List;

public class BinaryApproxMostFrequentAccumulator
    extends AbstractApproxMostFrequentAccumulator<Binary> {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(BinaryApproxMostFrequentAccumulator.class);

  public BinaryApproxMostFrequentAccumulator() {}

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE
        + RamUsageEstimator.shallowSizeOfInstance(BinaryApproxMostFrequentAccumulator.class)
        + state.getEstimatedSize();
  }

  @Override
  public TableAccumulator copy() {
    return new BinaryApproxMostFrequentAccumulator();
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
    SpaceSaving<Binary> spaceSaving = getOrCreateSpaceSaving(state, maxBuckets, capacity);

    int positionCount = mask.getPositionCount();

    Column valueColumn = arguments[0];

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

  @Override
  public void addIntermediate(Column argument) {
    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (!argument.isNull(i)) {
        SpaceSaving<Binary> current =
            new SpaceSaving<>(
                argument.getBinary(i).getValues(),
                BinaryApproxMostFrequentAccumulator::serializeBucket,
                BinaryApproxMostFrequentAccumulator::deserializeBucket,
                BinaryApproxMostFrequentAccumulator::calculateKeyByte);
        state.merge(current);
      }
    }
  }

  public static SpaceSaving<Binary> getOrCreateSpaceSaving(
      SpaceSavingStateFactory.SingleSpaceSavingState<Binary> state, int maxBuckets, int capacity) {
    SpaceSaving<Binary> spaceSaving = state.getSpaceSaving();
    if (spaceSaving == null) {
      spaceSaving =
          new SpaceSaving<>(
              maxBuckets,
              capacity,
              BinaryApproxMostFrequentAccumulator::serializeBucket,
              BinaryApproxMostFrequentAccumulator::deserializeBucket,
              BinaryApproxMostFrequentAccumulator::calculateKeyByte);
      state.setSpaceSaving(spaceSaving);
    }
    return spaceSaving;
  }

  public static void serializeBucket(Binary key, long count, ByteBuffer output) {
    ReadWriteIOUtils.write(key, output);
    ReadWriteIOUtils.write(count, output);
  }

  public static void deserializeBucket(ByteBuffer input, SpaceSaving<Binary> spaceSaving) {
    Binary key = ReadWriteIOUtils.readBinary(input);
    long count = ReadWriteIOUtils.readLong(input);
    spaceSaving.add(key, count);
  }

  public static int calculateKeyByte(List<Counter<Binary>> counters) {
    return counters.stream().mapToInt(counter -> counter.getItem().getLength()).sum();
  }
}
