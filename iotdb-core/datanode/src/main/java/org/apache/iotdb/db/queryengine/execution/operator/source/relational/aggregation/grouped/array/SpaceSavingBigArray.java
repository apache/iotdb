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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array;

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate.ApproxMostFrequentBucketDeserializer;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate.ApproxMostFrequentBucketSerializer;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate.SpaceSaving;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate.SpaceSavingByteCalculator;

import static org.apache.tsfile.utils.RamUsageEstimator.shallowSizeOf;
import static org.apache.tsfile.utils.RamUsageEstimator.shallowSizeOfInstance;

public class SpaceSavingBigArray<T> {
  private static final long INSTANCE_SIZE = shallowSizeOfInstance(SpaceSavingBigArray.class);
  private final ObjectBigArray<SpaceSaving<T>> array;
  private long sizeOfSpaceSaving;

  public SpaceSavingBigArray() {
    array = new ObjectBigArray<>();
  }

  public long sizeOf() {
    return INSTANCE_SIZE + shallowSizeOf(array) + sizeOfSpaceSaving;
  }

  public SpaceSaving<T> get(long index) {
    return array.get(index);
  }

  public SpaceSaving<T> get(
      long index,
      int maxBuckets,
      int capacity,
      ApproxMostFrequentBucketSerializer<T> serializer,
      ApproxMostFrequentBucketDeserializer<T> deserializer,
      SpaceSavingByteCalculator<T> calculator) {
    return get(
        index, new SpaceSaving<T>(maxBuckets, capacity, serializer, deserializer, calculator));
  }

  public SpaceSaving<T> get(long index, SpaceSaving<T> spaceSaving) {
    SpaceSaving<T> result = array.get(index);
    if (result == null) {
      set(index, spaceSaving);
      return spaceSaving;
    }
    return result;
  }

  public void set(long index, SpaceSaving<T> spaceSaving) {
    updateRetainedSize(index, spaceSaving);
    array.set(index, spaceSaving);
  }

  public boolean isEmpty() {
    return sizeOfSpaceSaving == 0;
  }

  public void ensureCapacity(long length) {
    array.ensureCapacity(length);
  }

  public void updateRetainedSize(long index, SpaceSaving<T> value) {
    SpaceSaving<T> spaceSaving = array.get(index);
    if (spaceSaving != null) {
      sizeOfSpaceSaving -= spaceSaving.getEstimatedSize();
    }
    if (value != null) {
      sizeOfSpaceSaving += value.getEstimatedSize();
    }
  }

  public void reset() {
    array.forEach(
        item -> {
          if (item != null) {
            item.reset();
          }
        });
  }
}
