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

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate.HyperLogLog;

import static org.apache.tsfile.utils.RamUsageEstimator.shallowSizeOf;
import static org.apache.tsfile.utils.RamUsageEstimator.shallowSizeOfInstance;

public final class HyperLogLogBigArray {
  private static final long INSTANCE_SIZE = shallowSizeOfInstance(HyperLogLogBigArray.class);
  private final ObjectBigArray<HyperLogLog> array;
  private long sizeOfHyperLogLog;

  public HyperLogLogBigArray() {
    array = new ObjectBigArray<>();
  }

  public long sizeOf() {
    return INSTANCE_SIZE + shallowSizeOf(array) + sizeOfHyperLogLog;
  }

  public HyperLogLog get(long index) {
    // Only use if certain that the object exists.
    return array.get(index);
  }

  public HyperLogLog get(long index, double maxStandardError) {
    return get(index, new HyperLogLog(maxStandardError));
  }

  public HyperLogLog get(long index, HyperLogLog hll) {
    HyperLogLog result = array.get(index);
    if (result == null) {
      set(index, hll);
      return hll;
    }
    return result;
  }

  public void set(long index, HyperLogLog hll) {
    updateRetainedSize(index, hll);
    array.set(index, hll);
  }

  public boolean isEmpty() {
    return sizeOfHyperLogLog == 0;
  }

  public void ensureCapacity(long length) {
    array.ensureCapacity(length);
  }

  public void updateRetainedSize(long index, HyperLogLog value) {
    HyperLogLog hll = array.get(index);
    if (hll != null) {
      sizeOfHyperLogLog -= hll.getEstimatedSize();
    }
    if (value != null) {
      sizeOfHyperLogLog += value.getEstimatedSize();
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
