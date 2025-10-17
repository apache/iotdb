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

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.Percentile;

import static org.apache.tsfile.utils.RamUsageEstimator.shallowSizeOf;
import static org.apache.tsfile.utils.RamUsageEstimator.shallowSizeOfInstance;

public final class PercentileBigArray {
  private static final long INSTANCE_SIZE = shallowSizeOfInstance(PercentileBigArray.class);
  private final ObjectBigArray<Percentile> array;
  private long sizeOfTDigest;

  public PercentileBigArray() {
    array = new ObjectBigArray<>();
  }

  public long sizeOf() {
    return INSTANCE_SIZE + shallowSizeOf(array) + sizeOfTDigest;
  }

  public Percentile get(long index) {
    Percentile percentile = array.get(index);
    if (percentile == null) {
      percentile = new Percentile();
      set(index, percentile);
    }
    return percentile;
  }

  public void set(long index, Percentile value) {
    updateRetainedSize(index, value);
    array.set(index, value);
  }

  public boolean isEmpty() {
    return sizeOfTDigest == 0;
  }

  public void ensureCapacity(long length) {
    array.ensureCapacity(length);
  }

  public void updateRetainedSize(long index, Percentile value) {
    Percentile percentile = array.get(index);
    if (percentile != null) {
      sizeOfTDigest -= percentile.getEstimatedSize();
    }
    if (value != null) {
      sizeOfTDigest += value.getEstimatedSize();
    }
  }

  public void reset() {
    array.forEach(
        item -> {
          if (item != null) {
            item.clear();
          }
        });
  }
}
