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

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate.TDigest;

import static org.apache.tsfile.utils.RamUsageEstimator.shallowSizeOf;
import static org.apache.tsfile.utils.RamUsageEstimator.shallowSizeOfInstance;

public final class TDigestBigArray {
  private static final long INSTANCE_SIZE = shallowSizeOfInstance(TDigestBigArray.class);
  private final ObjectBigArray<TDigest> array;
  private long sizeOfTDigest;

  public TDigestBigArray() {
    array = new ObjectBigArray<>();
  }

  public long sizeOf() {
    return INSTANCE_SIZE + shallowSizeOf(array) + sizeOfTDigest;
  }

  public TDigest get(long index) {
    TDigest tDigest = array.get(index);
    if (tDigest == null) {
      tDigest = new TDigest();
      set(index, tDigest);
    }
    return tDigest;
  }

  public void set(long index, TDigest value) {
    updateRetainedSize(index, value);
    array.set(index, value);
  }

  public boolean isEmpty() {
    return sizeOfTDigest == 0;
  }

  public void ensureCapacity(long length) {
    array.ensureCapacity(length);
  }

  public void updateRetainedSize(long index, TDigest value) {
    TDigest tDigest = array.get(index);
    if (tDigest != null) {
      sizeOfTDigest -= tDigest.getEstimatedSize();
    }
    if (value != null) {
      sizeOfTDigest += value.getEstimatedSize();
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
