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

import org.apache.tsfile.utils.Binary;

import static org.apache.tsfile.utils.RamUsageEstimator.shallowSizeOfInstance;
import static org.apache.tsfile.utils.RamUsageEstimator.sizeOfObject;

public final class BinaryBigArray {
  private static final long INSTANCE_SIZE = shallowSizeOfInstance(BinaryBigArray.class);
  private final ObjectBigArray<Binary> array;
  private long sizeOfBinarys;

  public BinaryBigArray() {
    array = new ObjectBigArray<>();
  }

  public BinaryBigArray(Binary slice) {
    array = new ObjectBigArray<>(slice);
  }

  /** Returns the size of this big array in bytes. */
  public long sizeOf() {
    return INSTANCE_SIZE + array.sizeOf() + sizeOfBinarys;
  }

  /**
   * Returns the element of this big array at specified index.
   *
   * @param index a position in this big array.
   * @return the element of this big array at the specified position.
   */
  public Binary get(long index) {
    return array.get(index);
  }

  /**
   * Sets the element of this big array at specified index.
   *
   * @param index a position in this big array.
   */
  public void set(long index, Binary value) {
    updateRetainedSize(index, value);
    array.set(index, value);
  }

  /**
   * Ensures this big array is at least the specified length. If the array is smaller, segments are
   * added until the array is larger then the specified length.
   */
  public void ensureCapacity(long length) {
    array.ensureCapacity(length);
  }

  private void updateRetainedSize(long index, Binary value) {
    Binary currentValue = array.get(index);
    if (currentValue != null) {
      sizeOfBinarys -= sizeOfObject(currentValue);
    }
    if (value != null) {
      sizeOfBinarys += sizeOfObject(value);
    }
  }

  public void reset() {
    array.reset();
  }
}
