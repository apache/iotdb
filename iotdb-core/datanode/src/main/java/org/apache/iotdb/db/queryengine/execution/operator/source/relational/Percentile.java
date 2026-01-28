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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.db.exception.sql.SemanticException;

import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Percentile {
  private double[] values;
  private int size;
  private int capacity;
  private boolean sorted;

  private static final int INITIAL_CAPACITY = 32;
  private static final double GROWTH_FACTOR = 1.5;

  public Percentile() {
    this.capacity = INITIAL_CAPACITY;
    this.values = new double[capacity];
    this.size = 0;
    this.sorted = true;
  }

  public void addValue(double value) {
    ensureCapacity();
    values[size++] = value;
    sorted = false;
  }

  public void addValues(double... vals) {
    if (vals == null || vals.length == 0) return;

    int newSize = size + vals.length;
    if (newSize > capacity) {
      grow(newSize);
    }

    System.arraycopy(vals, 0, values, size, vals.length);
    size = newSize;
    sorted = false;
  }

  public void merge(Percentile other) {
    if (other == null || other.size == 0) {
      return;
    }

    int newSize = size + other.size;
    if (newSize > capacity) {
      grow(newSize);
    }

    System.arraycopy(other.values, 0, values, size, other.size);
    size = newSize;
    sorted = false;
  }

  public double getPercentile(double percentile) {
    if (size == 0) {
      return Double.NaN;
    }
    if (percentile < 0.0 || percentile > 1.0) {
      throw new SemanticException("percentage should be in [0,1], got " + percentile);
    }

    ensureSorted();

    if (size == 1) {
      return values[0];
    }

    double realIndex = percentile * (size - 1);
    int index = (int) realIndex;
    double fraction = realIndex - index;

    if (index >= size - 1) {
      return values[size - 1];
    }

    return values[index] + fraction * (values[index + 1] - values[index]);
  }

  public int getSize() {
    return size;
  }

  public void clear() {
    size = 0;
    sorted = true;
  }

  private void ensureCapacity() {
    if (size >= capacity) {
      grow(size + 1);
    }
  }

  private void grow(int minCapacity) {
    int newCapacity = Math.max((int) (capacity * GROWTH_FACTOR), minCapacity);
    double[] newValues = new double[newCapacity];
    System.arraycopy(values, 0, newValues, 0, size);
    values = newValues;
    capacity = newCapacity;
  }

  private void ensureSorted() {
    if (!sorted && size > 1) {
      Arrays.sort(values, 0, size);
      sorted = true;
    }
  }

  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(size, buffer);
    ReadWriteIOUtils.write(capacity, buffer);
    ReadWriteIOUtils.write(sorted, buffer);
    for (int i = 0; i < size; i++) {
      ReadWriteIOUtils.write(values[i], buffer);
    }
  }

  public static Percentile deserialize(ByteBuffer buffer) {
    Percentile percentile = new Percentile();
    percentile.size = ReadWriteIOUtils.readInt(buffer);
    percentile.capacity = ReadWriteIOUtils.readInt(buffer);
    percentile.sorted = ReadWriteIOUtils.readBool(buffer);

    if (percentile.capacity != percentile.values.length) {
      percentile.values = new double[percentile.capacity];
    }

    for (int i = 0; i < percentile.size; i++) {
      percentile.values[i] = ReadWriteIOUtils.readDouble(buffer);
    }

    return percentile;
  }

  public int getSerializedSize() {
    return Integer.BYTES + Integer.BYTES + 1 + (size * Double.BYTES);
  }

  public long getEstimatedSize() {
    long shallowSize = RamUsageEstimator.shallowSizeOfInstance(Percentile.class);
    return shallowSize + getSerializedSize();
  }
}
