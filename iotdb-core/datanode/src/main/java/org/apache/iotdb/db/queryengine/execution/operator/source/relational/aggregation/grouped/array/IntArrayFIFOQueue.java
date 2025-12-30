/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.NoSuchElementException;

public class IntArrayFIFOQueue implements Serializable {
  private static final long serialVersionUID = 0L;
  public static final int INITIAL_CAPACITY = 4;
  protected transient int[] array;
  protected transient int length;
  protected transient int start;
  protected transient int end;

  public IntArrayFIFOQueue(int capacity) {
    if (capacity > 2147483638) {
      throw new IllegalArgumentException(
          "Initial capacity (" + capacity + ") exceeds " + 2147483638);
    } else if (capacity < 0) {
      throw new IllegalArgumentException("Initial capacity (" + capacity + ") is negative");
    } else {
      this.array = new int[Math.max(1, capacity + 1)];
      this.length = this.array.length;
    }
  }

  public IntArrayFIFOQueue() {
    this(4);
  }

  public IntComparator comparator() {
    return null;
  }

  public int dequeueInt() {
    if (this.start == this.end) {
      throw new NoSuchElementException();
    } else {
      int t = this.array[this.start];
      if (++this.start == this.length) {
        this.start = 0;
      }

      this.reduce();
      return t;
    }
  }

  public int dequeueLastInt() {
    if (this.start == this.end) {
      throw new NoSuchElementException();
    } else {
      if (this.end == 0) {
        this.end = this.length;
      }

      int t = this.array[--this.end];
      this.reduce();
      return t;
    }
  }

  private final void resize(int size, int newLength) {
    int[] newArray = new int[newLength];
    if (this.start >= this.end) {
      if (size != 0) {
        System.arraycopy(this.array, this.start, newArray, 0, this.length - this.start);
        System.arraycopy(this.array, 0, newArray, this.length - this.start, this.end);
      }
    } else {
      System.arraycopy(this.array, this.start, newArray, 0, this.end - this.start);
    }

    this.start = 0;
    this.end = size;
    this.array = newArray;
    this.length = newLength;
  }

  private final void expand() {
    this.resize(this.length, (int) Math.min(2147483639L, 2L * (long) this.length));
  }

  private final void reduce() {
    int size = this.size();
    if (this.length > 4 && size <= this.length / 4) {
      this.resize(size, this.length / 2);
    }
  }

  public void enqueue(int x) {
    this.array[this.end++] = x;
    if (this.end == this.length) {
      this.end = 0;
    }

    if (this.end == this.start) {
      this.expand();
    }
  }

  public void enqueueFirst(int x) {
    if (this.start == 0) {
      this.start = this.length;
    }

    this.array[--this.start] = x;
    if (this.end == this.start) {
      this.expand();
    }
  }

  public int firstInt() {
    if (this.start == this.end) {
      throw new NoSuchElementException();
    } else {
      return this.array[this.start];
    }
  }

  public int lastInt() {
    if (this.start == this.end) {
      throw new NoSuchElementException();
    } else {
      return this.array[(this.end == 0 ? this.length : this.end) - 1];
    }
  }

  public void clear() {
    this.start = this.end = 0;
  }

  public void trim() {
    int size = this.size();
    int[] newArray = new int[size + 1];
    if (this.start <= this.end) {
      System.arraycopy(this.array, this.start, newArray, 0, this.end - this.start);
    } else {
      System.arraycopy(this.array, this.start, newArray, 0, this.length - this.start);
      System.arraycopy(this.array, 0, newArray, this.length - this.start, this.end);
    }

    this.start = 0;
    this.length = (this.end = size) + 1;
    this.array = newArray;
  }

  public int size() {
    int apparentLength = this.end - this.start;
    return apparentLength >= 0 ? apparentLength : this.length + apparentLength;
  }

  private void writeObject(ObjectOutputStream s) throws IOException {
    s.defaultWriteObject();
    int size = this.size();
    s.writeInt(size);
    int i = this.start;

    while (size-- != 0) {
      s.writeInt(this.array[i++]);
      if (i == this.length) {
        i = 0;
      }
    }
  }

  private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException {
    s.defaultReadObject();
    this.end = s.readInt();
    this.array = new int[this.length = nextPowerOfTwo(this.end + 1)];

    for (int i = 0; i < this.end; ++i) {
      this.array[i] = s.readInt();
    }
  }

  private static int nextPowerOfTwo(int x) {
    return 1 << 32 - Integer.numberOfLeadingZeros(x - 1);
  }
}
