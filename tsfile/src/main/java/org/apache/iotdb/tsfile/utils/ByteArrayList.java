/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * This class includes code modified from Eclipse Collections project.
 *
 * <p>Copyright: 2021 Goldman Sachs
 *
 * <p>Project page: https://www.eclipse.org/collections/
 *
 * <p>License: https://github.com/eclipse/eclipse-collections/blob/master/LICENSE-EDL-1.0.txt
 */
package org.apache.iotdb.tsfile.utils;

import java.util.Arrays;

public class ByteArrayList {

  private static final byte[] DEFAULT_SIZED_EMPTY_ARRAY = {};
  private static final byte[] ZERO_SIZED_ARRAY = {};
  private static final int MAXIMUM_ARRAY_SIZE = Integer.MAX_VALUE - 8;

  protected int size;
  protected transient byte[] items = DEFAULT_SIZED_EMPTY_ARRAY;

  public ByteArrayList() {}

  public ByteArrayList(int initialCapacity) {
    this.items = initialCapacity == 0 ? ZERO_SIZED_ARRAY : new byte[initialCapacity];
  }

  private void ensureCapacityForAdd() {
    if (this.items == DEFAULT_SIZED_EMPTY_ARRAY) {
      this.items = new byte[10];
    } else {
      this.transferItemsToNewArrayWithCapacity(this.sizePlusFiftyPercent(this.size));
    }
  }

  private int sizePlusFiftyPercent(int oldSize) {
    int result = oldSize + (oldSize >> 1) + 1;
    return result < oldSize ? MAXIMUM_ARRAY_SIZE : result;
  }

  private void transferItemsToNewArrayWithCapacity(int newCapacity) {
    this.items = this.copyItemsWithNewCapacity(newCapacity);
  }

  private byte[] copyItemsWithNewCapacity(int newCapacity) {
    byte[] newItems = new byte[newCapacity];
    System.arraycopy(this.items, 0, newItems, 0, Math.min(this.size, newCapacity));
    return newItems;
  }

  public boolean add(byte newItem) {
    if (this.items.length == this.size) {
      this.ensureCapacityForAdd();
    }
    this.items[this.size] = newItem;
    this.size++;
    return true;
  }

  public boolean addAll(byte... source) {
    if (source.length < 1) {
      return false;
    }
    this.copyItems(source.length, source);
    return true;
  }

  private void copyItems(int sourceSize, byte[] source) {
    int newSize = this.size + sourceSize;
    this.ensureCapacity(newSize);
    System.arraycopy(source, 0, this.items, this.size, sourceSize);
    this.size = newSize;
  }

  public void ensureCapacity(int minCapacity) {
    int oldCapacity = this.items.length;
    if (minCapacity > oldCapacity) {
      int newCapacity = Math.max(this.sizePlusFiftyPercent(oldCapacity), minCapacity);
      this.transferItemsToNewArrayWithCapacity(newCapacity);
    }
  }

  public byte[] toArray() {
    byte[] newItems = new byte[this.size];
    System.arraycopy(this.items, 0, newItems, 0, this.size);
    return newItems;
  }

  public byte removeAtIndex(int index) {
    byte previous = this.get(index);
    int totalOffset = this.size - index - 1;
    if (totalOffset > 0) {
      System.arraycopy(this.items, index + 1, this.items, index, totalOffset);
    }
    --this.size;
    this.items[this.size] = (byte) 0;
    return previous;
  }

  public byte get(int index) {
    if (index < this.size) {
      return this.items[index];
    }
    throw new IndexOutOfBoundsException("Index: " + index + " Size: " + this.size);
  }

  public void clear() {
    Arrays.fill(this.items, 0, size, (byte) 0);
    this.size = 0;
  }

  public int size() {
    return this.size;
  }
}
