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

package org.apache.iotdb.db.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * This class is copied and modified from ArrayList JDK11 and used for TVList only. Change the size
 * enlargement method from 'newSize = oldSize >> 1' to 'newSize = oldSize + 4'. Change the clear()
 * method by adding automatic trimToSize(). Notice: Do not call the method that not in this class!!
 */
@SuppressWarnings(
    "java:S2177") // Suppress Child class methods named for parent class methods should be overrides
public class IoTDBArrayList<E> extends ArrayList<E> {

  private static final int DEFAULT_CAPACITY = 1;
  private static final Object[] DEFAULTCAPACITY_EMPTY_ELEMENTDATA = new Object[0];
  transient Object[] elementData;
  private int size;
  private static final int MAX_ARRAY_SIZE = 2147483639;

  public IoTDBArrayList(int initialCapacity) {
    if (initialCapacity > 0) {
      this.elementData = new Object[initialCapacity];
    } else {
      if (initialCapacity != 0) {
        throw new IllegalArgumentException("Illegal Capacity: " + initialCapacity);
      }

      this.elementData = DEFAULTCAPACITY_EMPTY_ELEMENTDATA;
    }
  }

  public IoTDBArrayList() {
    this.elementData = DEFAULTCAPACITY_EMPTY_ELEMENTDATA;
  }

  private Object[] grow(int minCapacity) {
    return this.elementData = Arrays.copyOf(this.elementData, this.newCapacity(minCapacity));
  }

  private Object[] grow() {
    return this.grow(this.size + 1);
  }

  @Override
  public int size() {
    return this.size;
  }

  @Override
  public boolean isEmpty() {
    return this.size == 0;
  }

  @Override
  public E get(int index) {
    return this.elementData(index);
  }

  private E elementData(int index) {
    return (E) this.elementData[index];
  }

  @Override
  public E set(int index, E element) {
    E oldValue = this.elementData(index);
    this.elementData[index] = element;
    return oldValue;
  }

  private void add(E e, Object[] elementData, int s) {
    if (s == elementData.length) {
      elementData = this.grow();
    }

    elementData[s] = e;
    this.size = s + 1;
  }

  @Override
  public boolean add(E e) {
    ++this.modCount;
    this.add(e, this.elementData, this.size);
    return true;
  }

  @Override
  public void add(int index, E element) {
    ++this.modCount;
    int s;
    Object[] elementData;
    if ((s = this.size) == (elementData = this.elementData).length) {
      elementData = this.grow();
    }

    System.arraycopy(elementData, index, elementData, index + 1, s - index);
    elementData[index] = element;
    this.size = s + 1;
  }

  private int newCapacity(int minCapacity) {
    int oldCapacity = this.elementData.length;
    int newCapacity = oldCapacity + 4;
    if (newCapacity - minCapacity <= 0) {
      if (this.elementData == DEFAULTCAPACITY_EMPTY_ELEMENTDATA) {
        return Math.max(1, minCapacity);
      } else if (minCapacity < 0) {
        throw new OutOfMemoryError();
      } else {
        return minCapacity;
      }
    } else {
      return newCapacity - 2147483639 <= 0 ? newCapacity : hugeCapacity(minCapacity);
    }
  }

  private static int hugeCapacity(int minCapacity) {
    if (minCapacity < 0) {
      throw new OutOfMemoryError();
    } else {
      return minCapacity > 2147483639 ? 2147483647 : 2147483639;
    }
  }

  @Override
  public E remove(int index) {
    Object[] es = this.elementData;
    E oldValue = (E) es[index];
    this.fastRemove(es, index);
    return oldValue;
  }

  private void fastRemove(Object[] es, int i) {
    ++this.modCount;
    int newSize;
    if ((newSize = this.size - 1) > i) {
      System.arraycopy(es, i + 1, es, i, newSize - i);
    }

    es[this.size = newSize] = null;
  }

  @Override
  public void clear() {
    ++this.modCount;
    Object[] es = this.elementData;
    int to = this.size;

    for (int i = this.size = 0; i < to; ++i) {
      es[i] = null;
    }
    trimToSize();
  }

  @Override
  public void trimToSize() {
    ++this.modCount;
    if (this.size < this.elementData.length) {
      this.elementData =
          this.size == 0
              ? DEFAULTCAPACITY_EMPTY_ELEMENTDATA
              : Arrays.copyOf(this.elementData, this.size);
    }
  }

  static <E> E elementAt(Object[] es, int index) {
    return (E) es[index];
  }

  public Iterator<E> iterator() {
    return new IoTDBArrayList.Itr();
  }

  private class Itr implements Iterator<E> {
    int cursor;
    int lastRet = -1;
    int expectedModCount;

    Itr() {
      this.expectedModCount = IoTDBArrayList.this.modCount;
    }

    public boolean hasNext() {
      return this.cursor != IoTDBArrayList.this.size;
    }

    public E next() {
      this.checkForComodification();
      int i = this.cursor;
      if (i >= IoTDBArrayList.this.size) {
        throw new NoSuchElementException();
      } else {
        Object[] elementData = IoTDBArrayList.this.elementData;
        if (i >= elementData.length) {
          throw new ConcurrentModificationException();
        } else {
          this.cursor = i + 1;
          return (E) elementData[this.lastRet = i];
        }
      }
    }

    public void remove() {
      if (this.lastRet < 0) {
        throw new IllegalStateException();
      } else {
        this.checkForComodification();

        try {
          IoTDBArrayList.this.remove(this.lastRet);
          this.cursor = this.lastRet;
          this.lastRet = -1;
          this.expectedModCount = IoTDBArrayList.this.modCount;
        } catch (IndexOutOfBoundsException var2) {
          throw new ConcurrentModificationException();
        }
      }
    }

    public void forEachRemaining(Consumer<? super E> action) {
      Objects.requireNonNull(action);
      int size = IoTDBArrayList.this.size;
      int i = this.cursor;
      if (i < size) {
        Object[] es = IoTDBArrayList.this.elementData;
        if (i >= es.length) {
          throw new ConcurrentModificationException();
        }

        while (i < size && IoTDBArrayList.this.modCount == this.expectedModCount) {
          action.accept(IoTDBArrayList.elementAt(es, i));
          ++i;
        }

        this.cursor = i;
        this.lastRet = i - 1;
        this.checkForComodification();
      }
    }

    final void checkForComodification() {
      if (IoTDBArrayList.this.modCount != this.expectedModCount) {
        throw new ConcurrentModificationException();
      }
    }
  }
}
