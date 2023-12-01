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

package org.apache.iotdb.commons.pipe.datastructure;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class allows dynamic iterating over the elements, namely an iterator is able to read the
 * incoming element.
 *
 * @param <E> Element type
 */
public class ConcurrentIterableLinkedQueue<E> {
  LinkedListNode<E> pilot = new LinkedListNode<>(null);
  LinkedListNode<E> first;
  LinkedListNode<E> last;
  ReentrantLock lock = new ReentrantLock();

  Condition hasNext = lock.newCondition();

  // The index of elements are [firstIndex, firstIndex + 1, ....,lastIndex - 1]
  int firstIndex = 0;
  int lastIndex = 0;

  public ConcurrentIterableLinkedQueue() {
    // first == last == null
  }

  public void add(E e) {
    lock.lock();
    try {
      if (e == null) {
        throw new IllegalArgumentException(
            "Null is reserved as the signal to imply a get operation failure. Please use another element to imply null.");
      }
      final LinkedListNode<E> l = last;
      final LinkedListNode<E> newNode = new LinkedListNode<>(e);
      last = newNode;
      if (l == null) {
        first = newNode;
        pilot.next = first;
      } else {
        l.next = newNode;
      }
      ++lastIndex;
      hasNext.signalAll();
    } finally {
      lock.unlock();
    }
  }

  public void removeBefore(int newFirst) {
    lock.lock();
    try {
      if (newFirst <= firstIndex) {
        return;
      }
      LinkedListNode<E> next;
      LinkedListNode<E> x;
      for (x = first; x != null && firstIndex < newFirst; ++firstIndex, x = next) {
        next = x.next;
        x.data = null;
        x.next = null;
      }
      first = x;
      pilot.next = first;
      if (first == null) {
        last = null;
      }
      hasNext.signalAll();
    } finally {
      lock.unlock();
    }
  }

  public void clear() {
    lock.lock();
    try {
      removeBefore(lastIndex);
      firstIndex = 0;
      lastIndex = 0;
    } finally {
      lock.unlock();
    }
  }

  public DynamicIterator iterateFrom(int offset) {
    return new DynamicIterator(offset);
  }

  public int getFirstIndex() {
    lock.lock();
    try {
      return firstIndex;
    } finally {
      lock.unlock();
    }
  }

  public int getLastIndex() {
    lock.lock();
    try {
      return lastIndex;
    } finally {
      lock.unlock();
    }
  }

  public DynamicIterator iterateFromEarliest() {
    return iterateFrom(Integer.MIN_VALUE);
  }

  public DynamicIterator iterateFromLatest() {
    return iterateFrom(Integer.MAX_VALUE);
  }

  private static class LinkedListNode<E> {
    E data;
    LinkedListNode<E> next;

    public LinkedListNode(E data) {
      this.data = data;
      this.next = null;
    }
  }

  // Temporarily, we do not use read lock because read lock in java does not
  // support condition. Besides, The pure park and un-park method is fairly slow,
  // thus we use Reentrant lock here.
  public class DynamicIterator {

    private LinkedListNode<E> next;

    // Offset is the position of the next element to read
    private int offset;

    DynamicIterator(int offset) {
      lock.lock();
      try {
        if (last != null && offset >= lastIndex) {
          next = last;
          offset = lastIndex;
        } else {
          next = pilot;
          if (firstIndex < offset) {
            for (int i = 0; i < offset - firstIndex; ++i) {
              next();
            }
          } else {
            offset = firstIndex;
          }
        }
        this.offset = offset;
      } finally {
        lock.unlock();
      }
    }

    /**
     * The getter of the iterator. Never timeOut.
     *
     * @return
     *     <p>1. Directly The next element if exists.
     *     <p>2. Blocking until available iff the next element does not exist.
     *     <p>3. Null iff the current element is null or the next element's data is null.
     */
    public E next() {
      return next(Long.MAX_VALUE);
    }

    /**
     * The getter of the iterator with the given timeOut.
     *
     * @param waitTimeMillis the timeOut of the get operation
     * @return
     *     <p>1. Directly The next element if exists.
     *     <p>2. Blocking until available iff the next element does not exist.
     *     <p>3. Null iff the current element is null or the next element's data is null.
     */
    public E next(long waitTimeMillis) {
      lock.lock();
      try {
        while (!hasNext()) {
          if (!hasNext.await(waitTimeMillis, TimeUnit.MILLISECONDS)) {
            return null;
          }
        }

        next = next.next;
        ++offset;
        return next.data;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (NullPointerException ignore) {
        // NullPointerException means the "next" node is null, typically because the
        // element is cleared. Though we don't except a linked queue to be cleared
        // when there are subscriptions alive, still we simply return null to notify the subscriber.
      } finally {
        lock.unlock();
      }
      return null;
    }

    public boolean hasNext() {
      lock.lock();
      try {
        return next.next != null;
      } finally {
        lock.unlock();
      }
    }

    /**
     * Seek the {@link DynamicIterator#offset} to the closest position allowed to the given offset.
     * Note that one can seek to {@link ConcurrentIterableLinkedQueue#lastIndex} to subscribe the
     * next incoming element.
     *
     * @param newOffset the attempt newOffset
     * @return the actual new offset
     */
    public int seek(int newOffset) {
      lock.lock();
      try {
        if (newOffset < firstIndex) {
          newOffset = firstIndex;
        }
        if (newOffset > lastIndex) {
          newOffset = lastIndex;
        }
        int oldOffset = offset;
        if (newOffset < oldOffset) {
          next = pilot;
          offset = firstIndex;
          for (int i = 0; i < newOffset - firstIndex; ++i) {
            next();
          }
        } else {
          for (int i = 0; i < newOffset - oldOffset; ++i) {
            next();
          }
        }
        return offset;
      } finally {
        lock.unlock();
      }
    }

    public int getOffset() {
      lock.lock();
      try {
        return offset;
      } finally {
        lock.unlock();
      }
    }
  }
}
