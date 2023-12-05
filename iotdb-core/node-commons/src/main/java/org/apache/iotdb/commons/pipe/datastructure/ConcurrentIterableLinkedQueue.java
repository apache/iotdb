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

import java.io.Closeable;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class allows dynamic iterating over the elements, namely an iterator is able to read the
 * incoming element.
 *
 * @param <E> Element type
 */
public class ConcurrentIterableLinkedQueue<E> {
  private final LinkedListNode<E> pilot = new LinkedListNode<>(null);
  private LinkedListNode<E> first;
  private LinkedListNode<E> last;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private final Condition hasNext = lock.writeLock().newCondition();

  private final Map<DynamicIterator, DynamicIterator> iteratorSet = new ConcurrentHashMap<>();

  // The index of elements are [firstIndex, firstIndex + 1, ....,lastIndex - 1]
  private int firstIndex = 0;
  private int lastIndex = 0;

  public ConcurrentIterableLinkedQueue() {
    first = pilot;
    last = pilot;
  }

  public void add(E e) {
    lock.writeLock().lock();
    try {
      if (e == null) {
        throw new IllegalArgumentException(
            "Null is reserved as the signal to imply a get operation failure. Please use another element to imply null.");
      }
      final LinkedListNode<E> l = last;
      final LinkedListNode<E> newNode = new LinkedListNode<>(e);
      last = newNode;
      l.next = newNode;
      if (l == pilot) {
        first = newNode;
      }
      ++lastIndex;
      hasNext.signalAll();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public int removeBefore(int newFirst) {
    lock.writeLock().lock();
    try {
      AtomicInteger tempFirst = new AtomicInteger(newFirst);
      iteratorSet.keySet().stream()
          .min(Comparator.comparing(DynamicIterator::getOffset))
          .ifPresent(e -> tempFirst.set(Math.min(tempFirst.get(), e.getOffset())));
      newFirst = tempFirst.get();

      if (newFirst <= firstIndex) {
        return firstIndex;
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
        first = pilot;
        last = pilot;
      }
      iteratorSet
          .keySet()
          .forEach(
              e -> {
                if (e.offset == firstIndex) {
                  e.next = pilot;
                }
              });
      return firstIndex;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void clear() {
    lock.writeLock().lock();
    try {
      iteratorSet.keySet().forEach(DynamicIterator::close);
      removeBefore(lastIndex);
      hasNext.signalAll();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public DynamicIterator iterateFrom(int offset) {
    DynamicIterator itr = new DynamicIterator(offset);
    iteratorSet.put(itr, itr);
    return itr;
  }

  public int getFirstIndex() {
    lock.readLock().lock();
    try {
      return firstIndex;
    } finally {
      lock.readLock().unlock();
    }
  }

  public int getLastIndex() {
    lock.readLock().lock();
    try {
      return lastIndex;
    } finally {
      lock.readLock().unlock();
    }
  }

  public Set<DynamicIterator> getIteratorSet() {
    return iteratorSet.keySet();
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
  public class DynamicIterator implements Closeable {

    private LinkedListNode<E> next;

    // Offset is the position of the next element to read
    private int offset;
    private volatile boolean isClosed = false;

    public DynamicIterator(int offset) {
      lock.writeLock().lock();
      try {
        if (offset >= lastIndex) {
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
        lock.writeLock().unlock();
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
      lock.writeLock().lock();
      try {
        if (isClosed) {
          return null;
        }

        while (!hasNext()) {
          if (!hasNext.await(waitTimeMillis, TimeUnit.MILLISECONDS) || isClosed) {
            return null;
          }
        }

        next = next.next;
        ++offset;
        return next.data;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        lock.writeLock().unlock();
      }
      return null;
    }

    public boolean hasNext() {
      lock.readLock().lock();
      try {
        if (isClosed) {
          return false;
        }
        return next.next != null;
      } finally {
        lock.readLock().unlock();
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
      lock.writeLock().lock();
      try {
        if (isClosed) {
          return -1;
        }
        newOffset = Math.max(firstIndex, Math.min(lastIndex, newOffset));
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
        lock.writeLock().unlock();
      }
    }

    public boolean getIsClosed() {
      return isClosed;
    }

    public int getOffset() {
      lock.readLock().lock();
      try {
        return !isClosed ? offset : -1;
      } finally {
        lock.readLock().unlock();
      }
    }

    @Override
    public void close() {
      isClosed = true;
      iteratorSet.remove(this);
    }
  }
}
