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

package org.apache.iotdb.commons.pipe.schema;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class allows dynamic iterating over the elements, namely an iterator is able to read the
 * incoming element.
 *
 * @param <E> Element type
 */
public class LinkedListMessageQueue<E> {
  LinkedListNode<E> pilot = new LinkedListNode<>(null);
  LinkedListNode<E> first;
  LinkedListNode<E> last;
  ReentrantLock lock = new ReentrantLock();
  Set<ConsumerItr> consumerItrSet = new CopyOnWriteArraySet<>();

  Condition hasNext = lock.newCondition();

  // The index of elements are [firstIndex, firstIndex + 1, ....,lastIndex - 1]
  int firstIndex = 0;
  int lastIndex = 0;

  public LinkedListMessageQueue() {
    // first == last == null
  }

  public boolean add(E e) {
    lock.lock();
    try {
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
    return true;
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
      hasNext.signalAll();
    } finally {
      lock.unlock();
    }
  }

  public void clear() {
    lock.lock();
    try {
      removeBefore(lastIndex);
      first = null;
      firstIndex = 0;
      lastIndex = 0;
      consumerItrSet.clear();
    } finally {
      lock.unlock();
    }
  }

  public ConsumerItr subscribe(int offset) {
    ConsumerItr itr = new ConsumerItr(offset);
    consumerItrSet.add(itr);
    return itr;
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

  public ConsumerItr subscribeEarliest() {
    return subscribe(Integer.MIN_VALUE);
  }

  public ConsumerItr subscribeLatest() {
    return subscribe(Integer.MAX_VALUE);
  }

  public int getSubscriptionNum() {
    return consumerItrSet.size();
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
  public class ConsumerItr {

    private LinkedListNode<E> next;

    // Offset is the position of the next element to read
    private int offset;

    ConsumerItr(int offset) {
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
     * The getter of the iterator.
     *
     * @return
     *     <p>1. Directly The next element if exists.
     *     <p>2. Blocking until available iff the next element does not exist.
     *     <p>3. Null iff the current element is null or the next element's data is null.
     */
    public E next() {
      lock.lock();
      try {
        while (!hasNext()) {
          hasNext.await();
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

    private boolean hasNext() {
      return next.next != null;
    }

    /**
     * Seek the {@link ConsumerItr#offset} to the closest position allowed to the given offset. Note
     * that one can seek to {@link LinkedListMessageQueue#lastIndex} to subscribe the next incoming
     * element.
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
        if (newOffset < offset) {
          next = pilot;
          for (int i = 0; i < newOffset - firstIndex; ++i) {
            next();
          }
        } else {
          for (int i = 0; i < newOffset - offset; ++i) {
            next();
          }
        }
        offset = newOffset;
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
