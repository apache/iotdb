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

public class LinkedListMessageQueue<E> {
  LinkedListNode<E> pilot = new LinkedListNode<>(null);
  LinkedListNode<E> first;
  LinkedListNode<E> last;
  ReentrantLock lock = new ReentrantLock();
  Set<ConsumerItr> consumerItrSet = new CopyOnWriteArraySet<>();

  Condition hasNext = lock.newCondition();

  int index = 0;
  int lastIndex = 0;
  int sealIndex = Integer.MAX_VALUE;

  public LinkedListMessageQueue() {
    // first == last == null
  }

  public boolean add(E e) {
    if (sealIndex != Integer.MAX_VALUE) {
      return false;
    }
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

  // Seal means the linked list will not apply any more elements. The
  // consumers can quit after having read all.
  public void seal() {
    lock.lock();
    try {
      sealIndex = lastIndex;
      hasNext.signalAll();
    } finally {
      lock.unlock();
    }
  }

  public void clear() {
    lock.lock();
    try {
      for (LinkedListNode<E> x = first; x != null; ) {
        LinkedListNode<E> next = x.next;
        x.data = null;
        x.next = null;
        x = next;
        ++index;
      }
      first = null;
      index = 0;
      lastIndex = 0;
      sealIndex = Integer.MAX_VALUE;
      consumerItrSet.clear();
      hasNext.signalAll();
    } finally {
      lock.unlock();
    }
  }

  public ConsumerItr subscribe(int offset) {
    ConsumerItr itr = new ConsumerItr(offset);
    consumerItrSet.add(itr);
    return itr;
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
    int offset;

    ConsumerItr(int offset) {
      lock.lock();
      try {
        if (last != null && offset >= lastIndex) {
          next = last;
          offset = lastIndex;
        } else {
          next = pilot;
          if (index > offset) {
            for (int i = 0; i < offset - index; ++i) {
              poll();
            }
          } else {
            offset = index;
          }
        }
        this.offset = offset;
      } finally {
        lock.unlock();
      }
    }

    public E poll() {
      lock.lock();
      try {
        while (!hasNext()) {
          hasNext.await();
        }

        if (offset == sealIndex) {
          return null;
        }

        next = next.next;
        ++offset;
        return next.data;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (NullPointerException ignore) {
        // NullPointerException means the "next" node is null, typically because the
        // linked queue is cleared. Though we don't except a linked queue to be cleared
        // when there are subscriptions alive, still we simply return null to notify the subscriber.
      } finally {
        lock.unlock();
      }
      return null;
    }

    private boolean hasNext() {
      return next.next != null;
    }

    public boolean seek(int newOffset) {
      lock.lock();
      try {
        if (newOffset < index && newOffset > lastIndex) {
          return false;
        }
        if (newOffset < offset) {
          next = pilot;
          for (int i = 0; i < offset - index; ++i) {
            poll();
          }
        } else {
          for (int i = 0; i < newOffset - offset; ++i) {
            poll();
          }
        }
        offset = newOffset;
        return true;
      } finally {
        lock.unlock();
      }
    }

    public int position() {
      lock.lock();
      try {
        return offset;
      } finally {
        lock.unlock();
      }
    }
  }
}
