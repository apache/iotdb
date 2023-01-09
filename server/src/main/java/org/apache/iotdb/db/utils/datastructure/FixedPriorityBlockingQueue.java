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

package org.apache.iotdb.db.utils.datastructure;

import com.google.common.collect.MinMaxPriorityQueue;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This is a priority blocking queue with a maximum size. If the queue's size is larger than the max
 * size, the element with max priority will be kick out.
 */
public class FixedPriorityBlockingQueue<T> {
  private int maxSize;
  private Comparator<T> comparator;
  private MinMaxPriorityQueue<T> queue;

  private ReentrantLock lock = new ReentrantLock();
  private Condition notEmpty = lock.newCondition();

  private List<PollLastHook> pollLastHookList = new CopyOnWriteArrayList<>();

  public FixedPriorityBlockingQueue(int maxSize, Comparator<T> comparator) {
    this.maxSize = maxSize;
    this.comparator = comparator;
    this.queue = MinMaxPriorityQueue.orderedBy(comparator).maximumSize(maxSize + 1).create();
  }

  public void put(T element) throws InterruptedException {
    final ReentrantLock lock = this.lock;
    lock.lockInterruptibly();
    try {
      queue.add(element);
      if (queue.size() > maxSize) {
        this.pollLast();
      }
      notEmpty.signal();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Return the element with min priority. If the queue is empty, the thread will be blocked util
   * there are some elements inserted into the queue.
   *
   * @return
   * @throws InterruptedException
   */
  public T take() throws InterruptedException {
    final ReentrantLock lock = this.lock;
    lock.lockInterruptibly();
    try {
      while (queue.isEmpty()) {
        notEmpty.await();
      }
      return queue.pollFirst();
    } finally {
      this.lock.unlock();
    }
  }

  /**
   * Return the element with max priority. If the queue is empty, the thread will be blocked util
   * there are some elements inserted into the queue.
   *
   * @return
   * @throws InterruptedException
   */
  public T takeMax() throws InterruptedException {
    final ReentrantLock lock = this.lock;
    lock.lockInterruptibly();
    try {
      while (queue.isEmpty()) {
        notEmpty.await();
      }
      return queue.pollLast();
    } finally {
      this.lock.unlock();
    }
  }

  /**
   * Add a hook for this queue. If an element is kicked out because the queue's size exceed the
   * largest value, the hook will apply to this element. Notice, multiple hooks can be added to a
   * queue, and all of them will be applied to the element kicked out. The order in which they are
   * applied depends on the order in which they were registered.
   *
   * @param hook
   */
  public void regsitPollLastHook(PollLastHook<T> hook) {
    this.pollLastHookList.add(hook);
  }

  public boolean contains(T element) {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      return queue.contains(element);
    } finally {
      lock.unlock();
    }
  }

  public int size() {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      return queue.size();
    } finally {
      lock.unlock();
    }
  }

  public void clear() {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      while (queue.size() != 0) {
        this.pollLast();
      }
    } finally {
      lock.unlock();
    }
  }

  public boolean isEmpty() {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      return queue.isEmpty();
    } finally {
      lock.unlock();
    }
  }

  private void pollLast() {
    final ReentrantLock lock = this.lock;
    lock.lock();
    T element = null;
    try {
      element = queue.pollLast();
    } finally {
      lock.unlock();
    }
    if (element != null) {
      T finalElement = element;
      pollLastHookList.forEach(x -> x.apply(finalElement));
    }
  }

  public Comparator<T> getComparator() {
    return this.comparator;
  }

  public int getMaxSize() {
    return this.maxSize;
  }

  @FunctionalInterface
  public interface PollLastHook<T> {
    void apply(T x);
  }

  @Override
  public String toString() {
    return queue.toString();
  }
}
