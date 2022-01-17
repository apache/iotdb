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

  public FixedPriorityBlockingQueue(int maxSize, Comparator<T> comparator) {
    this.maxSize = maxSize;
    this.comparator = comparator;
    this.queue = MinMaxPriorityQueue.orderedBy(comparator).maximumSize(maxSize).create();
  }

  public void put(T element) throws InterruptedException {
    final ReentrantLock lock = this.lock;
    lock.lockInterruptibly();
    try {
      queue.add(element);
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
      while (queue.size() == 0) {
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
      while (queue.size() == 0) {
        notEmpty.await();
      }
      return queue.pollLast();
    } finally {
      this.lock.unlock();
    }
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
      queue.clear();
    } finally {
      lock.unlock();
    }
  }

  public Comparator<T> getComparator() {
    return this.comparator;
  }

  public int getMaxSize() {
    return this.maxSize;
  }
}
