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

package org.apache.iotdb.calc.execution.schedule.queue;

import org.apache.iotdb.calc.i18n.CalcMessages;

import com.google.common.base.Preconditions;

/**
 * This class is different from <class>IndexedBlockingQueue</class> in that it will reserve space
 * for polled element in case it will be pushed again.
 */
public abstract class IndexedBlockingReserveQueue<E extends IDIndexedAccessible>
    extends IndexedBlockingQueue<E> {

  // To avoid some elements can't join the queue again that are polled out for running or blocked
  private int reservedSize;

  public IndexedBlockingReserveQueue(int maxCapacity, E queryHolder) {
    super(maxCapacity, queryHolder);
  }

  /**
   * Get and remove the first element of the queue. Reserve space for this polled element.
   *
   * @return the queue head element.
   */
  public synchronized E poll() throws InterruptedException {
    while (isEmpty()) {
      this.wait();
    }
    E output = pollFirst();
    size--;
    reservedSize++;
    markReserved(output);
    return output;
  }

  public synchronized void push(E element) {
    if (element == null) {
      throw new NullPointerException(CalcMessages.PUSHED_ELEMENT_IS_NULL);
    }
    Preconditions.checkState(size + reservedSize < capacity, TOO_MANY_CONCURRENT_QUERIES_ERROR_MSG);
    pushToQueue(element);
    size++;
    this.notifyAll();
  }

  /** RePush an element which is polled out for running or blocked before to the queue. */
  public synchronized void repush(E element) {
    if (element == null) {
      throw new NullPointerException(CalcMessages.PUSHED_ELEMENT_IS_NULL);
    }
    pushToQueue(element);
    decreaseReservedSizeIfNecessary(element);
    size++;
    this.notifyAll();
  }

  /**
   * For task that is not in readyQueue when it's cleared, it won't be added into the queue again.
   */
  public synchronized boolean decreaseReservedSize(E element) {
    if (element == null) {
      throw new NullPointerException(CalcMessages.PUSHED_ELEMENT_IS_NULL);
    }
    return decreaseReservedSizeIfNecessary(element);
  }

  public final synchronized int getReservedSize() {
    return reservedSize;
  }

  @Override
  public synchronized void clear() {
    super.clear();
    this.reservedSize = 0;
  }

  protected void markReserved(E element) {
    // Do nothing by default.
  }

  protected boolean releaseReserved(E element) {
    return true;
  }

  private boolean decreaseReservedSizeIfNecessary(E element) {
    if (!releaseReserved(element) || reservedSize <= 0) {
      return false;
    }
    reservedSize--;
    return true;
  }
}
