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

package org.apache.iotdb.db.queryengine.execution.schedule.queue;

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
    return output;
  }

  public synchronized void push(E element) {
    if (element == null) {
      throw new NullPointerException("pushed element is null");
    }
    Preconditions.checkState(
        size + reservedSize < capacity, "The system can't allow more query tasks.");
    pushToQueue(element);
    size++;
    this.notifyAll();
  }

  /** RePush an element which is polled out for running or blocked before to the queue. */
  public synchronized void repush(E element) {
    if (element == null) {
      throw new NullPointerException("pushed element is null");
    }
    pushToQueue(element);
    reservedSize--;
    size++;
    this.notifyAll();
  }

  /**
   * For task that is not in readyQueue when it's cleared, it won't be added into the queue again.
   */
  public synchronized void decreaseReservedSize() {
    this.reservedSize--;
  }
}
