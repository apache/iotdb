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
package org.apache.iotdb.db.mpp.schedule.queue;

/**
 * The base class of a special kind of blocking queue, which has these characters:
 *
 * <p>1. Thread-safe.
 *
 * <p>2. Can poll from queue head. When the queue is empty, the poll() will be blocked until an
 * element is inserted.
 *
 * <p>3. Can push a non-null element to queue. When the queue is beyond the max size, an exception
 * will be thrown.
 *
 * <p>4. Can remove an element by a type of {@link ID}.
 *
 * <p>5. Each element has the different ID.
 */
public abstract class IndexedBlockingQueue<E extends IDIndexedAccessible> {

  private final int MAX_CAPACITY;
  private final E queryHolder;
  private int size;

  /**
   * Init the queue with a max capacity. The queryHolder is just a simple reused object in query to
   * avoid small objects allocation. It should be not used in any other places out of the queue as
   * the id may be mutated.
   *
   * @param maxCapacity the max capacity of the queue.
   * @param queryHolder the query holder instance.
   * @throws IllegalArgumentException if maxCapacity <= 0.
   */
  public IndexedBlockingQueue(int maxCapacity, E queryHolder) {
    this.MAX_CAPACITY = maxCapacity;
    this.queryHolder = queryHolder;
  }

  /**
   * Get and remove the first element of the queue. If the queue is empty, this call will be blocked
   * until an element has been pushed.
   *
   * @return the queue head element.
   */
  public synchronized E poll() throws InterruptedException {
    while (isEmpty()) {
      this.wait();
    }
    E output = pollFirst();
    size--;
    return output;
  }

  /**
   * Push an element to the queue. The new element position is determined by the implementation. If
   * the queue size has been reached the maxCapacity, an {@link IllegalStateException} will be
   * thrown. If the element is null, an {@link NullPointerException} will be thrown.
   *
   * @param element the element to be pushed.
   * @throws NullPointerException the pushed element is null.
   * @throws IllegalStateException the queue size has been reached the maxCapacity.
   */
  public synchronized void push(E element) {
    if (element == null) {
      throw new NullPointerException("pushed element is null");
    }
    int sizeDelta = contains(element) ? 0 : 1;
    if (size + sizeDelta > MAX_CAPACITY) {
      throw new IllegalStateException("the queue is full");
    }
    pushToQueue(element);
    size += sizeDelta;
    this.notifyAll();
  }

  /**
   * Remove and return the element by id. It returns null if it doesn't exist.
   *
   * @param id the id of the element to be removed.
   * @return the removed element.
   */
  public synchronized E remove(ID id) {
    queryHolder.setId(id);
    E output = remove(queryHolder);
    if (output == null) {
      return null;
    }
    size--;
    return output;
  }

  /**
   * Get the element by id. It returns null if it doesn't exist.
   *
   * @param id the id of the element.
   * @return the removed element.
   */
  public synchronized E get(ID id) {
    queryHolder.setId(id);
    return get(queryHolder);
  }

  /**
   * Get the current queue size.
   *
   * @return the current queue size.
   */
  public final synchronized int size() {
    return size;
  }

  /**
   * Whether the queue is empty.
   *
   * <p>This implementation needn't be thread-safe.
   *
   * @return true if the queue is empty, otherwise false.
   */
  protected abstract boolean isEmpty();

  /**
   * Get and remove the first element.
   *
   * <p>This implementation needn't be thread-safe.
   *
   * @return The first element.
   */
  protected abstract E pollFirst();

  /**
   * Push the element into the queue.
   *
   * <p>This implementation needn't be thread-safe.
   *
   * @param element the element to be pushed.
   */
  protected abstract void pushToQueue(E element);

  /**
   * Remove and return the element by its ID. It returns null if it doesn't exist.
   *
   * <p>This implementation needn't be thread-safe.
   *
   * @param element the element to be removed.
   * @return the removed element.
   */
  protected abstract E remove(E element);

  /**
   * Check whether an element with the same ID exists.
   *
   * <p>This implementation needn't be thread-safe.
   *
   * @param element the element to be checked.
   * @return true if an element with the same ID exists, otherwise false.
   */
  protected abstract boolean contains(E element);

  /**
   * Return the element with the same id of the input, null if it doesn't exist.
   *
   * <p>This implementation needn't be thread-safe.
   *
   * @param element the element to be queried.
   * @return the element with the same id in the queue. Null if it doesn't exist.
   */
  protected abstract E get(E element);
}
