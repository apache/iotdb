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
package org.apache.iotdb.db.mpp.execution.schedule.queue;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * An efficient subclass of {@link IndexedBlockingQueue} with 1-level priority groups.
 *
 * <p>The time complexity of operations are:
 *
 * <ul>
 *   <li><b>{@link #remove(IDIndexedAccessible)} ()}: </b> O(logN).
 *   <li><b>{@link #push(IDIndexedAccessible)}: </b> O(logN).
 *   <li><b>{@link #poll()}: </b> O(logN).
 *   <li><b>{@link #get(ID)}}: </b> O(1).
 * </ul>
 */
public class L1PriorityQueue<E extends IDIndexedAccessible> extends IndexedBlockingQueue<E> {

  private final SortedSet<E> sortedElements; // Used for accessing in order
  private final Map<ID, E> keyedElements; // Used for accessing randomly

  /**
   * Init the queue with max capacity and specified comparator.
   *
   * @see IndexedBlockingQueue
   * @param maxCapacity the max capacity of the queue.
   * @param comparator the comparator for comparing the elements.
   * @param queryHolder the query holder instance.
   * @throws IllegalArgumentException if maxCapacity <= 0.
   */
  public L1PriorityQueue(int maxCapacity, Comparator<E> comparator, E queryHolder) {
    super(maxCapacity, queryHolder);
    this.sortedElements = new TreeSet<>(comparator);
    this.keyedElements = new HashMap<>();
  }

  @Override
  protected boolean isEmpty() {
    return keyedElements.isEmpty();
  }

  @Override
  protected E pollFirst() {
    E element = sortedElements.first();
    sortedElements.remove(element);
    keyedElements.remove(element.getDriverTaskId());
    return element;
  }

  @Override
  protected void pushToQueue(E element) {
    keyedElements.put(element.getDriverTaskId(), element);
    sortedElements.add(element);
  }

  @Override
  protected E remove(E element) {
    E e = keyedElements.remove(element.getDriverTaskId());
    if (e != null) {
      sortedElements.remove(e);
    }
    return e;
  }

  @Override
  protected boolean contains(E element) {
    return keyedElements.containsKey(element.getDriverTaskId());
  }

  @Override
  protected E get(E element) {
    return keyedElements.get(element.getDriverTaskId());
  }

  @Override
  protected void clearAllElements() {
    sortedElements.clear();
    keyedElements.clear();
  }
}
