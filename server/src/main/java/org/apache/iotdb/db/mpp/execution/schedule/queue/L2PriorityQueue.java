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
 * An efficient subclass of {@link IndexedBlockingQueue} with 2-level priority groups. The
 * advantages compared to {@link L1PriorityQueue} are that each element in this queue will not be
 * starved to death by its low sequence order.
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
public class L2PriorityQueue<E extends IDIndexedAccessible> extends IndexedBlockingQueue<E> {

  private SortedSet<E> workingSortedElements;
  private SortedSet<E> idleSortedElements;
  private Map<ID, E> workingKeyedElements;
  private Map<ID, E> idleKeyedElements;

  /**
   * Init the queue with max capacity and specified comparator.
   *
   * @see IndexedBlockingQueue
   * @param maxCapacity the max capacity of the queue.
   * @param comparator the comparator for comparing the elements.
   * @param queryHolder the query holder instance.
   * @throws IllegalArgumentException if maxCapacity <= 0.
   */
  public L2PriorityQueue(int maxCapacity, Comparator<E> comparator, E queryHolder) {
    super(maxCapacity, queryHolder);
    this.workingSortedElements = new TreeSet<>(comparator);
    this.idleSortedElements = new TreeSet<>(comparator);
    this.workingKeyedElements = new HashMap<>();
    this.idleKeyedElements = new HashMap<>();
  }

  @Override
  protected boolean isEmpty() {
    return workingKeyedElements.isEmpty() && idleKeyedElements.isEmpty();
  }

  @Override
  protected E pollFirst() {
    if (workingKeyedElements.isEmpty()) {
      // Switch the two queues
      Map<ID, E> tmp = workingKeyedElements;
      workingKeyedElements = idleKeyedElements;
      idleKeyedElements = tmp;
      SortedSet<E> tmpSet = workingSortedElements;
      workingSortedElements = idleSortedElements;
      idleSortedElements = tmpSet;
    }
    E element = workingSortedElements.first();
    workingSortedElements.remove(element);
    workingKeyedElements.remove(element.getDriverTaskId());
    return element;
  }

  @Override
  protected void pushToQueue(E element) {
    idleKeyedElements.put(element.getDriverTaskId(), element);
    idleSortedElements.add(element);
  }

  @Override
  protected E remove(E element) {
    E e = workingKeyedElements.remove(element.getDriverTaskId());
    if (e != null) {
      workingSortedElements.remove(e);
      return e;
    }
    e = idleKeyedElements.remove(element.getDriverTaskId());
    if (e != null) {
      idleSortedElements.remove(e);
    }
    return e;
  }

  @Override
  protected boolean contains(E element) {
    return workingKeyedElements.containsKey(element.getDriverTaskId())
        || idleKeyedElements.containsKey(element.getDriverTaskId());
  }

  @Override
  protected E get(E element) {
    E e = workingKeyedElements.get(element.getDriverTaskId());
    if (e != null) {
      return e;
    }
    return idleKeyedElements.get(element.getDriverTaskId());
  }

  @Override
  protected void clearAllElements() {
    workingKeyedElements.clear();
    workingSortedElements.clear();
    idleKeyedElements.clear();
    idleSortedElements.clear();
  }
}
