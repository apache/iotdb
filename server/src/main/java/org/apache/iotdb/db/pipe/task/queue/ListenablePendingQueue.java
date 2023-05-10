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

package org.apache.iotdb.db.pipe.task.queue;

import org.apache.iotdb.pipe.api.event.Event;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ListenablePendingQueue<E extends Event> {

  private final Queue<E> pendingQueue;

  private final Map<String, PendingQueueEmptyToNotEmptyListener> emptyToNotEmptyListeners =
      new ConcurrentHashMap<>();
  private final Map<String, PendingQueueNotEmptyToEmptyListener> notEmptyToEmptyListeners =
      new ConcurrentHashMap<>();
  private final Map<String, PendingQueueFullToNotFullListener> fullToNotFullListeners =
      new ConcurrentHashMap<>();
  private final Map<String, PendingQueueNotFullToFullListener> notFullToFullListeners =
      new ConcurrentHashMap<>();

  private final AtomicBoolean isFull = new AtomicBoolean(false);

  protected ListenablePendingQueue(Queue<E> pendingQueue) {
    this.pendingQueue = pendingQueue;
  }

  public ListenablePendingQueue<E> registerEmptyToNotEmptyListener(
      String id, PendingQueueEmptyToNotEmptyListener listener) {
    emptyToNotEmptyListeners.put(id, listener);
    return this;
  }

  public void removeEmptyToNotEmptyListener(String id) {
    emptyToNotEmptyListeners.remove(id);
  }

  public void notifyEmptyToNotEmptyListeners() {
    emptyToNotEmptyListeners
        .values()
        .forEach(PendingQueueEmptyToNotEmptyListener::onPendingQueueEmptyToNotEmpty);
  }

  public ListenablePendingQueue<E> registerNotEmptyToEmptyListener(
      String id, PendingQueueNotEmptyToEmptyListener listener) {
    notEmptyToEmptyListeners.put(id, listener);
    return this;
  }

  public void removeNotEmptyToEmptyListener(String id) {
    notEmptyToEmptyListeners.remove(id);
  }

  public void notifyNotEmptyToEmptyListeners() {
    notEmptyToEmptyListeners
        .values()
        .forEach(PendingQueueNotEmptyToEmptyListener::onPendingQueueNotEmptyToEmpty);
  }

  public ListenablePendingQueue<E> registerFullToNotFullListener(
      String id, PendingQueueFullToNotFullListener listener) {
    fullToNotFullListeners.put(id, listener);
    return this;
  }

  public void removeFullToNotFullListener(String id) {
    fullToNotFullListeners.remove(id);
  }

  public void notifyFullToNotFullListeners() {
    fullToNotFullListeners
        .values()
        .forEach(PendingQueueFullToNotFullListener::onPendingQueueFullToNotFull);
  }

  public ListenablePendingQueue<E> registerNotFullToFullListener(
      String id, PendingQueueNotFullToFullListener listener) {
    notFullToFullListeners.put(id, listener);
    return this;
  }

  public void removeNotFullToFullListener(String id) {
    notFullToFullListeners.remove(id);
  }

  public void notifyNotFullToFullListeners() {
    notFullToFullListeners
        .values()
        .forEach(PendingQueueNotFullToFullListener::onPendingQueueNotFullToFull);
  }

  public boolean offer(E event) {
    final boolean isEmpty = pendingQueue.isEmpty();
    final boolean isAdded = pendingQueue.offer(event);

    if (isAdded) {
      // we don't use size() == 1 to check whether the listener should be called,
      // because offer() and size() are not atomic, and we don't want to use lock
      // to make them atomic.
      if (isEmpty) {
        notifyEmptyToNotEmptyListeners();
      }
    } else {
      if (isFull.compareAndSet(false, true)) {
        notifyNotFullToFullListeners();
      }
    }

    return isAdded;
  }

  public E poll() {
    final boolean isEmpty = pendingQueue.isEmpty();
    final E event = pendingQueue.poll();

    if (event == null) {
      // we don't use size() == 0 to check whether the listener should be called,
      // because poll() and size() are not atomic, and we don't want to use lock
      // to make them atomic.
      if (!isEmpty) {
        notifyNotEmptyToEmptyListeners();
      }
    } else {
      if (isFull.compareAndSet(true, false)) {
        notifyFullToNotFullListeners();
      }
    }

    return event;
  }

  public void clear() {
    pendingQueue.clear();
  }

  public int size() {
    return pendingQueue.size();
  }
}
