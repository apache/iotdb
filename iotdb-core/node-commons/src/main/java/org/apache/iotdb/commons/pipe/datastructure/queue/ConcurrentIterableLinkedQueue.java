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

package org.apache.iotdb.commons.pipe.datastructure.queue;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConcurrentIterableLinkedQueue<E> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentIterableLinkedQueue.class);

  private static class LinkedListNode<E> {

    private E data;
    private LinkedListNode<E> next;

    private LinkedListNode(final E data) {
      this.data = data;
      this.next = null;
    }
  }

  // nodes:   [firstNode,  firstNode.next, firstNode.next.next, ..., lastNode     ]
  // indexes: [firstIndex, firstIndex + 1, firstIndex + 2,      ..., tailIndex - 1]
  private final LinkedListNode<E> pilotNode = new LinkedListNode<>(null);
  private LinkedListNode<E> firstNode = pilotNode;
  private LinkedListNode<E> lastNode = pilotNode;
  private long firstIndex = 0;
  private long tailIndex = 0;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final Condition hasNextCondition = lock.writeLock().newCondition();

  private final Set<DynamicIterator> iteratorSet =
      Collections.newSetFromMap(new ConcurrentHashMap<>());

  /**
   * Add an element to the tail of the queue.
   *
   * @param e the element to be added, which cannot be {@code null}
   */
  public void add(final E e) {
    if (e == null) {
      throw new IllegalArgumentException("Null element is not allowed.");
    }

    final LinkedListNode<E> newNode = new LinkedListNode<>(e);
    lock.writeLock().lock();
    try {
      if (firstNode == pilotNode) {
        firstIndex = tailIndex;
        firstNode = newNode;
        // Always explicitly set pilotNode.next to firstNode
        pilotNode.next = newNode;
      }

      ++tailIndex;
      // If firstNode == pilotNode, then lastNode == pilotNode
      lastNode.next = newNode;
      lastNode = newNode;

      hasNextCondition.signalAll();
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Try to remove all elements whose index is less than the given index. Elements whose index is
   * equal to the given index will not be removed.
   *
   * <p>Note that iterators will limit the actual removed index to be larger than their next index.
   *
   * <p>Note that the actual removed index may be larger than the given index if the given index is
   * smaller than {@link ConcurrentIterableLinkedQueue#firstIndex}.
   *
   * <p>Note that the actual removed index may be smaller than the given index if the given index is
   * larger than {@link ConcurrentIterableLinkedQueue#tailIndex}.
   *
   * @param newFirstIndex the given index
   * @return the actual first index that is not removed
   */
  public long tryRemoveBefore(long newFirstIndex) {
    lock.writeLock().lock();
    try {
      // Iterate over iterators to find the minimum valid newFirstIndex
      for (final DynamicIterator iterator : iteratorSet) {
        newFirstIndex = Math.min(newFirstIndex, iterator.getNextIndex());
      }
      newFirstIndex = Math.max(newFirstIndex, firstIndex);
      newFirstIndex = Math.min(newFirstIndex, tailIndex);

      LinkedListNode<E> currentNode = firstNode;
      for (; firstIndex < newFirstIndex; ++firstIndex) {
        final LinkedListNode<E> nextNode = currentNode.next;
        currentNode.data = null;
        currentNode.next = null;
        currentNode = nextNode;
      }

      firstNode = currentNode;
      // pilotNode.next shall be null when the queue is empty and firstNode == pilotNode
      // to make iterator.hasNext() == false when the iterator is on the pilotNode
      if (firstNode != pilotNode) {
        pilotNode.next = firstNode;
      }

      // Reset firstNode and lastNode to pilotNode if the queue becomes empty
      if (firstNode == null) {
        firstNode = pilotNode;
        lastNode = pilotNode;
      }

      // Update iterators if necessary
      iteratorSet.forEach(
          iterator -> {
            if (iterator.nextIndex == firstIndex) {
              iterator.currentNode = pilotNode;
            }
          });

      hasNextCondition.signalAll();

      return firstIndex;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /** Clear the queue. All elements will be removed. All iterators will be closed. */
  public void clear() {
    lock.writeLock().lock();
    try {
      // Use a new set to avoid ConcurrentModificationException
      ImmutableSet.copyOf(iteratorSet).forEach(DynamicIterator::close);

      tryRemoveBefore(tailIndex);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public boolean isEmpty() {
    lock.readLock().lock();
    try {
      return firstIndex == tailIndex;
    } finally {
      lock.readLock().unlock();
    }
  }

  public long size() {
    lock.readLock().lock();
    try {
      return tailIndex - firstIndex;
    } finally {
      lock.readLock().unlock();
    }
  }

  public long getFirstIndex() {
    lock.readLock().lock();
    try {
      return firstIndex;
    } finally {
      lock.readLock().unlock();
    }
  }

  public long getTailIndex() {
    lock.readLock().lock();
    try {
      return tailIndex;
    } finally {
      lock.readLock().unlock();
    }
  }

  public void setFirstIndex(final long firstIndex) {
    lock.writeLock().lock();
    try {
      this.firstIndex = firstIndex;
      if (tailIndex < firstIndex) {
        tailIndex = firstIndex;
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Check if the given index is valid. An index is valid if it is between {@link
   * ConcurrentIterableLinkedQueue#firstIndex} and {@link ConcurrentIterableLinkedQueue#tailIndex}.
   * If the queue is empty, the given index is valid if it is equal to {@link
   * ConcurrentIterableLinkedQueue#firstIndex}.
   */
  public boolean isNextIndexValid(final long nextIndex) {
    lock.readLock().lock();
    try {
      return firstIndex <= nextIndex && nextIndex <= tailIndex;
    } finally {
      lock.readLock().unlock();
    }
  }

  public boolean hasAnyIterators() {
    return !iteratorSet.isEmpty();
  }

  public DynamicIterator iterateFrom(final long offset) {
    final DynamicIterator iterator = new DynamicIterator(offset);
    iteratorSet.add(iterator);
    return iterator;
  }

  public DynamicIterator iterateFromEarliest() {
    return iterateFrom(Long.MIN_VALUE);
  }

  public DynamicIterator iterateFromLatest() {
    return iterateFrom(Long.MAX_VALUE);
  }

  public class DynamicIterator implements Iterator<E>, Closeable {

    private LinkedListNode<E> currentNode;
    private long nextIndex;

    private volatile boolean isClosed = false;

    public DynamicIterator(long nextIndex) {
      lock.writeLock().lock();
      try {
        if (tailIndex <= nextIndex) {
          this.currentNode = lastNode;
          this.nextIndex = tailIndex;
          return;
        }

        this.currentNode = pilotNode;
        if (firstIndex < nextIndex) {
          final long step = nextIndex - firstIndex;
          for (long i = 0; i < step; ++i) {
            next();
          }
        } else {
          nextIndex = firstIndex;
        }
        this.nextIndex = nextIndex;
      } finally {
        lock.writeLock().unlock();
      }
    }

    @Override
    public E next() {
      return next(Long.MAX_VALUE);
    }

    /**
     * Get the next element in the queue. If the queue is empty, wait for the next element to be
     * added. Note that this does NOT poll the next element.
     *
     * @param waitTimeMillis the maximum time to wait in milliseconds
     * @return the next element in the queue. {@code null} if the queue is closed, or if the waiting
     *     time elapsed, or the thread is interrupted
     */
    public E peek(final long waitTimeMillis) {
      lock.writeLock().lock();
      try {
        while (!hasNext()) {
          if (isClosed) {
            LOGGER.warn("Calling next() to a closed iterator, will return null.");
            return null;
          }
          if (!hasNextCondition.await(waitTimeMillis, TimeUnit.MILLISECONDS)) {
            return null;
          }
        }
        return currentNode.next.data;
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("Interrupted while waiting for next element.", e);
        return null;
      } finally {
        lock.writeLock().unlock();
      }
    }

    /**
     * Get the next element in the queue. If the queue is empty, wait for the next element to be
     * added.
     *
     * @param waitTimeMillis the maximum time to wait in milliseconds
     * @return the next element in the queue. {@code null} if the queue is closed, or if the waiting
     *     time elapsed, or the thread is interrupted
     */
    public E next(final long waitTimeMillis) {
      lock.writeLock().lock();
      try {
        while (!hasNext()) {
          if (isClosed) {
            LOGGER.warn("Calling next() to a closed iterator, will return null.");
            return null;
          }
          if (!hasNextCondition.await(waitTimeMillis, TimeUnit.MILLISECONDS)) {
            return null;
          }
        }

        currentNode = currentNode.next;
        ++nextIndex;

        return currentNode.data;
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("Interrupted while waiting for next element.", e);
        return null;
      } finally {
        lock.writeLock().unlock();
      }
    }

    @Override
    public boolean hasNext() {
      lock.readLock().lock();
      try {
        return !isClosed && currentNode.next != null;
      } finally {
        lock.readLock().unlock();
      }
    }

    /**
     * Seek to the given index, which represents the index of the next element to be returned. If
     * the given index is smaller than {@link ConcurrentIterableLinkedQueue#firstIndex}, the
     * iterator will be reset to the earliest element. If the given index is larger than {@link
     * ConcurrentIterableLinkedQueue#tailIndex}, the iterator will be reset to the latest element.
     * Otherwise, the iterator will be reset to the element whose index is the given index.
     *
     * @param newNextIndex the given index
     * @return the actual index of the next element to be returned
     */
    public long seek(long newNextIndex) {
      lock.writeLock().lock();
      try {
        if (isClosed) {
          return -1;
        }

        newNextIndex = Math.max(firstIndex, Math.min(tailIndex, newNextIndex));
        final long oldNextIndex = nextIndex;

        if (newNextIndex < oldNextIndex) {
          currentNode = pilotNode;
          nextIndex = firstIndex;

          final long step = newNextIndex - firstIndex;
          for (long i = 0; i < step; ++i) {
            next();
          }
        } else {
          final long step = newNextIndex - oldNextIndex;
          for (long i = 0; i < step; ++i) {
            next();
          }
        }

        return nextIndex;
      } finally {
        lock.writeLock().unlock();
      }
    }

    @Override
    public void close() {
      lock.writeLock().lock();
      try {
        isClosed = true;
        iteratorSet.remove(this);

        hasNextCondition.signalAll();
      } finally {
        lock.writeLock().unlock();
      }
    }

    public boolean isClosed() {
      return isClosed;
    }

    public long getNextIndex() {
      lock.readLock().lock();
      try {
        return isClosed ? -1 : nextIndex;
      } finally {
        lock.readLock().unlock();
      }
    }
  }
}
