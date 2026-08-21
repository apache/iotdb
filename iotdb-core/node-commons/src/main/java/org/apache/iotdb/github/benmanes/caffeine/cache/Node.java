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

package org.apache.iotdb.github.benmanes.caffeine.cache;

import org.apache.iotdb.github.benmanes.caffeine.cache.AccessOrderDeque.AccessOrder;
import org.apache.iotdb.github.benmanes.caffeine.cache.WriteOrderDeque.WriteOrder;

import java.lang.ref.ReferenceQueue;

/**
 * An entry in the cache containing the key, value, weight, access, and write metadata. The key or
 * value may be held weakly or softly requiring identity comparison.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@SuppressWarnings({"GuardedByChecker", "GuardedByValidator"})
abstract class Node<K, V> implements AccessOrder<Node<K, V>>, WriteOrder<Node<K, V>> {

  /** Return the key or {@code null} if it has been reclaimed by the garbage collector. */
  public abstract K getKey();

  /**
   * Returns the reference that the cache is holding the entry by. This is either the key if
   * strongly held or a {@link java.lang.ref.WeakReference} to that key.
   */
  public abstract Object getKeyReference();

  /** Return the value or {@code null} if it has been reclaimed by the garbage collector. */
  public abstract V getValue();

  /**
   * Returns the reference to the value. This is either the value if strongly held or a {@link
   * java.lang.ref.Reference} to that value.
   */
  public abstract Object getValueReference();

  /**
   * Sets the value, which may be held strongly, weakly, or softly. This update may be set lazily
   * and rely on the memory fence when the lock is released.
   */
  public abstract void setValue(V value, ReferenceQueue<V> referenceQueue);

  /**
   * Returns {@code true} if the given objects are considered equivalent. A strongly held value is
   * compared by equality and a weakly or softly held value is compared by identity.
   */
  public abstract boolean containsValue(Object value);

  /** Returns the weight of this entry from the entry's perspective. */
  public int getWeight() {
    return 1;
  }

  /** Sets the weight from the entry's perspective. */
  public void setWeight(int weight) {}

  /** Returns the weight of this entry from the policy's perspective. */
  public int getPolicyWeight() {
    return 1;
  }

  /** Sets the weight from the policy's perspective. */
  public void setPolicyWeight(int weight) {}

  /* --------------- Health --------------- */

  /** If the entry is available in the hash-table and page replacement policy. */
  public abstract boolean isAlive();

  /**
   * If the entry was removed from the hash-table and is awaiting removal from the page replacement
   * policy.
   */
  public abstract boolean isRetired();

  /** If the entry was removed from the hash-table and the page replacement policy. */
  public abstract boolean isDead();

  /** Sets the node to the <tt>retired</tt> state. */
  public abstract void retire();

  /** Sets the node to the <tt>dead</tt> state. */
  public abstract void die();

  /* --------------- Variable order --------------- */

  /** Returns the variable expiration time, in nanoseconds. */
  public long getVariableTime() {
    return 0L;
  }

  /**
   * Sets the variable expiration time in nanoseconds. This update may be set lazily and rely on the
   * memory fence when the lock is released.
   */
  public void setVariableTime(long time) {}

  /**
   * Atomically sets the variable time to the given updated value if the current value equals the
   * expected value and returns if the update was successful.
   */
  public boolean casVariableTime(long expect, long update) {
    throw new UnsupportedOperationException();
  }

  public Node<K, V> getPreviousInVariableOrder() {
    throw new UnsupportedOperationException();
  }

  public void setPreviousInVariableOrder(Node<K, V> prev) {
    throw new UnsupportedOperationException();
  }

  public Node<K, V> getNextInVariableOrder() {
    throw new UnsupportedOperationException();
  }

  public void setNextInVariableOrder(Node<K, V> prev) {
    throw new UnsupportedOperationException();
  }

  /* --------------- Access order --------------- */

  public static final int WINDOW = 0;
  public static final int PROBATION = 1;
  public static final int PROTECTED = 2;

  /** Returns if the entry is in the Window or Main space. */
  public boolean inWindow() {
    return getQueueType() == WINDOW;
  }

  /** Returns if the entry is in the Main space's probation queue. */
  public boolean inMainProbation() {
    return getQueueType() == PROBATION;
  }

  /** Returns if the entry is in the Main space's protected queue. */
  public boolean inMainProtected() {
    return getQueueType() == PROTECTED;
  }

  /** Sets the status to the Window queue. */
  public void makeWindow() {
    setQueueType(WINDOW);
  }

  /** Sets the status to the Main space's probation queue. */
  public void makeMainProbation() {
    setQueueType(PROBATION);
  }

  /** Sets the status to the Main space's protected queue. */
  public void makeMainProtected() {
    setQueueType(PROTECTED);
  }

  /** Returns the queue that the entry's resides in (window, probation, or protected). */
  public int getQueueType() {
    return WINDOW;
  }

  /** Set queue that the entry resides in (window, probation, or protected). */
  public void setQueueType(int queueType) {
    throw new UnsupportedOperationException();
  }

  /** Returns the time that this entry was last accessed, in ns. */
  public long getAccessTime() {
    return 0L;
  }

  /**
   * Sets the access time in nanoseconds. This update may be set lazily and rely on the memory fence
   * when the lock is released.
   */
  public void setAccessTime(long time) {}

  @Override
  public Node<K, V> getPreviousInAccessOrder() {
    return null;
  }

  @Override
  public void setPreviousInAccessOrder(Node<K, V> prev) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Node<K, V> getNextInAccessOrder() {
    return null;
  }

  @Override
  public void setNextInAccessOrder(Node<K, V> next) {
    throw new UnsupportedOperationException();
  }

  /* --------------- Write order --------------- */

  /** Returns the time that this entry was last written, in ns. */
  public long getWriteTime() {
    return 0L;
  }

  /**
   * Sets the write time in nanoseconds. This update may be set lazily and rely on the memory fence
   * when the lock is released.
   */
  public void setWriteTime(long time) {}

  /**
   * Atomically sets the write time to the given updated value if the current value equals the
   * expected value and returns if the update was successful.
   */
  public boolean casWriteTime(long expect, long update) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Node<K, V> getPreviousInWriteOrder() {
    return null;
  }

  @Override
  public void setPreviousInWriteOrder(Node<K, V> prev) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Node<K, V> getNextInWriteOrder() {
    return null;
  }

  @Override
  public void setNextInWriteOrder(Node<K, V> next) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final String toString() {
    return String.format(
        "%s=[key=%s, value=%s, weight=%d, queueType=%,d, accessTimeNS=%,d, "
            + "writeTimeNS=%,d, varTimeNs=%,d, prevInAccess=%s, nextInAccess=%s, prevInWrite=%s, "
            + "nextInWrite=%s]",
        getClass().getSimpleName(),
        getKey(),
        getValue(),
        getWeight(),
        getQueueType(),
        getAccessTime(),
        getWriteTime(),
        getVariableTime(),
        getPreviousInAccessOrder() != null,
        getNextInAccessOrder() != null,
        getPreviousInWriteOrder() != null,
        getNextInWriteOrder() != null);
  }
}
