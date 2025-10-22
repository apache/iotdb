/*
 * Copyright (C) 2007 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.common.collect;

import javax.annotation.CheckForNull;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import static com.google.common.collect.Multisets.setCountImpl;

/**
 * This class provides a skeletal implementation of the {@link Multiset} interface. A new multiset
 * implementation can be created easily by extending this class and implementing the {@link
 * Multiset#entrySet()} method, plus optionally overriding {@link #add(Object, int)} and {@link
 * #remove(Object, int)} to enable modifications to the multiset.
 *
 * <p>The {@link #count} and {@link #size} implementations all iterate across the set returned by
 * {@link Multiset#entrySet()}, as do many methods acting on the set returned by {@link
 * #elementSet()}. Override those methods for better performance.
 *
 * @author Kevin Bourrillion
 * @author Louis Wasserman
 */
abstract class AbstractMultiset<E extends Object> extends AbstractCollection<E>
    implements Multiset<E> {
  // Query Operations

  @Override
  public boolean isEmpty() {
    return entrySet().isEmpty();
  }

  @Override
  public boolean contains(@CheckForNull Object element) {
    return count(element) > 0;
  }

  // Modification Operations
  @Override
  public final boolean add(E element) {
    add(element, 1);
    return true;
  }

  @Override
  public int add(E element, int occurrences) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean remove(@CheckForNull Object element) {
    return remove(element, 1) > 0;
  }

  @Override
  public int remove(@CheckForNull Object element, int occurrences) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int setCount(E element, int count) {
    return setCountImpl(this, element, count);
  }

  @Override
  public boolean setCount(E element, int oldCount, int newCount) {
    return setCountImpl(this, element, oldCount, newCount);
  }

  // Bulk Operations

  /**
   * {@inheritDoc}
   *
   * <p>This implementation is highly efficient when {@code elementsToAdd} is itself a {@link
   * Multiset}.
   */
  @Override
  public final boolean addAll(Collection<? extends E> elementsToAdd) {
    return Multisets.addAllImpl(this, elementsToAdd);
  }

  @Override
  public final boolean removeAll(Collection<?> elementsToRemove) {
    return Multisets.removeAllImpl(this, elementsToRemove);
  }

  @Override
  public final boolean retainAll(Collection<?> elementsToRetain) {
    return Multisets.retainAllImpl(this, elementsToRetain);
  }

  @Override
  public abstract void clear();

  // Views

  @CheckForNull private transient Set<E> elementSet;

  @Override
  public Set<E> elementSet() {
    Set<E> result = elementSet;
    if (result == null) {
      elementSet = result = createElementSet();
    }
    return result;
  }

  /**
   * Creates a new instance of this multiset's element set, which will be returned by {@link
   * #elementSet()}.
   */
  Set<E> createElementSet() {
    return new ElementSet();
  }

  class ElementSet extends Multisets.ElementSet<E> {
    @Override
    Multiset<E> multiset() {
      return AbstractMultiset.this;
    }

    @Override
    public Iterator<E> iterator() {
      return elementIterator();
    }
  }

  abstract Iterator<E> elementIterator();

  @CheckForNull private transient Set<Entry<E>> entrySet;

  @Override
  public Set<Entry<E>> entrySet() {
    Set<Entry<E>> result = entrySet;
    if (result == null) {
      entrySet = result = createEntrySet();
    }
    return result;
  }

  class EntrySet extends Multisets.EntrySet<E> {
    @Override
    Multiset<E> multiset() {
      return AbstractMultiset.this;
    }

    @Override
    public Iterator<Entry<E>> iterator() {
      return entryIterator();
    }

    @Override
    public int size() {
      return distinctElements();
    }
  }

  Set<Entry<E>> createEntrySet() {
    return new EntrySet();
  }

  abstract Iterator<Entry<E>> entryIterator();

  abstract int distinctElements();

  // Object methods

  /**
   * {@inheritDoc}
   *
   * <p>This implementation returns {@code true} if {@code object} is a multiset of the same size
   * and if, for each element, the two multisets have the same count.
   */
  @Override
  public final boolean equals(@CheckForNull Object object) {
    return Multisets.equalsImpl(this, object);
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation returns the hash code of {@link Multiset#entrySet()}.
   */
  @Override
  public final int hashCode() {
    return entrySet().hashCode();
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation returns the result of invoking {@code toString} on {@link
   * Multiset#entrySet()}.
   */
  @Override
  public final String toString() {
    return entrySet().toString();
  }
}
