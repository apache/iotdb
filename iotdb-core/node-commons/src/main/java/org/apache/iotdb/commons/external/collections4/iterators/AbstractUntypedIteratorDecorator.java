/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.commons.external.collections4.iterators;

import java.util.Iterator;

/**
 * Provides basic behaviour for decorating an iterator with extra functionality without committing
 * the generic type of the Iterator implementation.
 *
 * <p>All methods are forwarded to the decorated iterator.
 *
 * @since 4.0
 */
public abstract class AbstractUntypedIteratorDecorator<I, O> implements Iterator<O> {

  /** The iterator being decorated */
  private final Iterator<I> iterator;

  /**
   * Create a new AbstractUntypedIteratorDecorator.
   *
   * @param iterator the iterator to decorate
   * @throws NullPointerException if the iterator is null
   */
  protected AbstractUntypedIteratorDecorator(final Iterator<I> iterator) {
    super();
    if (iterator == null) {
      throw new NullPointerException("Iterator must not be null");
    }
    this.iterator = iterator;
  }

  /**
   * Gets the iterator being decorated.
   *
   * @return the decorated iterator
   */
  protected Iterator<I> getIterator() {
    return iterator;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public void remove() {
    iterator.remove();
  }
}
