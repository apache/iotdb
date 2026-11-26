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
 * Provides basic behaviour for decorating an iterator with extra functionality.
 *
 * <p>All methods are forwarded to the decorated iterator.
 *
 * @since 3.0
 */
public abstract class AbstractIteratorDecorator<E> extends AbstractUntypedIteratorDecorator<E, E> {

  // -----------------------------------------------------------------------
  /**
   * Constructor that decorates the specified iterator.
   *
   * @param iterator the iterator to decorate, must not be null
   * @throws NullPointerException if the iterator is null
   */
  protected AbstractIteratorDecorator(final Iterator<E> iterator) {
    super(iterator);
  }

  /** {@inheritDoc} */
  @Override
  public E next() {
    return getIterator().next();
  }
}
