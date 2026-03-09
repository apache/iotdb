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
package org.apache.iotdb.commons.external.collections4.collection;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Predicate;

/**
 * Decorates another <code>Collection</code> to provide additional behaviour.
 *
 * <p>Each method call made on this <code>Collection</code> is forwarded to the decorated <code>
 * Collection</code>. This class is used as a framework on which to build to extensions such as
 * synchronized and unmodifiable behaviour. The main advantage of decoration is that one decorator
 * can wrap any implementation of <code>Collection</code>, whereas sub-classing requires a new class
 * to be written for each implementation.
 *
 * <p>This implementation does not perform any special processing with {@link #iterator()}. Instead
 * it simply returns the value from the wrapped collection. This may be undesirable, for example if
 * you are trying to write an unmodifiable implementation it might provide a loophole.
 *
 * <p>This implementation does not forward the hashCode and equals methods through to the backing
 * object, but relies on Object's implementation. This is necessary to preserve the symmetry of
 * equals. Custom definitions of equality are usually based on an interface, such as Set or List, so
 * that the implementation of equals can cast the object being tested for equality to the custom
 * interface. AbstractCollectionDecorator does not implement such custom interfaces directly; they
 * are implemented only in subclasses. Therefore, forwarding equals would break symmetry, as the
 * forwarding object might consider itself equal to the object being tested, but the reverse could
 * not be true. This behavior is consistent with the JDK's collection wrappers, such as {@link
 * java.util.Collections#unmodifiableCollection(Collection)}. Use an interface-specific subclass of
 * AbstractCollectionDecorator, such as AbstractListDecorator, to preserve equality behavior, or
 * override equals directly.
 *
 * @param <E> the type of the elements in the collection
 * @since 3.0
 */
public abstract class AbstractCollectionDecorator<E> implements Collection<E>, Serializable {

  /** Serialization version */
  private static final long serialVersionUID = 6249888059822088500L;

  /** The collection being decorated */
  private Collection<E> collection;

  /**
   * Constructor only used in deserialization, do not use otherwise.
   *
   * @since 3.1
   */
  protected AbstractCollectionDecorator() {
    super();
  }

  /**
   * Constructor that wraps (not copies).
   *
   * @param coll the collection to decorate, must not be null
   * @throws NullPointerException if the collection is null
   */
  protected AbstractCollectionDecorator(final Collection<E> coll) {
    if (coll == null) {
      throw new NullPointerException("Collection must not be null.");
    }
    this.collection = coll;
  }

  /**
   * Gets the collection being decorated. All access to the decorated collection goes via this
   * method.
   *
   * @return the decorated collection
   */
  protected Collection<E> decorated() {
    return collection;
  }

  /**
   * Sets the collection being decorated.
   *
   * <p><b>NOTE:</b> this method should only be used during deserialization
   *
   * @param coll the decorated collection
   */
  protected void setCollection(final Collection<E> coll) {
    this.collection = coll;
  }

  // -----------------------------------------------------------------------

  @Override
  public boolean add(final E object) {
    return decorated().add(object);
  }

  @Override
  public boolean addAll(final Collection<? extends E> coll) {
    return decorated().addAll(coll);
  }

  @Override
  public void clear() {
    decorated().clear();
  }

  @Override
  public boolean contains(final Object object) {
    return decorated().contains(object);
  }

  @Override
  public boolean isEmpty() {
    return decorated().isEmpty();
  }

  @Override
  public Iterator<E> iterator() {
    return decorated().iterator();
  }

  @Override
  public boolean remove(final Object object) {
    return decorated().remove(object);
  }

  @Override
  public int size() {
    return decorated().size();
  }

  @Override
  public Object[] toArray() {
    return decorated().toArray();
  }

  @Override
  public <T> T[] toArray(final T[] object) {
    return decorated().toArray(object);
  }

  @Override
  public boolean containsAll(final Collection<?> coll) {
    return decorated().containsAll(coll);
  }

  /**
   * @since 4.4
   */
  @Override
  public boolean removeIf(final Predicate<? super E> filter) {
    return decorated().removeIf(filter);
  }

  @Override
  public boolean removeAll(final Collection<?> coll) {
    return decorated().removeAll(coll);
  }

  @Override
  public boolean retainAll(final Collection<?> coll) {
    return decorated().retainAll(coll);
  }

  @Override
  public String toString() {
    return decorated().toString();
  }
}
