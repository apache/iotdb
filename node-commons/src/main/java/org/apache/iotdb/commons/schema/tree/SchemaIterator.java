package org.apache.iotdb.commons.schema.tree;

import java.util.NoSuchElementException;

public interface SchemaIterator<E> extends AutoCloseable {
  /**
   * Returns {@code true} if the iteration has more elements. (In other words, returns {@code true}
   * if {@link #next} would return an element rather than throwing an exception.)
   *
   * @return {@code true} if the iteration has more elements
   */
  boolean hasNext();

  /**
   * Returns the next element in the iteration.
   *
   * @return the next element in the iteration
   * @throws NoSuchElementException if the iteration has no more elements
   */
  E next();

  /**
   * Get failure if some exception is thrown while iterating.
   *
   * @return throwable
   */
  Throwable getFailure();

  /**
   * Check whether iterator success.
   *
   * @return true if successful
   */
  boolean isSuccess();

  void close();
}
