/*
 * Copyright (C) 2007 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.common.base;

import javax.annotation.CheckForNull;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.google.common.base.NullnessCasts.uncheckedCastNullableTToT;
import static com.google.common.base.Preconditions.checkState;

/**
 * Note this class is a copy of {@link com.google.common.collect.AbstractIterator} (for dependency
 * reasons).
 */
abstract class AbstractIterator<T extends Object> implements Iterator<T> {
  private State state = State.NOT_READY;

  protected AbstractIterator() {}

  private enum State {
    READY,
    NOT_READY,
    DONE,
    FAILED,
  }

  @CheckForNull private T next;

  @CheckForNull
  protected abstract T computeNext();

  @CheckForNull
  protected final T endOfData() {
    state = State.DONE;
    return null;
  }

  @Override
  public final boolean hasNext() {
    checkState(state != State.FAILED);
    switch (state) {
      case DONE:
        return false;
      case READY:
        return true;
      default:
    }
    return tryToComputeNext();
  }

  private boolean tryToComputeNext() {
    state = State.FAILED; // temporary pessimism
    next = computeNext();
    if (state != State.DONE) {
      state = State.READY;
      return true;
    }
    return false;
  }

  @Override
  public final T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    state = State.NOT_READY;
    // Safe because hasNext() ensures that tryToComputeNext() has put a T into `next`.
    T result = uncheckedCastNullableTToT(next);
    next = null;
    return result;
  }

  @Override
  public final void remove() {
    throw new UnsupportedOperationException();
  }
}
