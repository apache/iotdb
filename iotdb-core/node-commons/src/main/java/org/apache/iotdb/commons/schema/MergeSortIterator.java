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

package org.apache.iotdb.commons.schema;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class MergeSortIterator<E> implements Iterator<E> {
  private Iterator<E> leftIterator;
  private Iterator<E> rightIterator;

  private E leftHeader;
  private E rightHeader;

  protected MergeSortIterator(Iterator<E> leftIterator, Iterator<E> rightIterator) {
    this.leftIterator = leftIterator;
    this.rightIterator = rightIterator;
    leftHeader = leftIterator.hasNext() ? leftIterator.next() : null;
    rightHeader = rightIterator.hasNext() ? rightIterator.next() : null;
  }

  public boolean hasNext() {
    return leftHeader != null || rightHeader != null;
  }

  public E next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return tryGetNext();
  }

  protected E catchLeft() {
    E ans = leftHeader;
    leftHeader = leftIterator.hasNext() ? leftIterator.next() : null;
    return ans;
  }

  protected E catchRight() {
    E ans = rightHeader;
    rightHeader = rightIterator.hasNext() ? rightIterator.next() : null;
    return ans;
  }

  protected E catchEqual(int decide) {
    switch (decide) {
      case -1:
        rightHeader = rightIterator.hasNext() ? rightIterator.next() : null;
        return catchLeft();
      case 0:
        throw new NoSuchElementException();
      case 1:
        leftHeader = leftIterator.hasNext() ? leftIterator.next() : null;
        return catchRight();
      default:
        throw new IllegalArgumentException();
    }
  }

  E tryGetNext() {
    if (leftHeader != null && rightHeader != null) {
      switch (compare(leftHeader, rightHeader)) {
        case -1:
          return onReturnLeft(catchLeft());
        case 0:
          return onReturnEqual(catchEqual(decide()));
        case 1:
          return onReturnRight(catchRight());
        default:
          throw new IllegalArgumentException();
      }
    } else if (leftHeader != null) {
      return catchLeft();
    } else if (rightHeader != null) {
      return catchRight();
    } else {
      throw new NoSuchElementException();
    }
  }

  private E onReturnLeft(E left) {
    return left;
  }

  protected E onReturnRight(E right) {
    return right;
  }

  protected E onReturnEqual(E equal) {
    return equal;
  }

  protected int decide() {
    return 1;
  }

  protected abstract int compare(E left, E right);
}
