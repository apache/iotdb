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
  private Iterator<E> leftIterator; // iterator of the sequence on the left
  private Iterator<E> rightIterator; // iterator of the sequence on the right

  private E leftHeader; // The first element of the sequence on the left
  private E rightHeader; // The first element of the sequence on the right

  protected MergeSortIterator(Iterator<E> leftIterator, Iterator<E> rightIterator) {
    this.leftIterator = leftIterator;
    this.rightIterator = rightIterator;
    leftHeader = leftIterator.hasNext() ? leftIterator.next() : null;
    rightHeader = rightIterator.hasNext() ? rightIterator.next() : null;
  }

  // Determine whether there is a next element
  public boolean hasNext() {
    return leftHeader != null || rightHeader != null;
  }

  // Get the next element. If there is no next element, an error will be reported.
  public E next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return tryGetNext();
  }

  // In merge sort, the first element of the left sequence is added to the sorted sequence while
  // update the leftHeader.
  private E catchLeft() {
    E ans = leftHeader;
    leftHeader = leftIterator.hasNext() ? leftIterator.next() : null;
    return ans;
  }

  // In merge sort, the first element of the right sequence is added to the sorted sequence while
  // update the rightHeader.
  private E catchRight() {
    E ans = rightHeader;
    rightHeader = rightIterator.hasNext() ? rightIterator.next() : null;
    return ans;
  }

  // When two elements are the same, according to the choice of decide, the target element is left
  // and the other element is deleted.
  private E catchEqual(int decide) {
    switch (decide) {
      case -1:
        rightHeader = rightIterator.hasNext() ? rightIterator.next() : null;
        return onReturnLeft(catchLeft());
      case 0:
        throw new NoSuchElementException();
      case 1:
        leftHeader = leftIterator.hasNext() ? leftIterator.next() : null;
        return onReturnRight(catchRight());
      default:
        throw new IllegalArgumentException();
    }
  }

  // One step in merge sort: compare the first elements of the two sequences and process them based
  // on the comparison results
  E tryGetNext() {
    if (leftHeader != null && rightHeader != null) {
      switch (compare(leftHeader, rightHeader)) {
        case -1:
          return onReturnLeft(catchLeft());
        case 0:
          return catchEqual(decide());
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

  // Post-process the first element of the left sequence
  private E onReturnLeft(E left) {
    return left;
  }

  // Post-process the first element of the right sequence
  protected E onReturnRight(E right) {
    return right;
  }

  // Decide the target element when two elements are the same
  protected int decide() {
    return 1;
  }

  // Compare two elements
  protected abstract int compare(E left, E right);
}
