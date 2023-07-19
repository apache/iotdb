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
package org.apache.iotdb.lsm.sstable.interator;

import java.io.EOFException;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Used to iteratively read objects from disk
 *
 * @see java.util.Iterator
 * @param <T> The object type obtained by each iteration
 */
public interface IDiskIterator<T> {

  /**
   * Returns {@code true} if the iteration has more elements. (In other words, returns {@code true}
   * if {@link #next} would return an element rather than throwing an exception.)
   *
   * @return {@code true} if the iteration has more elements
   * @exception EOFException Signals that an end of file or end of stream has been reached
   *     unexpectedly during input.
   * @exception IOException Signals that an I/O exception of some sort has occurred. This class is
   *     the general class of exceptions produced by failed or interrupted I/O operations.
   */
  boolean hasNext() throws IOException;

  /**
   * Returns the next element in the iteration.
   *
   * @return the next element in the iteration
   * @throws NoSuchElementException if the iteration has no more elements
   * @exception IOException Signals that an I/O exception of some sort has occurred. This class is
   *     the general class of exceptions produced by failed or interrupted I/O operations.
   */
  T next() throws IOException;
}
