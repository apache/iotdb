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
package org.apache.iotdb.tsfile.read.common.block.column;

import static java.lang.Math.ceil;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ColumnUtil {

  private static final double BLOCK_RESET_SKEW = 1.25;

  private static final int DEFAULT_CAPACITY = 64;
  // See java.util.ArrayList for an explanation
  static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

  private ColumnUtil() {}

  static void checkArrayRange(int[] array, int offset, int length) {
    requireNonNull(array, "array is null");
    if (offset < 0 || length < 0 || offset + length > array.length) {
      throw new IndexOutOfBoundsException(
          format(
              "Invalid offset %s and length %s in array with %s elements",
              offset, length, array.length));
    }
  }

  static void checkValidRegion(int positionCount, int positionOffset, int length) {
    if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
      throw new IndexOutOfBoundsException(
          format(
              "Invalid position %s and length %s in block with %s positions",
              positionOffset, length, positionCount));
    }
  }

  static void checkValidPositions(boolean[] positions, int positionCount) {
    if (positions.length != positionCount) {
      throw new IllegalArgumentException(
          format(
              "Invalid positions array size %d, actual position count is %d",
              positions.length, positionCount));
    }
  }

  static void checkValidPosition(int position, int positionCount) {
    if (position < 0 || position >= positionCount) {
      throw new IllegalArgumentException(
          format("Invalid position %s in block with %s positions", position, positionCount));
    }
  }

  static int calculateNewArraySize(int currentSize) {
    // grow array by 50%
    long newSize = (long) currentSize + (currentSize >> 1);

    // verify new size is within reasonable bounds
    if (newSize < DEFAULT_CAPACITY) {
      newSize = DEFAULT_CAPACITY;
    } else if (newSize > MAX_ARRAY_SIZE) {
      newSize = MAX_ARRAY_SIZE;
      if (newSize == currentSize) {
        throw new IllegalArgumentException(format("Cannot grow array beyond '%s'", MAX_ARRAY_SIZE));
      }
    }
    return (int) newSize;
  }

  static int calculateBlockResetSize(int currentSize) {
    long newSize = (long) ceil(currentSize * BLOCK_RESET_SKEW);

    // verify new size is within reasonable bounds
    if (newSize < DEFAULT_CAPACITY) {
      newSize = DEFAULT_CAPACITY;
    } else if (newSize > MAX_ARRAY_SIZE) {
      newSize = MAX_ARRAY_SIZE;
    }
    return (int) newSize;
  }
}
