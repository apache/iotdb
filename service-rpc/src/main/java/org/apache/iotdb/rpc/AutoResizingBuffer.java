/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Helper class that wraps a byte[] so that it can expand and be reused. Users should call
 * resizeIfNecessary to make sure the buffer has suitable capacity, and then use the array as
 * needed.
 *
 * <p>Resizing policies: Expanding: If the required size > current capacity * 1.5, expand to the
 * required size, otherwise expand to current capacity * 1.5. Shrinking: If initial size < the
 * required size < current capacity * 0.6, and such small requests last for more than 5 times,
 * shrink to the middle of the required size and current capacity.
 */
class AutoResizingBuffer {

  private byte[] array;
  private int bufTooLargeCounter = RpcUtils.MAX_BUFFER_OVERSIZE_TIME;
  private final int initialCapacity;
  private long lastShrinkTime;

  private static final Logger logger = LoggerFactory.getLogger(AutoResizingBuffer.class);

  public AutoResizingBuffer(int initialCapacity) {
    this.array = new byte[initialCapacity];
    this.initialCapacity = initialCapacity;
  }

  public void resizeIfNecessary(int size) {
    final int currentCapacity = this.array.length;
    final double loadFactor = 0.6;
    if (currentCapacity < size) {
      // Increase by a factor of 1.5x
      int growCapacity = currentCapacity + (currentCapacity >> 1);
      int newCapacity = Math.max(growCapacity, size);
      this.array = Arrays.copyOf(array, newCapacity);
      bufTooLargeCounter = RpcUtils.MAX_BUFFER_OVERSIZE_TIME;
      logger.debug(
          "{} expand from {} to {}, request: {}", this, currentCapacity, newCapacity, size);
    } else if (size > initialCapacity
        && currentCapacity * loadFactor > size
        && bufTooLargeCounter-- <= 0
        && System.currentTimeMillis() - lastShrinkTime > RpcUtils.MIN_SHRINK_INTERVAL) {
      // do not resize if it is reading the request size and do not shrink too often
      array = Arrays.copyOf(array, size + (currentCapacity - size) / 2);
      bufTooLargeCounter = RpcUtils.MAX_BUFFER_OVERSIZE_TIME;
      lastShrinkTime = System.currentTimeMillis();
      logger.debug("{} shrink from {} to {}", this, currentCapacity, size);
    }
  }

  public byte[] array() {
    return this.array;
  }
}
