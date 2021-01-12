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

package org.apache.iotdb.db.query.udf.datastructure;

/**
 * <b>Note: It's not thread safe.</b>
 */
public abstract class Cache {

  private final int[] cacheBuffer;

  protected final int cacheCapacity;
  protected int cacheSize;

  protected Cache(int capacity) {
    cacheBuffer = new int[capacity];
    cacheCapacity = capacity;
    cacheSize = 0;
  }

  protected boolean removeFirstOccurrence(int value) {
    int firstIndex = -1;
    for (int i = 0; i < cacheSize; ++i) {
      if (value == cacheBuffer[i]) {
        firstIndex = i;
        break;
      }
    }
    if (firstIndex == -1) {
      return false;
    }
    System.arraycopy(cacheBuffer, 0, cacheBuffer, 1, firstIndex);
    --cacheSize;
    return true;
  }

  protected int removeLast() {
    int last = cacheBuffer[cacheSize - 1];
    System.arraycopy(cacheBuffer, 0, cacheBuffer, 1, cacheSize - 1);
    --cacheSize;
    return last;
  }

  protected void addFirst(int value) {
    cacheBuffer[0] = value;
    ++cacheSize;
  }
}
