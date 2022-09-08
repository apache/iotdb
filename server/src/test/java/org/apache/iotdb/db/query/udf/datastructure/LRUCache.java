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

import org.apache.iotdb.db.mpp.transformation.datastructure.Cache;

import java.util.Arrays;

public class LRUCache extends Cache {

  private final int[] inMemory;
  private final int[] outMemory;

  public LRUCache(int capacity) {
    super(capacity);
    inMemory = new int[capacity << 4];
    outMemory = new int[capacity << 4];
    Arrays.fill(inMemory, Integer.MIN_VALUE);
    Arrays.fill(outMemory, Integer.MIN_VALUE);
  }

  public int get(int targetIndex) {
    access(targetIndex);
    return inMemory[targetIndex];
  }

  public void set(int targetIndex, int value) {
    access(targetIndex);
    inMemory[targetIndex] = value;
  }

  private void access(int targetIndex) {
    if (!containsKey(targetIndex)) {
      if (cacheCapacity <= size()) {
        int lastIndex = getLast();
        outMemory[lastIndex] = inMemory[lastIndex];
        inMemory[lastIndex] = Integer.MIN_VALUE;
      }
      inMemory[targetIndex] = outMemory[targetIndex];
      outMemory[targetIndex] = Integer.MIN_VALUE;
    }
    putKey(targetIndex);
  }
}
