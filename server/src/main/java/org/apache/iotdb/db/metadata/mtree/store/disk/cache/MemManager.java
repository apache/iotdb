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
package org.apache.iotdb.db.metadata.mtree.store.disk.cache;

import org.apache.iotdb.db.metadata.mnode.IMNode;

public class MemManager implements IMemManager {

  private int capacity = 0;

  private int size;

  private int pinnedSize;

  @Override
  public void initCapacity(int capacity) {
    this.capacity = capacity;
  }

  @Override
  public boolean isEmpty() {
    return size == 0;
  }

  @Override
  public boolean isUnderThreshold() {
    return isEmpty() || size <= (capacity - pinnedSize) * 0.6;
  }

  @Override
  public boolean requestMemResource(IMNode node) {
    if (size < capacity - 1 - pinnedSize) {
      size++;
      return true;
    }
    return false;
  }

  @Override
  public void releaseMemResource(IMNode node) {
    size--;
  }

  @Override
  public void requestPinnedMemResource(IMNode node) {
    pinnedSize++;
  }

  @Override
  public void releasePinnedMemResource(IMNode node) {
    pinnedSize--;
    size++;
  }

  @Override
  public void clear() {
    size = 0;
  }
}
