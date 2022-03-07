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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.mnode.IMNode;

public class MemManager implements IMemManager {

  private int capacity =
      IoTDBDescriptor.getInstance().getConfig().getCachedMetadataSizeInPersistentMode();

  private volatile int size;

  private volatile int pinnedSize;

  @Override
  public synchronized void initCapacity(int capacity) {
    this.capacity = capacity;
  }

  @Override
  public synchronized boolean isEmpty() {
    return size == 0;
  }

  @Override
  public synchronized boolean isExceedThreshold() {
    return !isEmpty() && size + pinnedSize > capacity * 0.6;
  }

  @Override
  public synchronized boolean isExceedCapacity() {
    return size + pinnedSize > capacity;
  }

  @Override
  public synchronized boolean requestMemResource(IMNode node) {
    if (size < capacity - pinnedSize) {
      size++;
      return true;
    }
    return false;
  }

  @Override
  public synchronized void releaseMemResource(IMNode node) {
    size--;
  }

  @Override
  public synchronized void requestPinnedMemResource(IMNode node) {
    pinnedSize++;
  }

  @Override
  public synchronized void upgradeMemResource(IMNode node) {
    pinnedSize++;
    size--;
  }

  @Override
  public synchronized void releasePinnedMemResource(IMNode node) {
    pinnedSize--;
    size++;
  }

  @Override
  public synchronized void clear() {
    size = 0;
  }

  @Override
  public int getPinnedSize() {
    return pinnedSize;
  }

  @Override
  public int getCachedSize() {
    return size;
  }
}
