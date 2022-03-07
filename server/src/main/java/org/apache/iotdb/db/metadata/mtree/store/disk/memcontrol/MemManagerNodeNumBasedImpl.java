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
package org.apache.iotdb.db.metadata.mtree.store.disk.memcontrol;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.util.List;

public class MemManagerNodeNumBasedImpl implements IMemManager {

  private int capacity;

  private volatile int size;

  private volatile int pinnedSize;

  MemManagerNodeNumBasedImpl() {}

  @Override
  public void init() {
    capacity = IoTDBDescriptor.getInstance().getConfig().getCachedMetadataSizeInPersistentMode();
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
  public synchronized void releaseMemResource(IMNode node) {
    size--;
  }

  @Override
  public synchronized void releaseMemResource(List<IMNode> evictedNodes) {
    size -= evictedNodes.size();
  }

  @Override
  public synchronized void clear() {
    size = 0;
    pinnedSize = 0;
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
