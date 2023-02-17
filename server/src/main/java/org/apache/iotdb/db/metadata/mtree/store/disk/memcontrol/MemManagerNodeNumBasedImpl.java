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
import java.util.concurrent.atomic.AtomicInteger;

// This class is used for memory control in dev and debug environment.
public class MemManagerNodeNumBasedImpl implements IMemManager {

  private int capacity;

  private final AtomicInteger size = new AtomicInteger(0);

  private final AtomicInteger pinnedSize = new AtomicInteger(0);

  MemManagerNodeNumBasedImpl() {}

  @Override
  public void init() {
    capacity = IoTDBDescriptor.getInstance().getConfig().getCachedMNodeSizeInSchemaFileMode();
  }

  @Override
  public boolean isEmpty() {
    return size.get() == 0;
  }

  @Override
  public boolean isExceedReleaseThreshold() {
    return size.get() + pinnedSize.get() > capacity * 0.6;
  }

  @Override
  public boolean isExceedFlushThreshold() {
    return size.get() + pinnedSize.get() > capacity;
  }

  @Override
  public void requestPinnedMemResource(IMNode node) {
    pinnedSize.getAndIncrement();
  }

  @Override
  public void upgradeMemResource(IMNode node) {
    pinnedSize.getAndIncrement();
    size.getAndDecrement();
  }

  @Override
  public void releasePinnedMemResource(IMNode node) {
    size.getAndIncrement();
    pinnedSize.getAndDecrement();
  }

  @Override
  public void releaseMemResource(IMNode node) {
    size.getAndDecrement();
  }

  @Override
  public void releaseMemResource(List<IMNode> evictedNodes) {
    size.getAndUpdate(value -> value -= evictedNodes.size());
  }

  @Override
  public void updatePinnedSize(int deltaSize) {
    // do nothing
  }

  @Override
  public void clear() {
    size.getAndSet(0);
    pinnedSize.getAndSet(0);
  }

  @Override
  public long getPinnedSize() {
    return pinnedSize.get();
  }

  @Override
  public long getCachedSize() {
    return size.get();
  }
}
