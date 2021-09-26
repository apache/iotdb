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
package org.apache.iotdb.db.metadata.metadisk.cache;

import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LRUCacheStrategy implements ICacheStrategy {

  private volatile int size = 0;
  private CacheEntry first;
  private CacheEntry last;

  private final Lock lock = new ReentrantLock();

  @Override
  public int getSize() {
    return size;
  }

  @Override
  public void lockMNode(IMNode mNode) {
    if (mNode == null) {
      return;
    }
    try {
      lock.lock();
      CacheEntry entry = mNode.getCacheEntry();
      if (entry == null) {
        entry = new CacheEntry(mNode);
      }
      if (mNode.getParent() != null && !mNode.isLockedInMemory()) {
        increaseLock(mNode.getParent().getCacheEntry());
      }
      increaseLock(entry);
    } finally {
      lock.unlock();
    }
  }

  private void increaseLock(CacheEntry entry) {
    if (!entry.isLocked() && isInCacheList(entry)) {
      removeOne(entry);
    }
    entry.increaseLock();
  }

  private boolean isInCacheList(CacheEntry entry) {
    return entry.pre != null || entry.next != null || first == entry || last == entry;
  }

  @Override
  public void unlockMNode(IMNode mNode) {
    if (mNode == null) {
      return;
    }
    try {
      lock.lock();
      CacheEntry entry = mNode.getCacheEntry();
      if (entry == null || !entry.isLocked()) {
        return;
      }
      decreaseLock(entry);
      while (mNode.getParent() != null && !mNode.isLockedInMemory()) {
        mNode = mNode.getParent();
        decreaseLock(mNode.getCacheEntry());
      }
    } finally {
      lock.unlock();
    }
  }

  private void decreaseLock(CacheEntry entry) {
    if (entry == null) {
      return;
    }
    entry.decreaseLock();
    if (!entry.isLocked()) {
      moveToFirst(entry);
    }
  }

  @Override
  public void applyChange(IMNode mNode) {
    if (mNode == null) {
      return;
    }
    try {
      lock.lock();
      if (mNode.getParent() != null && !mNode.getParent().isCached()) {
        return;
      }
      CacheEntry entry = mNode.getCacheEntry();
      if (entry == null) {
        entry = new CacheEntry(mNode);
      }
      if (!entry.isLocked()) {
        moveToFirst(entry);
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void setModified(IMNode mNode, boolean modified) {
    if (mNode == null || mNode.getCacheEntry() == null) {
      return;
    }
    mNode.getCacheEntry().setModified(modified);
  }

  private void moveToFirst(CacheEntry entry) {

    if (!isInCacheList(entry)) {
      size++;
    }

    if (first == null || last == null) { // empty linked list
      first = last = entry;
      return;
    }

    if (first == entry) {
      return;
    }
    if (entry.pre != null) {
      entry.pre.next = entry.next;
    }
    if (entry.next != null) {
      entry.next.pre = entry.pre;
    }

    if (entry == last) {
      last = last.pre;
    }

    entry.next = first;
    first.pre = entry;
    first = entry;
    first.pre = null;
  }

  @Override
  public void remove(IMNode mNode) {
    if (mNode == null || !mNode.isCached()) {
      return;
    }
    try {
      lock.lock();
      removeRecursively(mNode);
    } finally {
      lock.unlock();
    }
  }

  private void removeOne(CacheEntry entry) {
    if (entry.pre != null) {
      entry.pre.next = entry.next;
    }
    if (entry.next != null) {
      entry.next.pre = entry.pre;
    }
    if (entry == first) {
      first = entry.next;
    }
    if (entry == last) {
      last = entry.pre;
    }
    size--;
    entry.pre = null;
    entry.next = null;
  }

  private void removeRecursively(IMNode mNode) {
    CacheEntry entry = mNode.getCacheEntry();
    if (entry == null) {
      return;
    }
    if (isInCacheList(entry)) {
      removeOne(entry);
    }
    mNode.setCacheEntry(null);
    for (IMNode child : mNode.getChildren().values()) {
      removeRecursively(child);
    }
  }

  @Override
  public List<IMNode> evict() {
    try {
      lock.lock();
      List<IMNode> modifiedMNodes = new LinkedList<>();
      if (last == null) {
        return modifiedMNodes;
      }

      IMNode mNode = last.value;
      IMNode parent = mNode.getParent();
      while (parent != null && parent.getCacheEntry().isModified) {
        mNode = parent;
        parent = mNode.getParent();
      }
      collectModifiedRecursively(mNode, modifiedMNodes);

      mNode = last.value;
      removeRecursively(last.value);
      modifiedMNodes.add(0, mNode);
      return modifiedMNodes;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public List<IMNode> collectModified(IMNode mNode) {
    try {
      lock.lock();
      List<IMNode> result = new LinkedList<>();
      collectModifiedRecursively(mNode, result);
      return result;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void clear() {
    try {
      lock.lock();
      while (last != null) {
        removeRecursively(last.value);
      }
    } finally {
      lock.unlock();
    }
  }

  private void collectModifiedRecursively(IMNode mNode, Collection<IMNode> mNodeCollection) {
    CacheEntry cacheEntry = mNode.getCacheEntry();
    if (cacheEntry == null) {
      return;
    }
    for (IMNode child : mNode.getChildren().values()) {
      collectModifiedRecursively(child, mNodeCollection);
    }
    if (cacheEntry.isModified) {
      mNodeCollection.add(mNode);
    }
  }

  private void showCachedMNode() {
    CacheEntry entry = first;
    while (entry != null) {
      System.out.print(entry.value);
      System.out.print(entry.getMNode().getCacheEntry() == null);
      System.out.print("->");
      entry = entry.next;
    }
    System.out.print(" ");
  }
}
