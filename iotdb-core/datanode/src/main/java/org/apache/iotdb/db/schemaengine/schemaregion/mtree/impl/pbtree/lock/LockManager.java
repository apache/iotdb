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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.lock;

import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class LockManager {

  private final LockPool lockPool = new LockPool();

  private final StampedWriterPreferredLock readWriteLock = new StampedWriterPreferredLock();

  public long stampedReadLock(ICachedMNode node) {
    AtomicLong stamp = new AtomicLong();
    takeMNodeLock(node, lock -> stamp.set(lock.stampedReadLock()));
    return stamp.get();
  }

  public void stampedReadUnlock(ICachedMNode node, long stamp) {
    checkAndReleaseMNodeLock(node, lock -> lock.stampedReadUnlock(stamp));
  }

  public void threadReadLock(ICachedMNode node) {
    takeMNodeLock(node, StampedWriterPreferredLock::threadReadLock);
  }

  public void threadReadLock(ICachedMNode node, boolean prior) {
    takeMNodeLock(node, lock -> lock.threadReadLock(prior));
  }

  public void threadReadUnlock(ICachedMNode node) {
    checkAndReleaseMNodeLock(node, StampedWriterPreferredLock::threadReadUnlock);
  }

  public void writeLock(ICachedMNode node) {
    takeMNodeLock(node, StampedWriterPreferredLock::writeLock);
  }

  public void writeUnlock(ICachedMNode node) {
    checkAndReleaseMNodeLock(node, StampedWriterPreferredLock::writeUnlock);
  }

  private void takeMNodeLock(
      ICachedMNode node, Consumer<StampedWriterPreferredLock> lockOperation) {
    LockEntry lockEntry;
    synchronized (this) {
      lockEntry = node.getLockEntry();
      if (lockEntry == null) {
        lockEntry = lockPool.borrowLock();
        node.setLockEntry(lockEntry);
      }
      lockEntry.pin();
    }
    lockOperation.accept(lockEntry.getLock());
  }

  private void checkAndReleaseMNodeLock(
      ICachedMNode node, Consumer<StampedWriterPreferredLock> unLockOperation) {
    synchronized (this) {
      LockEntry lockEntry = node.getLockEntry();
      StampedWriterPreferredLock lock = lockEntry.getLock();
      unLockOperation.accept(lock);
      lockEntry.unpin();
      if (lock.isFree() && !lockEntry.isPinned()) {
        node.setLockEntry(null);
        lockPool.returnLock(lockEntry);
      }
    }
  }

  public long globalStampedReadLock() {
    return readWriteLock.stampedReadLock();
  }

  public void globalStampedReadUnlock(long stamp) {
    readWriteLock.stampedReadUnlock(stamp);
  }

  public void globalReadLock() {
    readWriteLock.threadReadLock();
  }

  public void globalReadLock(boolean prior) {
    readWriteLock.threadReadLock(prior);
  }

  public void globalReadUnlock() {
    readWriteLock.threadReadUnlock();
  }

  public void globalWriteLock() {
    readWriteLock.writeLock();
  }

  public void globalWriteUnlock() {
    readWriteLock.writeUnlock();
  }

  private static class LockPool {
    private static final int LOCK_POOL_CAPACITY = 400;

    private final List<LockEntry> lockList = new LinkedList<>();

    private LockPool() {
      for (int i = 0; i < LOCK_POOL_CAPACITY; i++) {
        lockList.add(new LockEntry());
      }
    }

    private LockEntry borrowLock() {
      synchronized (lockList) {
        if (lockList.isEmpty()) {
          return new LockEntry();
        } else {
          return lockList.remove(0);
        }
      }
    }

    private void returnLock(LockEntry lockEntry) {
      synchronized (lockList) {
        if (lockList.size() == LOCK_POOL_CAPACITY) {
          return;
        }
        lockList.add(0, lockEntry);
      }
    }
  }
}
