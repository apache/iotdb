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

public class LockManager {

  private final LockPool lockPool = new LockPool();

  private final SimpleWriterPreferredLock readWriteLock = new SimpleWriterPreferredLock();

  public long stampedReadLock(ICachedMNode node) {
    readWriteLock.readLock();
    return getMNodeLock(node).stampedReadLock();
  }

  public void stampedReadUnlock(ICachedMNode node, long stamp) {
    getMNodeLock(node).stampedReadUnlock(stamp);
    checkAndReleaseMNodeLock(node);
    readWriteLock.readUnlock();
  }

  public void threadReadLock(ICachedMNode node) {
    readWriteLock.readLock();
    getMNodeLock(node).threadReadLock();
  }

  public void threadReadLock(ICachedMNode node, boolean prior) {
    readWriteLock.readLock();
    getMNodeLock(node).threadReadLock(prior);
  }

  public void threadReadUnlock(ICachedMNode node) {
    node.getLock().threadReadUnlock();
    checkAndReleaseMNodeLock(node);
    readWriteLock.readUnlock();
  }

  public void writeLock(ICachedMNode node) {
    readWriteLock.readLock();
    getMNodeLock(node).writeLock();
  }

  public void writeUnlock(ICachedMNode node) {
    node.getLock().writeUnlock();
    checkAndReleaseMNodeLock(node);
    readWriteLock.readUnlock();
  }

  private StampedWriterPreferredLock getMNodeLock(ICachedMNode node) {
    StampedWriterPreferredLock lock = node.getLock();
    if (lock == null) {
      synchronized (this) {
        lock = node.getLock();
        if (lock == null) {
          lock = lockPool.borrowLock();
          node.setLock(lock);
        }
      }
    }
    return lock;
  }

  private void checkAndReleaseMNodeLock(ICachedMNode node) {
    StampedWriterPreferredLock lock = node.getLock();
    if (!lock.isFree()) {
      return;
    }
    synchronized (this) {
      if (lock.isFree()) {
        node.setLock(null);
      }
    }
    lockPool.returnLock(lock);
  }

  public void globalWriteLock() {
    readWriteLock.writeLock();
  }

  public void globalWriteUnlock() {
    readWriteLock.writeUnlock();
  }

  private static class LockPool {
    private static final int LOCK_POOL_CAPACITY = 400;

    private final List<StampedWriterPreferredLock> lockList = new LinkedList<>();

    private LockPool() {
      for (int i = 0; i < LOCK_POOL_CAPACITY; i++) {
        lockList.add(new StampedWriterPreferredLock());
      }
    }

    private StampedWriterPreferredLock borrowLock() {
      synchronized (lockList) {
        if (lockList.isEmpty()) {
          return new StampedWriterPreferredLock();
        } else {
          return lockList.remove(0);
        }
      }
    }

    private void returnLock(StampedWriterPreferredLock lock) {
      synchronized (lockList) {
        if (lockList.size() == LOCK_POOL_CAPACITY) {
          return;
        }
        lockList.add(0, lock);
      }
    }
  }
}
