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
package org.apache.iotdb.db.metadata.mtree.store;

public class CachedMTreeReadWriteLock {
  private volatile Thread exclusiveOwnerThread;
  private volatile int readCnt;

  /**
   * Get a ReadLock, if no thread has a WriteLock or the current thread already has a WriteLock,
   * return directly; if another thread has a WriteLock, block and wait
   */
  public void readLock() {
    synchronized (this) {
      while (exclusiveByOtherThread()) {
        try {
          this.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      readCnt++;
    }
  }

  /** Get a WriteLock and block and wait until the lock status is empty. */
  public void writeLock() {
    synchronized (this) {
      while (exclusiveByOtherThread() || readCnt > 0) {
        try {
          this.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      if (exclusiveOwnerThread != Thread.currentThread()) {
        exclusiveOwnerThread = Thread.currentThread();
      }
    }
  }

  /** Unlock ReadLock */
  void unlockRead() {
    synchronized (this) {
      if (readCnt > 0) {
        readCnt--;
        this.notifyAll();
      }
    }
  }

  /** Unlock WriteLock */
  void unlockWrite() {
    synchronized (this) {
      if (exclusiveOwnerThread == Thread.currentThread()) {
        exclusiveOwnerThread = null;
        this.notifyAll();
      }
    }
  }

  private boolean exclusiveByOtherThread() {
    return exclusiveOwnerThread != null && exclusiveOwnerThread != Thread.currentThread();
  }
}
