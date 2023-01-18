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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * CachedMTreeReadWriteLock is a special read-write lock.
 *
 * <p>There are two modes of reading locks. The first mode uses NON_STAMP for identification, and
 * the acquirement and release of the read lock is thread-bound, supporting reentry within the same
 * thread. In the other mode, the read operation may be performed collaboratively by multiple
 * threads, so the acquirement and release of the read lock is not thread-bound, but stamp-bound.
 * Read lock can be locked by one thread but unlocked by another thread. It supports reentry by
 * stamp.
 *
 * <p>Write lock is thread-bound, so it must be acquired and released by the same thread.
 *
 * <ul>
 *   <li>!!!WARNING!!!
 *   <li>As the lock holder is not recorded, the caller must assure that lock() and unlock() match,
 *       i.e., if you only call lock() once then do not call unlock() more than once and vice versa.
 *   <li>In particular, if the current thread has already requested a WriteLock, it can continue to
 *       reenter to acquire a WriteLock or a ReadLock. However, if a ReadLock has already been
 *       acquired (including by the current thread), the WriteLock acquiring will be blocked, which
 *       may cause deadlocks. Therefore, if a write operation needs to be performed, a WriteLock
 *       must be applied first.
 *   <li>Writer preferred is used to avoid starving the write lock request, which means once a write
 *       lock request is waiting, it will block the newly arriving read lock request expect for the
 *       re-entrant read lock request.
 * </ul>
 */
public class CachedMTreeReadWriteLock {
  private final Lock lock = new ReentrantLock();
  private final Condition okToRead = lock.newCondition();
  private final Condition okToWrite = lock.newCondition();
  private long stampAllocator = 0;

  private Thread exclusiveOwnerThread;
  private final Map<Long, Integer> readCnt = new HashMap<>();
  private int readWait = 0;
  private int writeCnt = 0;
  private int writeWait = 0;

  private final ThreadLocal<Long> sharedOwnerStamp = new ThreadLocal<>();

  public static final long NON_STAMP = -1;
  public static final long ALLOCATE_STAMP = -2;

  /**
   * Acquires the read lock.
   *
   * @param stamp NON_STAMP, ALLOCATE_STAMP or re-entry based on stamp
   *     <p>NON_STAMP means read lock acquire and release is thread-bound and supports re-entry
   *     within the same thread. Return directly if no thread holds a write lock or the current
   *     thread already holds a write lock; block and wait if another thread holds a write lock or
   *     the write lock waiting queue is not empty.
   *     <p>ALLOCATE_STAMP means read lock acquire and release is stamp-bound and supports re-entry
   *     by the same stamp. Return a new stamp if no thread holds a write lock; block and wait if
   *     another thread holds a write lock or the write lock waiting queue is not empty.
   *     <p>Re-entry based on stamp. If the stamp exists, increase the read count and return;
   *     otherwise, same as ALLOCATE_STAMP.
   * @return stamp or NON_STAMP
   */
  public long readLock(long stamp) {
    lock.lock();
    try {
      if (stamp == NON_STAMP) {
        return threadReadLock();
      } else if (stamp == ALLOCATE_STAMP) {
        return acquireReadLock();
      } else {
        if (readCnt.containsKey(stamp)) {
          readCnt.put(stamp, readCnt.get(stamp) + 1);
          return stamp;
        } else {
          return acquireReadLock();
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private long threadReadLock() {
    Long allocateStamp = sharedOwnerStamp.get();
    if (allocateStamp == null) {
      // first time entry, acquire read lock and set thread local
      sharedOwnerStamp.set(acquireReadLock());
    } else {
      // reentry, add read count
      readCnt.put(allocateStamp, readCnt.get(allocateStamp) + 1);
    }
    return NON_STAMP;
  }

  /**
   * Acquires a read lock and block and wait if another thread holds a write lock or the write lock
   * waiting queue is not empty.
   *
   * @return read lock stamp
   */
  private long acquireReadLock() {
    if (exclusiveOwnerThread != Thread.currentThread()) {
      if (writeCnt + writeWait > 0) {
        readWait++;
        try {
          okToRead.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          readWait--;
        }
      }
    }
    long allocateStamp = allocateUniqueStamp();
    readCnt.put(allocateStamp, 1);
    return allocateStamp;
  }

  /**
   * Allocate unique stamp based on stampAllocator. Because stamp is non-negative, overflow needs to
   * be avoided.
   */
  private long allocateUniqueStamp() {
    if (++stampAllocator < 0) {
      stampAllocator = 1;
    }
    return stampAllocator;
  }

  /**
   * Attempts to release read lock.
   *
   * @param stamp read lock stamp or NON_STAMP
   *     <p>NON_STAMP means release the thread-bound read lock.
   *     <p>Read lock stamp means release the stamp-bound read lock.
   */
  public void unlockRead(long stamp) {
    lock.lock();
    try {
      if (stamp == NON_STAMP) {
        threadReadUnlock();
      } else {
        if (readCnt.containsKey(stamp)) {
          if (readCnt.get(stamp) == 1) {
            readCnt.remove(stamp);
            if (readCnt.isEmpty() && writeWait > 0) {
              // no reader, then signal all writer
              okToWrite.signalAll();
            }
          } else {
            readCnt.put(stamp, readCnt.get(stamp) - 1);
          }
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private void threadReadUnlock() {
    if (sharedOwnerStamp.get() != null) {
      long allocateStamp = sharedOwnerStamp.get();
      unlockRead(allocateStamp);
      if (!readCnt.containsKey(allocateStamp)) {
        sharedOwnerStamp.remove();
      }
    }
  }

  /** Get a WriteLock and block and wait until the lock status is empty. */
  public void writeLock() {
    lock.lock();
    try {
      if (exclusiveOwnerThread != Thread.currentThread()) {
        while (!readCnt.isEmpty() || writeCnt > 0) {
          writeWait++;
          try {
            okToWrite.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            writeWait--;
          }
        }
      }
      exclusiveOwnerThread = Thread.currentThread();
      writeCnt++;
    } finally {
      lock.unlock();
    }
  }

  /** Unlock WriteLock */
  public void unlockWrite() {
    lock.lock();
    try {
      writeCnt--;
      if (writeCnt == 0) {
        exclusiveOwnerThread = null;
        if (writeWait > 0) {
          okToWrite.signalAll();
        } else if (readWait > 0) {
          okToRead.signalAll();
        }
      }
    } finally {
      lock.unlock();
    }
  }
}
