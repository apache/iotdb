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

package org.apache.iotdb.db.engine.storagegroup;

/**
 * TsFileLock is a special read-write lock that can be locked by one thread but unlocked by another
 * thread.
 *
 * <p>When the query thread crashes, the locks it used can no longer be unlocked by itself and
 * another thread must do the unlock for it. Unfortunately, normal Java ReentrantLock can only be
 * unlocked by the thread which locked it, so we have to use a different implementation. It is also
 * interesting to use a third-party implementation and if you do find a better one, please submit an
 * issue on our website.
 *
 * <p>WARNING: as the lock holder is not recorded, the caller must assure that lock() and unlock()
 * match, i.e., if you only call lock() once then do not call unlock() more than once and vice
 * versa.
 */
public class TsFileLock {

  private volatile int readCnt;
  private volatile int writeCnt;

  public void readLock() {
    synchronized (this) {
      while (writeCnt > 0) {
        try {
          this.wait(1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          // ignore
        }
      }

      readCnt++;
    }
  }

  public void readUnlock() {
    synchronized (this) {
      if (readCnt > 0) {
        readCnt--;
        this.notifyAll();
      }
    }
  }

  public void writeLock() {
    synchronized (this) {
      try {
        while (writeCnt > 0 || readCnt > 0) {
          this.wait(1);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // ignore
      }

      writeCnt++;
    }
  }

  public void writeUnlock() {
    synchronized (this) {
      if (writeCnt > 0) {
        writeCnt--;
        this.notifyAll();
      }
    }
  }

  public boolean tryWriteLock() {
    synchronized (this) {
      if (writeCnt > 0 || readCnt > 0) {
        return false;
      }

      writeCnt++;
      return true;
    }
  }

  public boolean tryReadLock() {
    synchronized (this) {
      if (writeCnt > 0) {
        return false;
      }

      readCnt++;
      return true;
    }
  }
}
