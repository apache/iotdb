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

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SimpleWriterPreferredLock {

  private final Lock lock = new ReentrantLock();
  private final Condition okToRead = lock.newCondition();
  private final Condition okToWrite = lock.newCondition();

  private volatile int readCount = 0;
  private volatile int readWait = 0;
  private volatile int writeCount = 0;
  private volatile int writeWait = 0;

  public void readLock() {
    lock.lock();
    try {
      readWait++;
      while (writeCount > 0 || writeWait > 0) {
        try {
          okToRead.wait();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      readWait--;
      readCount++;
    } finally {
      lock.unlock();
    }
  }

  public void readUnlock() {
    lock.lock();
    try {
      readCount--;
      if (readCount == 0 && writeWait > 0) {
        okToWrite.signal();
      }
    } finally {
      lock.unlock();
    }
  }

  public void writeLock() {
    lock.lock();
    try {
      writeWait++;
      while (readCount > 0) {
        try {
          okToWrite.await();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      writeWait--;
      writeCount++;
    } finally {
      lock.unlock();
    }
  }

  public void writeUnlock() {
    lock.lock();
    try {
      writeCount--;
      if (writeWait > 0) {
        okToWrite.signal();
      } else if (readWait > 0) {
        okToRead.signalAll();
      }
    } finally {
      lock.unlock();
    }
  }
}
