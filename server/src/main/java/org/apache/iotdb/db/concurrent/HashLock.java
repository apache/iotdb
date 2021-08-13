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
package org.apache.iotdb.db.concurrent;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class manages an array of locks and use the hash code of an object as an index of the array
 * to find the corresponding lock, so that the operations on the same key (string) can be prevented
 * while the number of locks remain controlled.
 */
public class HashLock {

  private static final int DEFAULT_LOCK_NUM = 100;

  private ReentrantReadWriteLock[] locks;
  private int lockSize;

  public HashLock() {
    this.lockSize = DEFAULT_LOCK_NUM;
    init();
  }

  private void init() {
    locks = new ReentrantReadWriteLock[lockSize];
    for (int i = 0; i < lockSize; i++) {
      locks[i] = new ReentrantReadWriteLock();
    }
  }

  public void readLock(Object obj) {
    this.locks[Math.abs(obj.hashCode() % lockSize)].readLock().lock();
  }

  public void readUnlock(Object obj) {
    this.locks[Math.abs(obj.hashCode() % lockSize)].readLock().unlock();
  }

  public void writeLock(Object obj) {
    this.locks[Math.abs(obj.hashCode() % lockSize)].writeLock().lock();
  }

  public void writeUnlock(Object obj) {
    this.locks[Math.abs(obj.hashCode() % lockSize)].writeLock().unlock();
  }
}
