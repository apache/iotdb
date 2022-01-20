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
package org.apache.iotdb.db.metadata.mtree.store.disk;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Segment lock for saving memory space
 * Notice that this class is a static class and all it's methods are static
 */
public class SegmentLock {

  /**
   * number of reentrant read write lock.
   * Notice that this number should be a prime number for uniform hash
   */
  private static final int NUM_OF_LOCKS = 1013;

  /**
   * locks array
   */
  private static ReentrantReadWriteLock[] locks;

  // initialize locks
  static {
    locks = new ReentrantReadWriteLock[NUM_OF_LOCKS];
    for (int i = 0; i < NUM_OF_LOCKS; i++) {
      locks[i] = new ReentrantReadWriteLock();
    }
  }

  /**
   * read lock of lock at slot[hash % NUM_OF_LOCKS]
   * @param hash hash value of object to be locked
   */
  public static void readLock(int hash){
    findLock(hash).readLock().lock();
  }

  /**
   * read unlock of lock at slot[hash % NUM_OF_LOCKS]
   * @param hash hash value of object to be locked
   */
  public static void readUnlock(int hash){
    findLock(hash).readLock().unlock();
  }

  /**
   * write lock of lock at slot[hash % NUM_OF_LOCKS]
   * @param hash hash value of object to be locked
   */
  public static void writeLock(int hash){
    findLock(hash).writeLock().lock();
  }

  /**
   * write unlock of lock at slot[hash % NUM_OF_LOCKS]
   * @param hash hash value of object to be locked
   */
  public static void writeUnlock(int hash){
    findLock(hash).writeLock().unlock();
  }

  /**
   * find lock at slot[hash % NUM_OF_LOCKS]
   * @param hash hash value of object to be locked
   * @return lock at slot[hash % NUM_OF_LOCKS]
   */
  private static ReentrantReadWriteLock findLock(int hash){
    return locks[hash % NUM_OF_LOCKS];
  }
}
