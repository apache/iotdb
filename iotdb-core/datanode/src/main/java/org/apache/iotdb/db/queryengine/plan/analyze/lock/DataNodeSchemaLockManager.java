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

package org.apache.iotdb.db.queryengine.plan.analyze.lock;

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataNodeSchemaLockManager {

  private final ReentrantReadWriteLock[] locks;

  private static class DataNodeSchemaLockManagerHold {
    private static final DataNodeSchemaLockManager INSTANCE = new DataNodeSchemaLockManager();
  }

  public static DataNodeSchemaLockManager getInstance() {
    return DataNodeSchemaLockManagerHold.INSTANCE;
  }

  private DataNodeSchemaLockManager() {
    final int lockNum = SchemaLockType.values().length;
    this.locks = new ReentrantReadWriteLock[lockNum];
    for (int i = 0; i < lockNum; i++) {
      locks[i] = new ReentrantReadWriteLock(false);
    }
  }

  public void takeReadLock(final MPPQueryContext context, final SchemaLockType lockType) {
    if (context.addAcquiredLock(lockType)) {
      locks[lockType.ordinal()].readLock().lock();
    }
  }

  public void releaseReadLock(final MPPQueryContext queryContext) {
    if (queryContext != null && !queryContext.getAcquiredLocks().isEmpty()) {
      queryContext
          .getAcquiredLocks()
          .forEach(lockType -> locks[lockType.ordinal()].readLock().unlock());
    }
  }

  public void takeWriteLock(final SchemaLockType lockType) {
    locks[lockType.ordinal()].writeLock().lock();
  }

  public void releaseWriteLock(final SchemaLockType lockType) {
    locks[lockType.ordinal()].writeLock().unlock();
  }
}
