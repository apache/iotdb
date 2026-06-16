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

package org.apache.iotdb.db.schemaengine.table;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.Semaphore;

public class DataNodeTableCacheTest {

  private static final String DATABASE = "interrupted_fetch_database";

  @Test
  public void interruptedFetchDoesNotLeakSemaphorePermit() throws Exception {
    final DataNodeTableCache cache = DataNodeTableCache.getInstance();
    cache.invalid(DATABASE);
    try {
      final Semaphore fetchTableSemaphore = getFetchTableSemaphore(cache);
      final int permitsBeforeFetch = fetchTableSemaphore.availablePermits();

      Thread.currentThread().interrupt();
      try {
        Assert.assertFalse(cache.isDatabaseExist(DATABASE));
      } finally {
        Thread.interrupted();
      }

      Assert.assertEquals(permitsBeforeFetch, fetchTableSemaphore.availablePermits());
    } finally {
      cache.invalid(DATABASE);
    }
  }

  private Semaphore getFetchTableSemaphore(final DataNodeTableCache cache) throws Exception {
    final Field field = DataNodeTableCache.class.getDeclaredField("fetchTableSemaphore");
    field.setAccessible(true);
    return (Semaphore) field.get(cache);
  }
}
